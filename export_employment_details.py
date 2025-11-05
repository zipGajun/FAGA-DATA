from __future__ import annotations
import os
import time
from datetime import datetime
from typing import List, Tuple, Dict, Optional

import requests
import pandas as pd

# =========================
# 사용자 설정
# =========================
START = "2000-01"   # YYYY-MM (월 단위)
END   = None        # None -> latest
RETRIES = 3
BACKOFF = 1.6
OUTFILE_PREFIX = "employment_details"

# 시리즈 매핑 CSV(선택). 없으면 아래 DEFAULT_SERIES_MAP 사용
# CSV 컬럼: series_id,label,value_type
# value_type: "level" (수준, e.g., 고용자수/천명, 임금/달러) 또는 "rate" (율, e.g., 실업률)
SERIES_CSV = "employment_series_map.csv"

# 기본 시리즈 (계절조정 SA)
DEFAULT_SERIES_MAP: List[Tuple[str, str, str]] = [
    # CES (Establishment survey)
    ("CES0000000001", "Total Nonfarm Payrolls (000s, SA)", "level"),
    ("CES0500000001", "Total Private Payrolls (000s, SA)", "level"),
    ("CES3000000001", "Manufacturing Payrolls (000s, SA)", "level"),
    ("CES0500000002", "Avg Weekly Hours - Total Private (hrs, SA)", "level"),
    ("CES0500000003", "Avg Hourly Earnings - Total Private (USD, SA)", "level"),

    # CPS (Household survey)
    ("LNS14000000", "Unemployment Rate (%, SA)", "rate"),
    ("LNS11300000", "Labor Force Participation Rate (%, SA)", "rate"),
    ("LNS12300000", "Employment-Population Ratio (%, SA)", "rate"),
    ("LNS12000000", "Employment Level (000s, SA)", "level"),
    ("LNS13000000", "Unemployment Level (000s, SA)", "level"),
]

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data"
BLS_API_KEY = os.getenv("BLS_API_KEY")  # 선택

# =========================
# 유틸
# =========================
def read_series_map(csv_path: str) -> List[Tuple[str, str, str]]:
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        cols = {c.lower(): c for c in df.columns}
        required = {"series_id", "label", "value_type"}
        if not required.issubset(set(k.lower() for k in df.columns)):
            raise ValueError(f"{csv_path} must have columns: series_id,label,value_type")
        # 정규화
        df.columns = [c.lower() for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            sid = str(r["series_id"]).strip()
            lab = str(r["label"]).strip()
            vtype = str(r["value_type"]).strip().lower()
            if vtype not in {"level", "rate"}:
                raise ValueError(f"value_type must be 'level' or 'rate' (got {vtype} for {sid})")
            rows.append((sid, lab, vtype))
        if not rows:
            raise ValueError(f"{csv_path} is empty.")
        return rows
    return DEFAULT_SERIES_MAP


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def bls_payload(series_ids: List[str], start_year: int, end_year: int) -> Dict:
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY
    }
    if not BLS_API_KEY:
        payload.pop("registrationkey", None)
    return payload


def call_bls(series_ids: List[str], start_year: int, end_year: int) -> Dict:
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(BLS_API_URL, json=bls_payload(series_ids, start_year, end_year), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                raise RuntimeError(f"BLS API not succeeded: {data.get('message')}")
            return data
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
            else:
                raise last_err


def parse_bls_result(data: Dict, labels: Dict[str, str], vtypes: Dict[str, str]) -> pd.DataFrame:
    """
    BLS 응답(JSON) -> Long DataFrame
    columns: [series_id, label, value_type, PeriodEnd, value]
    """
    rows = []
    for ts in data.get("Results", {}).get("series", []):
        sid = ts.get("seriesID")
        lab = labels.get(sid, sid)
        vtype = vtypes.get(sid, "level")
        for item in ts.get("data", []):
            period = item["period"]  # 'M01'~'M12' (M13=연평균)
            if not period.startswith("M"):  # 연평균 등 스킵
                continue
            year = int(item["year"])
            month = int(period[1:])
            val = float(str(item["value"]).replace(",", ""))
            dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            rows.append((sid, lab, vtype, dt, val))
    df = pd.DataFrame(rows, columns=["series_id", "label", "value_type", "PeriodEnd", "value"])
    df.sort_values(["label", "PeriodEnd"], inplace=True)
    return df


def year_bounds_from_params(start_ym: str, end_ym: Optional[str]) -> tuple[int, int]:
    start = pd.Period(start_ym, freq="M").to_timestamp(how="end")
    end = pd.Period(end_ym, freq="M").to_timestamp(how="end") if end_ym else pd.Timestamp.today().to_period("M").to_timestamp(how="end")
    return start.year, end.year


def build_changes(pivot_level: pd.DataFrame, vtype_by_col: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    vtype_by_col: {label: "level"/"rate"}
    - level: MoM_abs, MoM_pct, YoY_abs, YoY_pct
    - rate : MoM_pp, YoY_pp (퍼센트포인트)
    """
    out = {}
    # 공통 시프트
    lag1 = pivot_level.shift(1)
    lag12 = pivot_level.shift(12)

    # 레벨과 레이트 컬럼 분리
    level_cols = [c for c, t in vtype_by_col.items() if t == "level" and c in pivot_level.columns]
    rate_cols  = [c for c, t in vtype_by_col.items() if t == "rate"  and c in pivot_level.columns]

    if level_cols:
        lvl = pivot_level[level_cols]
        out["MoM_abs_level"] = (lvl - lag1[level_cols])
        out["MoM_pct_level"] = (lvl / lag1[level_cols] - 1.0) * 100.0
        out["YoY_abs_level"] = (lvl - lag12[level_cols])
        out["YoY_pct_level"] = (lvl / lag12[level_cols] - 1.0) * 100.0

    if rate_cols:
        rte = pivot_level[rate_cols]
        out["MoM_pp_rate"]  = (rte - lag1[rate_cols])          # 퍼센트포인트
        out["YoY_pp_rate"]  = (rte - lag12[rate_cols])         # 퍼센트포인트

    return out


def main():
    # 1) 시리즈 매핑
    series_map = read_series_map(SERIES_CSV)
    series_ids = [sid for sid, _, _ in series_map]
    labels = {sid: lab for sid, lab, _ in series_map}
    vtypes = {sid: vtype for sid, _, vtype in series_map}

    # 2) 연도 경계 (BLS는 연도 파라미터)
    start_year, end_year = year_bounds_from_params(START, END)

    # 3) 배치 호출 (공개 기준 50개 이내 권장)
    batch_size = 50
    frames = []
    for batch in chunked(series_ids, batch_size):
        data = call_bls(batch, start_year, end_year)
        frames.append(parse_bls_result(data, labels, vtypes))

    if not frames:
        raise RuntimeError("No data returned from BLS.")
    long_df = pd.concat(frames, ignore_index=True)

    # 4) 날짜 컷
    start_cut = pd.Period(START, freq="M").to_timestamp(how="end")
    end_cut = pd.Period(END, freq="M").to_timestamp(how="end") if END else None
    if end_cut is not None:
        long_df = long_df[(long_df["PeriodEnd"] >= start_cut) & (long_df["PeriodEnd"] <= end_cut)]
    else:
        long_df = long_df[long_df["PeriodEnd"] >= start_cut]

    # 5) 피벗(수준)
    pivot_level = (long_df
                   .pivot_table(index="PeriodEnd", columns="label", values="value")
                   .sort_index())

    # 6) 변동치 계산
    vtype_by_col = {labels[sid]: vtypes[sid] for sid in series_ids}
    changes = build_changes(pivot_level, vtype_by_col)

    # 7) 커버리지/메타
    coverage = (long_df.groupby(["label", "value_type"])["PeriodEnd"]
                .agg(start="min", end="max", rows="count")
                .reset_index()
                .sort_values(["value_type", "label"]))

    meta = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "BLS Public API v2 (https://api.bls.gov)",
        "start_param": START,
        "end_param": END or "latest",
        "series_count": len(series_ids),
        "has_api_key": bool(BLS_API_KEY),
        "notes": "Levels: absolute and % changes; Rates: changes in percentage points (pp). All series seasonally adjusted unless you change IDs.",
    }])

    # 8) 저장
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"{OUTFILE_PREFIX}_{today}.xlsx"
    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        long_df.to_excel(w, sheet_name="Raw_Long", index=False)
        pivot_level.to_excel(w, sheet_name="Pivot_Level")
        for name, df in changes.items():
            df.to_excel(w, sheet_name=name)
        coverage.to_excel(w, sheet_name="Coverage", index=False)
        meta.to_excel(w, sheet_name="Meta_Info", index=False)

    print(f"✅ Saved: {outfile}")


if __name__ == "__main__":
    main()
