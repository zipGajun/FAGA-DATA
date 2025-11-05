from __future__ import annotations
import os
import json
import math
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

# =========================
# 사용자 설정
# =========================
START = "2000-01"   # YYYY-MM (월 단위)
END   = None        # None -> latest available
RETRIES = 3
BACKOFF = 1.6
OUTFILE_PREFIX = "cpi_details"

# 시리즈 매핑 CSV(선택). 없으면 아래 DEFAULT_SERIES_MAP 사용
SERIES_CSV = "cpi_series_map.csv"
# CSV 형식: series_id,label
# 예)
# CUSR0000SA0,All items (SA)
# CUSR0000SA0L1E,Core CPI ex. Food & Energy (SA)

# CSV가 없을 때 사용하는 기본 매핑 (필요 시 여기에 원하는 시리즈를 추가하세요)
DEFAULT_SERIES_MAP: List[Tuple[str, str]] = [
    ("CUSR0000SA0", "All items (SA)"),
    ("CUSR0000SA0L1E", "Core CPI ex. Food & Energy (SA)"),
]

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data"
BLS_API_KEY = os.getenv("BLS_API_KEY")  # 있으면 자동 사용


# =========================
# 유틸
# =========================
def read_series_map(csv_path: str) -> List[Tuple[str, str]]:
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        required = {"series_id", "label"}
        missing = required - set(c.lower() for c in df.columns)
        # 컬럼 대소문자 유연 처리
        if missing:
            # 최소한 series_id, label 두 컬럼이 있어야 함
            raise ValueError(f"{csv_path} must have columns: series_id,label")
        # 컬럼명 소문자로 통일
        df.columns = [c.lower() for c in df.columns]
        series_map = [(str(r["series_id"]).strip(), str(r["label"]).strip()) for _, r in df.iterrows()]
        if not series_map:
            raise ValueError(f"{csv_path} is empty.")
        return series_map
    return DEFAULT_SERIES_MAP


def chunked(lst, n):
    """리스트를 n개 크기의 묶음으로 나눕니다."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def bls_payload(series_ids: List[str], start_year: int, end_year: int) -> Dict:
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": BLS_API_KEY
    }
    # 키가 없으면 제거(공개 쿼리)
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
            status = data.get("status")
            if status != "REQUEST_SUCCEEDED":
                raise RuntimeError(f"BLS API not succeeded: {data.get('message')}")
            return data
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
            else:
                raise last_err


def parse_bls_result(data: Dict, labels: Dict[str, str]) -> pd.DataFrame:
    """
    BLS 응답(JSON) -> Long DataFrame [series_id, label, date(PeriodEnd), value(Float)]
    BLS 월 데이터는 'year'와 'period'('M01'~'M12')로 제공됨.
    """
    rows = []
    for ts in data.get("Results", {}).get("series", []):
        sid = ts.get("seriesID")
        lab = labels.get(sid, sid)
        for item in ts.get("data", []):
            year = int(item["year"])
            period = item["period"]  # e.g., 'M01'
            if not period.startswith("M"):
                # BLS는 'M13' 같은 연간 평균(Annual avg)도 있음 -> 스킵
                continue
            month = int(period[1:])
            value = float(item["value"].replace(",", ""))
            # PeriodEnd 날짜: 해당 월의 마지막 날로 설정
            dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            rows.append((sid, lab, dt, value))
    df = pd.DataFrame(rows, columns=["series_id", "label", "PeriodEnd", "value"])
    df.sort_values(["label", "PeriodEnd"], inplace=True)
    return df


def compute_pct_changes(pivot_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    pivot_df: index=PeriodEnd, columns=label, values=value(지수 수준)
    MoM%: (t / t-1 - 1) * 100
    YoY%: (t / t-12 - 1) * 100
    """
    mom = (pivot_df / pivot_df.shift(1) - 1.0) * 100.0
    yoy = (pivot_df / pivot_df.shift(12) - 1.0) * 100.0
    return mom, yoy


def year_bounds_from_params(start_ym: str, end_ym: Optional[str]) -> Tuple[int, int]:
    start = pd.Period(start_ym, freq="M").to_timestamp(how="end")
    if end_ym:
        end = pd.Period(end_ym, freq="M").to_timestamp(how="end")
    else:
        end = pd.Timestamp.today().to_period("M").to_timestamp(how="end")
    return start.year, end.year


def main():
    # 1) 시리즈 매핑 읽기
    series_map = read_series_map(SERIES_CSV)
    series_ids = [sid for sid, _ in series_map]
    labels = {sid: lab for sid, lab in series_map}

    # 2) 연도 경계 계산 (BLS는 연도 단위 파라미터)
    start_year, end_year = year_bounds_from_params(START, END)

    # 3) 배치 호출 (공개 엔드포인트는 최대 50개 권장)
    #    키가 없어도 50개 이하로 쪼개서 요청
    batch_size = 50
    long_frames = []
    print(f"Fetching data in batches of {batch_size}...")
    for batch in chunked(series_ids, batch_size):
        data = call_bls(batch, start_year, end_year)
        df_long = parse_bls_result(data, labels)
        long_frames.append(df_long)

    if not long_frames:
        raise RuntimeError("No data returned from BLS.")
    long_df = pd.concat(long_frames, ignore_index=True)

    # 4) 날짜 필터 (START~END 월 범위로 컷)
    start_cut = pd.Period(START, freq="M").to_timestamp(how="end")
    end_cut = pd.Period(END, freq="M").to_timestamp(how="end") if END else None
    if end_cut is not None:
        long_df = long_df[(long_df["PeriodEnd"] >= start_cut) & (long_df["PeriodEnd"] <= end_cut)]
    else:
        long_df = long_df[long_df["PeriodEnd"] >= start_cut]

    # 5) 피벗 (수준)
    pivot_level = long_df.pivot_table(index="PeriodEnd", columns="label", values="value").sort_index()

    # 6) 변동률
    mom_pct, yoy_pct = compute_pct_changes(pivot_level)

    # 7) 메타/요약
    meta = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "BLS Public API v2 (https://api.bls.gov)",
        "start_param": START,
        "end_param": END or "latest",
        "series_count": len(series_ids),
        "has_api_key": bool(BLS_API_KEY),
        "notes": "Values are CPI index levels; MoM% and YoY% are computed from levels.",
    }])

    coverage = (long_df.groupby("label")["PeriodEnd"]
                .agg(start="min", end="max", rows="count")
                .reset_index()
                .sort_values("label"))

    # 8) 저장
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"{OUTFILE_PREFIX}_{today}.xlsx"
    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        long_df.to_excel(w, sheet_name="Raw_Long", index=False)
        pivot_level.to_excel(w, sheet_name="Pivot_Level")
        mom_pct.to_excel(w, sheet_name="MoM_pct")
        yoy_pct.to_excel(w, sheet_name="YoY_pct")
        coverage.to_excel(w, sheet_name="Coverage", index=False)
        meta.to_excel(w, sheet_name="Meta_Info", index=False)

    print(f"✅ Saved: {outfile}")


if __name__ == "__main__":
    main()
