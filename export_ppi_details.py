# filename: export_ppi_details.py
# 기능:
# - BLS(미국 노동통계국) API에서 PPI(생산자물가지수) 상세 데이터 수집
# - MoM(전월 대비), YoY(전년 동기 대비) 변동률 계산
# - 엑셀 파일로 결과 저장 (시트: Raw, Level, MoM, YoY, 요약 정보)

from __future__ import annotations
import os
import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd

# =========================
# 사용자 설정
# =========================
START = "2010-01"   # YYYY-MM (월 단위)
END   = None        # None -> 사용 가능한 최신 데이터까지
RETRIES = 3
BACKOFF = 1.6
OUTFILE_PREFIX = "ppi_details"

# 시리즈 매핑 CSV(선택). 없으면 아래 DEFAULT_SERIES_MAP 사용
SERIES_CSV = "ppi_series_map.csv"
# CSV 형식: series_id,label
# 예)
# WPSFD4,PPI Final Demand
# PCUOMFG--OMFG--,PPI All Manufacturing

# CSV가 없을 때 사용하는 기본 PPI 매핑 (원하는 시리즈 ID를 추가/변경하세요)
# 주요 PPI ID는 https://www.bls.gov/ppi/data.htm 에서 찾을 수 있습니다.
DEFAULT_SERIES_MAP: List[Tuple[str, str]] = [
    ("WPSFD4", "PPI Final Demand"),
    ("WPSFD41", "PPI Final Demand Goods"),
    ("WPSFD49", "PPI Final Demand Services"),
    ("PCUOMFG--OMFG--", "PPI All Manufacturing"),
]

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data"
BLS_API_KEY = os.getenv("BLS_API_KEY")  # 시스템 환경변수에 BLS_API_KEY가 있으면 자동 사용


# =========================
# 유틸리티 함수 (CPI 스크립트와 동일)
# =========================
def read_series_map(csv_path: str) -> List[Tuple[str, str]]:
    """지정된 CSV 파일에서 Series ID와 Label 매핑을 읽어옵니다."""
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        required = {"series_id", "label"}
        if not required.issubset(set(c.lower() for c in df.columns)):
            raise ValueError(f"{csv_path} must have columns: series_id, label")
        df.columns = [c.lower() for c in df.columns]
        series_map = [(str(r["series_id"]).strip(), str(r["label"]).strip()) for _, r in df.iterrows()]
        if not series_map:
            raise ValueError(f"{csv_path} is empty.")
        return series_map
    return DEFAULT_SERIES_MAP

def chunked(lst: list, n: int):
    """리스트를 n개 크기의チャン크로 나눕니다."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def bls_payload(series_ids: List[str], start_year: int, end_year: int) -> Dict:
    """BLS API 요청을 위한 payload를 생성합니다."""
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
    """BLS API를 호출하고 결과를 JSON으로 반환합니다."""
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(BLS_API_URL, json=bls_payload(series_ids, start_year, end_year), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                raise RuntimeError(f"BLS API Error: {data.get('message')}")
            return data
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
            else:
                raise last_err

def parse_bls_result(data: Dict, labels: Dict[str, str]) -> pd.DataFrame:
    """BLS API 응답을 DataFrame으로 파싱합니다."""
    rows = []
    for ts in data.get("Results", {}).get("series", []):
        sid = ts.get("seriesID")
        lab = labels.get(sid, sid)
        for item in ts.get("data", []):
            year = int(item["year"])
            period = item["period"]
            if not period.startswith("M") or period == "M13": continue # 월별 데이터만 사용
            month = int(period[1:])
            value = float(item["value"])
            dt = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            rows.append((sid, lab, dt, value))
    df = pd.DataFrame(rows, columns=["series_id", "label", "PeriodEnd", "value"])
    df.sort_values(["label", "PeriodEnd"], inplace=True)
    return df

def compute_pct_changes(pivot_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """지수 수준(level) DataFrame에서 MoM, YoY 변동률을 계산합니다."""
    mom = (pivot_df / pivot_df.shift(1) - 1.0) * 100.0
    yoy = (pivot_df / pivot_df.shift(12) - 1.0) * 100.0
    return mom, yoy

def year_bounds_from_params(start_ym: str, end_ym: Optional[str]) -> Tuple[int, int]:
    """YYYY-MM 형식의 시작/종료일로부터 연도 범위를 계산합니다."""
    start_dt = pd.to_datetime(start_ym)
    end_dt = pd.to_datetime(end_ym) if end_ym else pd.Timestamp.today()
    return start_dt.year, end_dt.year

def main():
    print("Fetching PPI details from BLS API...")
    # 1. 시리즈 ID와 라벨 매핑 읽기
    series_map = read_series_map(SERIES_CSV)
    series_ids = [sid for sid, _ in series_map]
    labels = {sid: lab for sid, lab in series_map}
    print(f"Series to fetch: {', '.join(series_ids)}")

    # 2. API 호출을 위한 연도 범위 계산
    start_year, end_year = year_bounds_from_params(START, END)

    # 3. API 배치 호출 (API 키 없으면 50개, 있으면 50개 제한)
    batch_size = 50
    long_frames = []
    for batch in chunked(series_ids, batch_size):
        print(f"  - Fetching batch of {len(batch)} series...")
        data = call_bls(batch, start_year, end_year)
        df_long = parse_bls_result(data, labels)
        long_frames.append(df_long)

    if not long_frames:
        raise RuntimeError("No data returned from BLS.")
    long_df = pd.concat(long_frames, ignore_index=True)

    # 4. 피벗 (Level) 및 변동률 계산
    pivot_level = long_df.pivot_table(index="PeriodEnd", columns="label", values="value").sort_index()
    mom_pct, yoy_pct = compute_pct_changes(pivot_level)

    # 5. 메타데이터 생성
    meta_info = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "BLS Public API v2 (https://api.bls.gov)",
        "start_param": START,
        "end_param": END or "latest",
        "series_count": len(series_ids),
        "has_api_key": bool(BLS_API_KEY),
        "notes": "Values are PPI index levels; MoM% and YoY% are computed from levels.",
    }])
    coverage = (long_df.groupby("label")["PeriodEnd"]
                .agg(start="min", end="max", rows="count")
                .reset_index()
                .sort_values("label"))

    # 6. 엑셀 파일로 저장
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"{OUTFILE_PREFIX}_{today}.xlsx"
    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        long_df.to_excel(w, sheet_name="Raw_Long", index=False)
        pivot_level.to_excel(w, sheet_name="Pivot_Level")
        mom_pct.to_excel(w, sheet_name="MoM_pct")
        yoy_pct.to_excel(w, sheet_name="YoY_pct")
        coverage.to_excel(w, sheet_name="Coverage", index=False)
        meta_info.to_excel(w, sheet_name="Meta_Info", index=False)

    print(f"✅ Saved: {outfile}")

if __name__ == "__main__":
    main()