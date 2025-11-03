# filename: export_treasury_yields.py
# 기능:
# - FRED에서 DGS1(1Y), DGS2(2Y), DGS10(10Y), DGS20(20Y) 일별 금리(%) 수집
# - 결측치 전파(ffill) 및 영업일 캘린더 정렬(선택)
# - 월 평균/영업월말(BM) 집계
# - 엑셀 시트: 개별, 통합(일/월평균/월말), 메타 저장

from __future__ import annotations
import time
from typing import Dict, List
from datetime import datetime
import pandas as pd
from pandas_datareader import data as pdr

START = "2010-01-01"
END = None  # None이면 오늘까지
RETRIES = 3
BACKOFF = 1.6
ALIGN_TO_BUSINESS_DAYS = True  # True면 영업일 캘린더로 리인덱스 후 ffill

# FRED series id 매핑 (단위: % per annum)
SERIES_MAP: Dict[str, str] = {
    "1Y": "DGS1",
    "2Y": "DGS2",
    "10Y": "DGS10",
    "20Y": "DGS20",
}

def fetch_fred_series(series_id: str, start: str, end: str | None,
                      retries: int = RETRIES, backoff: float = BACKOFF) -> pd.DataFrame:
    """FRED 일별 시계열(%) 다운로드 + 정리. 컬럼명은 series_id로."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            df = pdr.DataReader(series_id, "fred", start=start, end=end)
            if df is None or df.empty:
                raise ValueError(f"No data for {series_id}")
            df.index.name = "Date"
            df.columns = [series_id]
            return df
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                raise last_err

def align_business_days_ffill(df: pd.DataFrame) -> pd.DataFrame:
    """영업일(B) 기준으로 리인덱스 후 결측을 직전값으로 채움."""
    all_days = pd.date_range(df.index.min(), df.index.max(), freq="B")
    df = df.reindex(all_days).ffill()
    df.index.name = "Date"
    return df

def to_monthly_agg(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    월 평균과 영업월말(BM) 마지막 값 산출.
    - 월 평균: resample('M').mean()
    - 영업월말: resample('BM').last()
    """
    monthly_mean = df.resample("M").mean().rename(columns=lambda c: f"{c}_MAVG")
    monthly_bm = df.resample("BM").last().rename(columns=lambda c: f"{c}_MBE")  # Month Business End
    return monthly_mean, monthly_bm

def main():
    print("Fetching US Treasury yields from FRED...")
    # 개별 시리즈 수집
    frames: List[pd.DataFrame] = []
    for tenor, sid in SERIES_MAP.items():
        print(f"  - {tenor}: {sid}")
        d = fetch_fred_series(sid, START, END)
        frames.append(d)

    # 일간 통합 (열: DGS1, DGS2, DGS10, DGS20)
    daily = pd.concat(frames, axis=1).sort_index()

    # 선택: 영업일 캘린더로 정렬(주말/휴일 ffill)
    if ALIGN_TO_BUSINESS_DAYS:
        daily = align_business_days_ffill(daily)

    # 월 평균 / 영업월말
    monthly_mean_list = []
    monthly_bm_list = []
    for col in daily.columns:
        mavg, mbe = to_monthly_agg(daily[[col]])
        monthly_mean_list.append(mavg)
        monthly_bm_list.append(mbe)
    monthly_mean = pd.concat(monthly_mean_list, axis=1).sort_index()
    monthly_bm = pd.concat(monthly_bm_list, axis=1).sort_index()

    # 메타
    meta_summary = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "FRED via pandas-datareader",
        "series": ", ".join([f"{k}:{v}" for k, v in SERIES_MAP.items()]),
        "unit": "Percent per annum",
        "start_param": START,
        "end_param": END or "today",
        "align_to_business_days": ALIGN_TO_BUSINESS_DAYS,
        "rows_daily": len(daily),
        "rows_monthly_mean": len(monthly_mean),
        "rows_monthly_bm": len(monthly_bm),
        "notes": "Monthly mean = resample('M').mean(); Month-end(BM)=resample('BM').last()"
    }])

    # 저장
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"us_treasury_yields_{today}.xlsx"
    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        # 개별 시트 (원본 일간)
        for tenor, sid in SERIES_MAP.items():
            daily[[sid]].to_excel(w, sheet_name=f"{tenor}_Daily")
        # 통합 시트
        daily.to_excel(w, sheet_name="Combined_Daily")
        monthly_mean.to_excel(w, sheet_name="Combined_Monthly_MAVG")
        monthly_bm.to_excel(w, sheet_name="Combined_Monthly_MBE")
        # 메타
        meta_summary.to_excel(w, sheet_name="Meta", index=False)

    print(f"✅ Saved: {outfile}")
    print("Sheets: " +
          ", ".join([f"{t}_Daily" for t in SERIES_MAP.keys()]) +
          ", Combined_Daily, Combined_Monthly_MAVG, Combined_Monthly_MBE, Meta")

if __name__ == "__main__":
    main()
