# filename: export_btc_gold_fixed.py
# pip install yfinance pandas openpyxl curl_cffi pandas-datareader

from __future__ import annotations
import time
from datetime import datetime
from typing import Dict, Tuple, Optional
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr

START = "2010-01-01"
END = None
INTERVAL = "1d"
RETRIES = 3
BACKOFF = 1.6

BTC_SYMBOL = "BTC-USD"
GOLD_CANDIDATES = [
    ("XAUUSD=X", "GoldSpot"),   # 야후 현물 (지역/시점에 따라 404 가능)
    ("GC=F",     "GoldFut"),    # 야후 금 선물
    ("GLD",      "GoldETF"),    # 야후 금 ETF
]
FRED_GOLD_SERIES = ("GOLDAMGBD228NLBM", "GoldLBMA_AM")  # LBMA Gold Price AM (USD/oz)

def _flatten_single_ticker_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance가 ('Open','BTC-USD') 같은 MultiIndex를 반환할 때를 대비해
    상위 레벨(가격 컬럼명)만 사용하도록 평탄화한다.
    """
    if isinstance(df.columns, pd.MultiIndex):
        # 보통 level 0: Open/High/Low/Close/Adj Close/Volume
        #       level 1: ticker (단일 심볼 반복)
        try:
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        except Exception:
            # 혹시 구조가 다른 경우엔 문자열로 합쳐서 안전하게 처리
            df.columns = ["_".join([str(x) for x in col if x is not None]) for col in df.columns]
    return df

def fetch_yf(symbol: str, start: str, end: Optional[str], interval: str = "1d") -> pd.DataFrame:
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if df is None or df.empty:
                raise ValueError(f"No data for {symbol}")

            df.index.name = "Date"
            df = _flatten_single_ticker_columns(df)  # ★ 추가: MultiIndex → 단일 컬럼

            cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in df.columns]
            return df[cols] if cols else df
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
            else:
                raise last_err


def fetch_gold_with_fallback(start: str, end: Optional[str]) -> Tuple[str, pd.DataFrame]:
    # 1~3) 야후
    for sym, label in GOLD_CANDIDATES:
        try:
            print(f"  * Trying Yahoo: {sym} ({label})")
            df = fetch_yf(sym, start=start, end=end, interval=INTERVAL)
            return label, df
        except Exception as e:
            print(f"    - Failed: {sym} ({e})")
    # 4) FRED
    fred_id, label = FRED_GOLD_SERIES
    print(f"  * Trying FRED: {fred_id} ({label})")
    d = pdr.DataReader(fred_id, "fred", start=start, end=end)
    if d is None or d.empty:
        raise ValueError("No data from FRED for gold fallback.")
    d.index.name = "Date"
    d.columns = ["Price"]  # USD/oz
    return label, d

def to_monthly_last_close(daily: pd.DataFrame) -> pd.DataFrame:
    """
    월말(달력 기준) 마지막 값. 컬럼명은 항상 'MonthEnd'.
    FutureWarning 대응: 'M' -> 'ME'. 영업월말이 필요하면 'BM'로 교체.
    """
    if "Close" in daily.columns:
        base = daily[["Close"]]
    elif daily.shape[1] == 1:
        base = daily.copy()
    else:
        raise ValueError("Cannot determine close/price column for monthly resample.")
    monthly = base.resample("ME").last()    # 필요 시 'BM'
    monthly.columns = ["MonthEnd"]
    return monthly

def build_combined_close(all_data: Dict[str, Dict[str, pd.DataFrame]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    daily_frames, monthly_frames = [], []
    for sym, parts in all_data.items():
        if "Close" in parts["daily"].columns:
            d = parts["daily"][["Close"]].rename(columns={"Close": sym})
        else:
            only_col = parts["daily"].columns[0]
            d = parts["daily"][[only_col]].rename(columns={only_col: sym})
        m = parts["monthly"][["MonthEnd"]].rename(columns={"MonthEnd": sym})
        if d.empty or m.empty:
            raise ValueError(f"Empty table detected for {sym}: daily={d.empty}, monthly={m.empty}")
        daily_frames.append(d)
        monthly_frames.append(m)
    daily_combined = pd.concat(daily_frames, axis=1).sort_index()
    monthly_combined = pd.concat(monthly_frames, axis=1).sort_index()
    return daily_combined, monthly_combined

def main():
    print("Fetching BTC & Gold (with fallbacks)...")
    all_data: Dict[str, Dict[str, pd.DataFrame]] = {}

    # BTC
    print(f"- {BTC_SYMBOL}")
    btc_daily = fetch_yf(BTC_SYMBOL, start=START, end=END, interval=INTERVAL)
    btc_monthly = to_monthly_last_close(btc_daily)
    all_data["BTC"] = {"daily": btc_daily, "monthly": btc_monthly}

    # GOLD (폴백)
    gold_label, gold_daily = fetch_gold_with_fallback(start=START, end=END)
    gold_monthly = to_monthly_last_close(gold_daily)
    all_data[gold_label] = {"daily": gold_daily, "monthly": gold_monthly}

    # 통합
    daily_close, monthly_close = build_combined_close(all_data)

    # 메타
    meta_rows = []
    for sym, parts in all_data.items():
        d = parts["daily"]
        meta_rows.append({
            "asset": sym,
            "rows_daily": len(d),
            "start_daily": d.index.min().date().isoformat(),
            "end_daily": d.index.max().date().isoformat(),
            "columns": ", ".join(map(str, d.columns)),  # ★ tuple 방지
        })

    meta = pd.DataFrame(meta_rows)
    info = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "Yahoo Finance (yfinance) + FRED fallback",
        "interval": INTERVAL,
        "start_param": START,
        "end_param": END or "today",
        "notes": "Gold tried XAUUSD=X -> GC=F -> GLD -> FRED GOLDAMGBD228NLBM; Month-End uses 'ME'",
    }])

    # 저장
    today = datetime.now().strftime("%Y%m%d")
    outfile = f"btc_gold_fallback_{today}.xlsx"
    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        for sym, parts in all_data.items():
            parts["daily"].to_excel(w, sheet_name=f"{sym}_Daily")
            parts["monthly"].to_excel(w, sheet_name=f"{sym}_Monthly")
        daily_close.to_excel(w, sheet_name="Combined_Close_Daily")
        monthly_close.to_excel(w, sheet_name="Combined_Close_Monthly")
        meta.to_excel(w, sheet_name="Meta_Summary", index=False)
        info.to_excel(w, sheet_name="Meta_Info", index=False)

    print(f"✅ Saved: {outfile}")

if __name__ == "__main__":
    main()
