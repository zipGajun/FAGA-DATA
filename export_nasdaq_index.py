# pip install yfinance pandas openpyxl curl_cffi

import time
from datetime import datetime
import pandas as pd
import yfinance as yf

'''
🟢 1. ^IXIC — 나스닥 종합지수 (NASDAQ Composite)
🔵 2. ^NDX — 나스닥 100 (NASDAQ-100)
🟣 3. SPY — S&P 500 ETF (SPDR S&P 500 ETF Trust)
🔴 4. ^GSPC — S&P 500 지수 (S&P 500 Index)
'''
SYMBOLS = ["^IXIC", "^NDX"]  # 필요시 확장: ["^IXIC", "^NDX", "SPY", "^GSPC"]
START = "2010-01-01"
END = None  # None = 오늘
INTERVAL = "1d"

def fetch_index(symbol: str, start: str, end: str | None, interval: str = "1d", retries: int = 3, backoff: float = 1.5) -> pd.DataFrame:
    """
    yfinance에서 지수 시세 다운로드 (세션 인자 사용 금지).
    auto_adjust=True 로 분할/배당 반영.
    간헐적 네트워크 오류를 대비해 재시도.
    """
    for attempt in range(1, retries + 1):
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
                raise ValueError(f"No data for {symbol} (attempt={attempt}).")
            df.index.name = "Date"
            # 필요한 컬럼만 정리
            cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in df.columns]
            return df[cols]
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(backoff ** attempt)

def to_monthly_last_close(daily: pd.DataFrame) -> pd.DataFrame:
    monthly = daily.resample("M").last()
    return monthly[["Close"]].rename(columns={"Close": "Close_MonthEnd"})

def fetch_all_indices(symbols, start, end) -> dict[str, dict[str, pd.DataFrame]]:
    results: dict[str, dict[str, pd.DataFrame]] = {}
    for symbol in symbols:
        print(f"Fetching {symbol}...")
        daily_df = fetch_index(symbol, start=start, end=end, interval=INTERVAL)
        monthly_df = to_monthly_last_close(daily_df)
        results[symbol] = {"daily": daily_df, "monthly": monthly_df}
    return results

def main():
    today_str = datetime.now().strftime("%Y%m%d")
    outfile = f"nasdaq_indices_full_{today_str}.xlsx"

    all_data = fetch_all_indices(SYMBOLS, start=START, end=END)

    # 메타 요약
    meta_rows = []
    for sym, parts in all_data.items():
        meta_rows.append({
            "symbol": sym,
            "rows_daily": len(parts["daily"]),
            "rows_monthly": len(parts["monthly"]),
            "start_date_daily": parts["daily"].index.min().date().isoformat(),
            "end_date_daily": parts["daily"].index.max().date().isoformat(),
        })
    meta_df = pd.DataFrame(meta_rows)
    meta_info = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "Yahoo Finance via yfinance",
        "interval": INTERVAL,
        "start_param": START,
        "end_param": END or "today",
        "notes": "auto_adjust=True; monthly = resample('M').last()",
    }])

    with pd.ExcelWriter(outfile, engine="openpyxl") as w:
        # 각 심볼별 시트
        for sym, parts in all_data.items():
            parts["daily"].to_excel(w, sheet_name=f"{sym}_Daily")
            parts["monthly"].to_excel(w, sheet_name=f"{sym}_Monthly")
        meta_df.to_excel(w, sheet_name="Meta_Summary", index=False)
        meta_info.to_excel(w, sheet_name="Meta_Info", index=False)

    print(f"✅ Saved: {outfile}")

if __name__ == "__main__":
    main()
