# -*- coding: utf-8 -*-
"""
M2 통화량 데이터를 FRED에서 가져와 Excel로 저장하는 코드
"""

import os
import pandas as pd
from pandas_datareader import data as pdr

# FRED API Key 설정 (https://fred.stlouisfed.org/docs/api/fred/)
# FRED API 키를 환경변수에 등록해두면 더 편합니다.
# 예시: os.environ["FRED_API_KEY"] = "YOUR_FRED_API_KEY"

# -----------------------------
# 설정
# -----------------------------
FRED_SERIES = "M2SL"          # M2 통화량 (시즌별 조정된 월간 데이터)
START_DATE = "1990-01-01"     # 시작일
OUTPUT_FILE = "M2_money_supply.xlsx"

# -----------------------------
# 데이터 수집
# -----------------------------
try:
    df = pdr.DataReader(FRED_SERIES, "fred", start=START_DATE)
    df = df.rename(columns={FRED_SERIES: "M2_Money_Supply"})
    df.index.name = "Date"
    print("✅ 데이터 다운로드 완료")
except Exception as e:
    print(f"❌ FRED 데이터 수집 실패: {e}")
    raise

# -----------------------------
# 엑셀로 저장
# -----------------------------
try:
    df.to_excel(OUTPUT_FILE)
    print(f"✅ '{OUTPUT_FILE}' 파일로 저장 완료")
except Exception as e:
    print(f"❌ 엑셀 저장 실패: {e}")
    raise

# -----------------------------
# 데이터 미리보기
# -----------------------------
print(df.tail())
