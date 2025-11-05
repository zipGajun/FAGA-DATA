# -*- coding: utf-8 -*-
"""
산업생산지수 (Industrial Production Index, INDPRO) 데이터를
FRED에서 가져와 Excel로 저장하는 코드
"""

import os
import pandas as pd
from pandas_datareader import data as pdr

# FRED API Key 설정 (필요 시 직접 입력)
# os.environ["FRED_API_KEY"] = "YOUR_FRED_API_KEY"

# -----------------------------
# 설정
# -----------------------------
FRED_SERIES = "INDPRO"             # 산업생산지수 코드
START_DATE = "1990-01-01"
OUTPUT_FILE = "industrial_production.xlsx"

# -----------------------------
# 데이터 수집
# -----------------------------
try:
    df = pdr.DataReader(FRED_SERIES, "fred", start=START_DATE)
    df = df.rename(columns={FRED_SERIES: "Industrial_Production_Index"})
    df.index.name = "Date"
    print("✅ 산업생산 데이터 다운로드 완료")
except Exception as e:
    print(f"❌ 데이터 수집 실패: {e}")
    raise

# -----------------------------
# 엑셀 저장
# -----------------------------
try:
    df.to_excel(OUTPUT_FILE)
    print(f"✅ '{OUTPUT_FILE}' 파일로 저장 완료")
except Exception as e:
    print(f"❌ 엑셀 저장 실패: {e}")
    raise

# -----------------------------
# 데이터 확인
# -----------------------------
print(df.tail())
