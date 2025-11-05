# -*- coding: utf-8 -*-
"""
M2 대체지표 (RRPONTSYD, WALCL 등) 일별 데이터 수집 예시
"""

import pandas as pd
from pandas_datareader import data as pdr
import os

# os.environ["FRED_API_KEY"] = "YOUR_FRED_API_KEY"

start = "2020-01-01"

series = {
    "RRPONTSYD": "Reverse_Repo",
    "WALCL": "Fed_Balance_Sheet",
}

dfs = []
for code, name in series.items():
    df = pdr.DataReader(code, "fred", start=start)
    df = df.rename(columns={code: name})
    dfs.append(df)

df_all = pd.concat(dfs, axis=1)
df_all.to_excel("daily_liquidity_proxies.xlsx")

print("✅ 일별 대체 유동성 데이터 저장 완료")
print(df_all.tail())
