# pip install python-dotenv fredapi pandas openpyxl

from fredapi import Fred
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

# ✅ FRED에서 존재 확인된 CPI 시리즈 (시즌조정 SA 기준)
series_dict = {
    "CPIAUCSL": "All Items (Headline CPI, SA)",
    "CPILFESL": "Core CPI (Ex. Food & Energy, SA)",
    "CPIFABSL": "Food & Beverages (SA)",
    "CPIENGSL": "Energy (SA)",
    "CPIHOSSL": "Housing (SA)",
    "CPIAPPSL": "Apparel (SA)",
    "CPITRNSL": "Transportation (SA)",
    "CPIMEDSL": "Medical Care (SA)",
    "CPIRECSL": "Recreation (SA)",
    "CPIEDUSL": "Education & Communication (SA)",
    "CPIOGSSL": "Other Goods & Services (SA)",
}

frames = []
failed = {}

for code, label in series_dict.items():
    try:
        s = fred.get_series(code)
        df = s.to_frame(name=label)
        df.index.name = "Date"
        frames.append(df)
    except Exception as e:
        failed[code] = str(e)

# 병합
result = pd.concat(frames, axis=1).sort_index()

# YoY(%) 계산
yoy = result.pct_change(12) * 100
yoy.columns = [c + " YoY(%)" for c in result.columns]

out = pd.concat([result, yoy], axis=1)

# 엑셀 저장 (시트 분리: 값/YoY/로그)
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"cpi_detailed_{today}.xlsx"
with pd.ExcelWriter(file_name, engine="openpyxl") as w:
    result.to_excel(w, sheet_name="Index(SA)")
    yoy.to_excel(w, sheet_name="YoY(%)")
    if failed:
        pd.DataFrame.from_dict(failed, orient="index", columns=["error"]).to_excel(w, sheet_name="Skipped")

print(f"✅ 엑셀 저장 완료: {file_name}")
if failed:
    print("⚠️ 스킵된 시리즈:", failed)
