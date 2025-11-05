import pandas_datareader.data as web
import datetime as dt
import pandas as pd

# --- 설정 ---
# 가져올 시작 날짜 설정 (예: 2000년 1월 1일부터)
start_date = dt.datetime(2000, 1, 1)

# 끝 날짜 설정 (현재 날짜)
end_date = dt.datetime.now()

# FRED 데이터 시리즈 코드: DFF (Federal Funds Effective Rate)
series_code = 'DFF'
data_source = 'fred'

# 저장할 엑셀 파일 이름
# 파일명에 날짜를 포함시켜 중복을 방지할 수 있습니다.
file_name = f'Fed_Funds_Rate_{end_date.strftime("%Y%m%d")}.xlsx' 

try:
    # --- 데이터 가져오기 ---
    fed_funds_rate = web.DataReader(
        series_code, 
        data_source, 
        start_date, 
        end_date
    )
    
    # 컬럼 이름 변경 (더 직관적으로)
    fed_funds_rate.columns = ['Federal_Funds_Rate']
    
    # --- 데이터 엑셀로 저장 ---
    # to_excel 함수를 사용하여 엑셀 파일로 저장합니다.
    fed_funds_rate.to_excel(file_name)
    
    print(f"✅ 연준 기준금리 데이터 수집 및 엑셀 저장 완료!")
    print(f"   -> 파일명: **{file_name}**")
    print(f"   -> 데이터 개수: {len(fed_funds_rate)}개")
    
except Exception as e:
    print(f"❌ 데이터 로드 또는 저장 중 오류 발생: {e}")
    print("-> 필요한 라이브러리(pandas, pandas-datareader, openpyxl) 설치 여부를 확인해 주세요.")