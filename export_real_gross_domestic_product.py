import pandas_datareader.data as web
import datetime as dt
import pandas as pd

# --- 설정 ---
# 가져올 시작 날짜 설정 (예: 1980년 1분기부터)
start_date = dt.datetime(1980, 1, 1)

# 끝 날짜 설정 (현재 날짜)
end_date = dt.datetime.now()

# FRED 데이터 시리즈 코드: A191RL1Q225SBEA (Real GDP, 분기별 연환산 변화율)
series_code = 'A191RL1Q225SBEA' 
data_source = 'fred'

# 저장할 엑셀 파일 이름
file_name = f'Real_GDP_Growth_Rate_{end_date.strftime("%Y%m%d")}.xlsx' 

try:
    # --- 데이터 가져오기 ---
    # GDP는 분기별로 발표되므로, 데이터가 분기별 날짜로 로드됩니다.
    gdp_growth_rate = web.DataReader(
        series_code, 
        data_source, 
        start_date, 
        end_date
    )
    
    # 컬럼 이름 변경 (더 직관적으로)
    gdp_growth_rate.columns = ['Real_GDP_Growth_Rate']
    
    # --- 데이터 엑셀로 저장 ---
    gdp_growth_rate.to_excel(file_name)
    
    print(f"✅ GDP 성장률 데이터 수집 및 엑셀 저장 완료!")
    print(f"   -> 파일명: **{file_name}**")
    print(f"   -> 데이터 개수: {len(gdp_growth_rate)}개 (분기별)")
    print("\n[최근 5개 데이터 확인]")
    print(gdp_growth_rate.tail())
    
except Exception as e:
    print(f"❌ 데이터 로드 또는 저장 중 오류 발생: {e}")
    print("-> 인터넷 연결 및 라이브러리 설치 여부를 확인해 주세요.")