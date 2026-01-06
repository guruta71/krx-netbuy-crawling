
import sys
import os
import datetime

# 프로젝트 루트의 src 디렉토리를 경로에 추가
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from infra.adapters.krx_http_adapter import KrxHttpAdapter
from core.domain.models import Market, Investor

def run_verification():
    print("=== KRX Hybrid Download Verification ===")
    
    try:
        adapter = KrxHttpAdapter()
        
        # 어제 날짜 구하기 (주말 고려 x, 단순 테스트용)
        # 만약 휴장일이면 0바이트일 수 있으나, 일단 연결 성공 여부 확인이 우선
        target_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        
        # 주말인 경우 금요일로 조정
        if datetime.date.today().weekday() == 0: # 월요일이면
            target_date = (datetime.date.today() - datetime.timedelta(days=3)).strftime('%Y%m%d')
        elif datetime.date.today().weekday() == 6: # 일요일이면
            target_date = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y%m%d')
            
        print(f"Target Date: {target_date}")
        
        # KOSPI, 외국인 데이터 수집 시도
        data = adapter.fetch_net_value_data(Market.KOSPI, Investor.FOREIGNER, target_date)
        
        print(f"\nVerification Result:")
        print(f"  - Downloaded Bytes: {len(data)}")
        
        if len(data) > 0:
            print("  - [SUCCESS] Data downloaded successfully.")
        else:
            print("  - [WARNING] Downloaded data is empty (0 bytes).")
            
    except Exception as e:
        print(f"\n  - [ERROR] Verification failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_verification()
