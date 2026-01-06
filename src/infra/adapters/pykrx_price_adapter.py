"""pykrx 기반 가격 조회 어댑터"""

from pykrx import stock
from datetime import datetime, timedelta
from typing import Optional

from core.ports.price_data_port import PriceDataPort, StockPriceInfo


class PykrxPriceAdapter(PriceDataPort):
    """pykrx를 사용한 가격 데이터 조회 어댑터
    
    pykrx 라이브러리를 통해 KRX 데이터를 조회하여
    종목의 종가, 52주 신고가, 역사적 신고가를 제공합니다.
    """
    
    def __init__(self):
        """PykrxPriceAdapter 초기화"""
        print("[Adapter:PykrxPrice] 초기화 완료")
    
    def get_price_info(self, ticker: str, date_str: str) -> Optional[StockPriceInfo]:
        """종목의 가격 정보를 조회합니다.
        
        Args:
            ticker (str): 종목코드 (6자리, 예: '005930')
            date_str (str): 조회 날짜 (YYYYMMDD 형식, 예: '20250102')
            
        Returns:
            Optional[StockPriceInfo]: 가격 정보, 조회 실패 시 None
        """
        try:
            print(f"  [Adapter:PykrxPrice] {ticker} 가격 정보 조회 시작 ({date_str})...")
            
            target_date_dt = datetime.strptime(date_str, "%Y%m%d")
            # 역사적 신고가는 1990년 1월 1일부터 조회
            start_date_hist = "19900101"
            
            # API 호출 최적화: 한 번에 과거 데이터 조회
            df = stock.get_market_ohlcv(start_date_hist, date_str, ticker)
            
            if df is None or df.empty:
                print(f"  [Adapter:PykrxPrice] {ticker} 데이터 없음")
                return None
            
            # 종가 (오늘 데이터)
            # 마지막 행을 기준일자로 가정
            close_price = float(df['종가'].iloc[-1])
            
            # 기준 데이터 (오늘 제외)
            df_past = df.iloc[:-1]
            
            if df_past.empty:
                # 과거 데이터가 없으면(상장 첫날 등) 0으로 설정하여 무조건 신고가 달성 처리
                high_52w = 0.0
                all_time_high = 0.0
            else:
                # 역사적 신고가 (과거 전체 고가 중 최고가)
                all_time_high = float(df_past['고가'].max())
                
                # 52주 신고가 (과거 52주 고가 중 최고가)
                start_date_52w = target_date_dt - timedelta(weeks=52)
                # DataFrame 인덱스가 datetime 형식이므로 직접 비교 가능
                df_52w = df_past[df_past.index >= start_date_52w]
                
                if not df_52w.empty:
                    high_52w = float(df_52w['고가'].max())
                else:
                    # 52주 데이터가 없으면(예: 상장 1달차) 역사적 고가 사용 
                    high_52w = all_time_high

            print(f"  [Adapter:PykrxPrice] OK: {ticker} 조회 완료 (종가: {close_price:,.0f}, 전일기준 52주고가: {high_52w:,.0f}, 전일기준 역사적고가: {all_time_high:,.0f})")
            
            return StockPriceInfo(
                ticker=ticker,
                close_price=close_price,
                high_52w=high_52w,
                all_time_high=all_time_high
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  [Adapter:PykrxPrice] ERROR: {ticker} 가격 조회 중 오류: {e}")
            return None
