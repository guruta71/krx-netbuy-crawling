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
            
            # 1. 해당 날짜의 종가 조회
            close_price = self._get_close_price(ticker, date_str)
            if close_price is None:
                print(f"  [Adapter:PykrxPrice] {ticker} 종가 조회 실패")
                return None
            
            # 2. 52주 신고가 조회
            high_52w = self._get_52w_high(ticker, date_str)
            if high_52w is None:
                print(f"  [Adapter:PykrxPrice] {ticker} 52주 신고가 조회 실패")
                return None
            
            # 3. 역사적 신고가 조회 (최근 10년으로 제한)
            all_time_high = self._get_all_time_high(ticker, date_str)
            if all_time_high is None:
                print(f"  [Adapter:PykrxPrice] {ticker} 역사적 신고가 조회 실패")
                return None
            
            print(f"  [Adapter:PykrxPrice] ✅ {ticker} 조회 완료 (종가: {close_price:,}, 52주: {high_52w:,}, 역사적: {all_time_high:,})")
            
            return StockPriceInfo(
                ticker=ticker,
                close_price=close_price,
                high_52w=high_52w,
                all_time_high=all_time_high
            )
            
        except Exception as e:
            print(f"  [Adapter:PykrxPrice] 🚨 {ticker} 가격 조회 중 오류: {e}")
            return None
    
    def _get_close_price(self, ticker: str, date_str: str) -> Optional[float]:
        """해당 날짜의 종가를 조회합니다."""
        try:
            df = stock.get_market_ohlcv(date_str, date_str, ticker)
            if df.empty:
                return None
            return float(df['종가'].iloc[0])
        except Exception as e:
            print(f"  [Adapter:PykrxPrice] 종가 조회 오류: {e}")
            return None
    
    def _get_52w_high(self, ticker: str, date_str: str) -> Optional[float]:
        """52주(365일) 신고가를 조회합니다."""
        try:
            # 365일 전 날짜 계산
            target_date = datetime.strptime(date_str, "%Y%m%d")
            start_date = (target_date - timedelta(days=365)).strftime("%Y%m%d")
            
            df = stock.get_market_ohlcv(start_date, date_str, ticker)
            if df.empty:
                return None
            return float(df['고가'].max())
        except Exception as e:
            print(f"  [Adapter:PykrxPrice] 52주 신고가 조회 오류: {e}")
            return None
    
    def _get_all_time_high(self, ticker: str, date_str: str) -> Optional[float]:
        """역사적 신고가를 조회합니다 (최근 10년으로 제한)."""
        try:
            # 10년(3650일) 전 날짜 계산
            target_date = datetime.strptime(date_str, "%Y%m%d")
            start_date = (target_date - timedelta(days=3650)).strftime("%Y%m%d")
            
            df = stock.get_market_ohlcv(start_date, date_str, ticker)
            if df.empty:
                return None
            return float(df['고가'].max())
        except Exception as e:
            print(f"  [Adapter:PykrxPrice] 역사적 신고가 조회 오류: {e}")
            return None
