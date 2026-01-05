"""가격 데이터 조회를 위한 Port 인터페이스"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class StockPriceInfo:
    """종목 가격 정보
    
    Attributes:
        ticker (str): 종목코드 (6자리)
        close_price (float): 종가
        high_52w (float): 52주 신고가
        all_time_high (float): 역사적 신고가
    """
    ticker: str
    close_price: float
    high_52w: float
    all_time_high: float
    
    # 근접 판정 기준: 신고가의 90% 이상
    NEAR_THRESHOLD = 0.90
    
    @property
    def is_52w_high(self) -> bool:
        """52주 신고가 여부"""
        return self.close_price >= self.high_52w
    
    @property
    def is_all_time_high(self) -> bool:
        """역사적 신고가 여부"""
        return self.close_price >= self.all_time_high
    
    @property
    def is_near_52w_high(self) -> bool:
        """52주 신고가 근접 여부 (신고가의 90% 이상)"""
        threshold = self.high_52w * self.NEAR_THRESHOLD
        return self.close_price >= threshold and not self.is_52w_high
    
    @property
    def is_near_all_time_high(self) -> bool:
        """역사적 신고가 근접 여부 (신고가의 90% 이상)"""
        threshold = self.all_time_high * self.NEAR_THRESHOLD
        return self.close_price >= threshold and not self.is_all_time_high


class PriceDataPort(ABC):
    """가격 데이터 조회 포트 인터페이스"""
    
    @abstractmethod
    def get_price_info(self, ticker: str, date_str: str) -> Optional[StockPriceInfo]:
        """종목의 가격 정보를 조회합니다.
        
        Args:
            ticker (str): 종목코드 (6자리, 예: '005930')
            date_str (str): 조회 날짜 (YYYYMMDD 형식, 예: '20250102')
            
        Returns:
            Optional[StockPriceInfo]: 가격 정보, 조회 실패 시 None
        """
        pass
