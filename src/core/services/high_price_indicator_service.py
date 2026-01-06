"""
신고가 지표 판정 서비스

종목의 가격 정보를 분석하여 신고가 지표를 판정하고,
우선순위에 따른 표시 텍스트 및 색상을 결정합니다.
"""
from typing import Dict, Tuple, Optional
from core.ports.price_data_port import PriceDataPort, StockPriceInfo


class HighPriceIndicatorService:
    """신고가 지표 판정 서비스.
    
    종목별 가격 정보를 조회하고 신고가 지표를 판정합니다.
    우선순위: 역사적 신고가(역신) > 역사적 근접(역근) > 52주 신고가(52신) > 52주 근접(52근)
    """
    
    # 색상 코드 (ExcelFormatter의 COLORS 키와 매칭)
    COLOR_ALL_TIME_HIGH = 'all_time_high'
    COLOR_NEAR_ALL_TIME_HIGH = 'near_all_time_high'
    COLOR_WEEK_52_HIGH = 'week_52_high'
    COLOR_NEAR_52W_HIGH = 'near_52w_high'
    
    def __init__(self, price_port: PriceDataPort):
        """HighPriceIndicatorService 초기화.
        
        Args:
            price_port (PriceDataPort): 가격 데이터 조회 포트.
        """
        self.price_port = price_port
    
    def analyze_high_price_indicators(
        self,
        ticker_map: Dict[str, str],
        date_str: str
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """종목별 신고가 지표를 분석합니다.
        
        Args:
            ticker_map (Dict[str, str]): 종목명 -> 종목코드 매핑.
            date_str (str): 조회 날짜 (YYYYMMDD 형식).
            
        Returns:
            Dict[str, Dict[str, Optional[str]]]: 종목명을 키로, 
                {'text': 표시 텍스트, 'color': 색상 코드}를 밸류로 하는 딕셔너리.
        """
        result = {}
        
        print(f"[Service:HighPriceIndicator] 신고가 지표 분석 시작 ({len(ticker_map)}개 종목, {date_str})")
        
        for stock_name, ticker in ticker_map.items():
            try:
                price_info = self.price_port.get_price_info(ticker, date_str)
                
                if price_info:
                    text, color = self._get_indicator_display(price_info)
                    result[stock_name] = {'text': text, 'color': color}
                else:
                    result[stock_name] = {'text': None, 'color': None}
                    
            except Exception as e:
                print(f"  [Service:HighPriceIndicator] ⚠️ {stock_name}({ticker}) 분석 실패: {e}")
                result[stock_name] = {'text': None, 'color': None}
        
        indicators_count = sum(1 for v in result.values() if v['text'] is not None)
        print(f"[Service:HighPriceIndicator] 분석 완료 ({indicators_count}개 지표 발견)")
        
        return result
    
    def _get_indicator_display(
        self,
        price_info: StockPriceInfo
    ) -> Tuple[Optional[str], Optional[str]]:
        """신고가 지표의 표시 텍스트와 색상을 결정합니다.
        
        우선순위: 역신 > 역근 > 52신 > 52근
        
        Args:
            price_info (StockPriceInfo): 가격 정보.
            
        Returns:
            Tuple[Optional[str], Optional[str]]: (표시 텍스트, 색상 코드).
        """
        # 우선순위 1: 역사적 신고가
        if price_info.is_all_time_high:
            return ("역·신", self.COLOR_ALL_TIME_HIGH)
        
        # 우선순위 2: 역사적 근접
        if price_info.is_near_all_time_high:
            return ("역·근", self.COLOR_NEAR_ALL_TIME_HIGH)
        
        # 우선순위 3: 52주 신고가
        if price_info.is_52w_high:
            return ("52·신", self.COLOR_WEEK_52_HIGH)
        
        # 우선순위 4: 52주 근접
        if price_info.is_near_52w_high:
            return ("52·근", self.COLOR_NEAR_52W_HIGH)
        
        # 해당 없음
        return (None, None)
