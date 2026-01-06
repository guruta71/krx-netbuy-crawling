"""신고가 지표 서비스 테스트"""

import pytest
from unittest.mock import Mock
from core.services.high_price_indicator_service import HighPriceIndicatorService
from core.ports.price_data_port import StockPriceInfo


class TestHighPriceIndicatorService:
    """HighPriceIndicatorService 테스트"""
    
    @pytest.fixture
    def price_port(self):
        """Mock PriceDataPort"""
        return Mock()
    
    @pytest.fixture
    def service(self, price_port):
        """HighPriceIndicatorService 인스턴스"""
        return HighPriceIndicatorService(price_port)
    
    def test_all_time_high_priority(self, service, price_port):
        """역사적 신고가가 최우선 순위인지 테스트"""
        # Given: 역사적 신고가 달성한 종목
        price_info = StockPriceInfo(
            ticker="005930",
            close_price=90000,
            high_52w=85000,
            all_time_high=90000
        )
        price_port.get_price_info.return_value = price_info
        
        # When
        ticker_map = {"삼성전자": "005930"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["삼성전자"]["text"] == "⭐역사적 신고가"
        assert result["삼성전자"]["color"] == "all_time_high"
    
    def test_near_all_time_high_priority(self, service, price_port):
        """역사적 근접이 52주 신고가보다 우선인지 테스트"""
        # Given: 역사적 근접 + 52주 신고가 달성
        price_info = StockPriceInfo(
            ticker="000660",
            close_price=95000,
            high_52w=95000,
            all_time_high=100000  # 95% 이상이므로 근접
        )
        price_port.get_price_info.return_value = price_info
        
        # When
        ticker_map = {"SK하이닉스": "000660"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then: 역사적 근접이 우선
        assert result["SK하이닉스"]["text"] == "📈역사적 근접"
        assert result["SK하이닉스"]["color"] == "near_all_time_high"
    
    def test_week_52_high_priority(self, service, price_port):
        """52주 신고가가 52주 근접보다 우선인지 테스트"""
        # Given: 52주 신고가만 달성
        price_info = StockPriceInfo(
            ticker="005380",
            close_price=50000,
            high_52w=50000,
            all_time_high=100000  # 50% 미만
        )
        price_port.get_price_info.return_value = price_info
        
        # When
        ticker_map = {"현대차": "005380"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["현대차"]["text"] == "🔥52주"
        assert result["현대차"]["color"] == "week_52_high"
    
    def test_near_52w_high_priority(self, service, price_port):
        """52주 근접이 가장 낮은 우선순위인지 테스트"""
        # Given: 52주 근접만 달성
        price_info = StockPriceInfo(
            ticker="035720",
            close_price=46000,
            high_52w=50000,  # 92% - 근접
            all_time_high=100000
        )
        price_port.get_price_info.return_value = price_info
        
        # When
        ticker_map = {"카카오": "035720"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["카카오"]["text"] == "📊52주 근접"
        assert result["카카오"]["color"] == "near_52w_high"
    
    def test_no_indicator(self, service, price_port):
        """신고가 지표가 없는 경우 테스트"""
        # Given: 모든 조건 미달성
        price_info = StockPriceInfo(
            ticker="005930",
            close_price=50000,
            high_52w=80000,
            all_time_high=100000
        )
        price_port.get_price_info.return_value = price_info
        
        # When
        ticker_map = {"삼성전자": "005930"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["삼성전자"]["text"] is None
        assert result["삼성전자"]["color"] is None
    
    def test_price_info_not_found(self, service, price_port):
        """가격 정보 조회 실패 시 테스트"""
        # Given
        price_port.get_price_info.return_value = None
        
        # When
        ticker_map = {"테스트종목": "999999"}
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["테스트종목"]["text"] is None
        assert result["테스트종목"]["color"] is None
    
    def test_multiple_stocks(self, service, price_port):
        """여러 종목 동시 처리 테스트"""
        # Given
        def get_price_info_side_effect(ticker, date_str):
            if ticker == "005930":
                return StockPriceInfo("005930", 90000, 85000, 90000)
            elif ticker == "000660":
                return StockPriceInfo("000660", 50000, 50000, 100000)
            return None
        
        price_port.get_price_info.side_effect = get_price_info_side_effect
        
        # When
        ticker_map = {
            "삼성전자": "005930",
            "SK하이닉스": "000660"
        }
        result = service.analyze_high_price_indicators(ticker_map, "20250105")
        
        # Then
        assert result["삼성전자"]["text"] == "⭐역사적 신고가"
        assert result["SK하이닉스"]["text"] == "🔥52주"
