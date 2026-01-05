"""pykrx 가격 어댑터 단위 테스트"""

import pytest
from infra.adapters.pykrx_price_adapter import PykrxPriceAdapter


@pytest.fixture
def adapter():
    """PykrxPriceAdapter 인스턴스 픽스처"""
    return PykrxPriceAdapter()


def test_get_price_info_samsung_jan2(adapter):
    """삼성전자 2025년 1월 2일 가격 정보 조회 테스트"""
    # Given
    ticker = "005930"  # 삼성전자
    date_str = "20250102"  # 2025년 1월 2일
    
    # When
    result = adapter.get_price_info(ticker, date_str)
    
    # 실제 값 출력
    print("\n" + "=" * 60)
    print("📊 삼성전자 가격 정보 (2025-01-02)")
    print("=" * 60)
    if result:
        print(f"💰 종가: {result.close_price:,.0f}원")
        print(f"📈 52주 신고가: {result.high_52w:,.0f}원")
        print(f"🏆 역사적 신고가 (최근 10년): {result.all_time_high:,.0f}원")
        print(f"🔥 52주 신고가 달성: {result.is_52w_high}")
        print(f"⭐ 역사적 신고가 달성: {result.is_all_time_high}")
    print("=" * 60 + "\n")
    
    # Then
    assert result is not None, "삼성전자 가격 정보를 조회할 수 없습니다"
    assert result.ticker == ticker
    assert result.close_price > 0, "종가는 0보다 커야 합니다"
    assert result.high_52w > 0, "52주 신고가는 0보다 커야 합니다"
    assert result.all_time_high > 0, "역사적 신고가는 0보다 커야 합니다"
    assert result.all_time_high >= result.high_52w, "역사적 신고가는 52주 신고가보다 크거나 같아야 합니다"
    
    # 신고가 여부 프로퍼티 테스트
    assert isinstance(result.is_52w_high, bool)
    assert isinstance(result.is_all_time_high, bool)


def test_get_price_info_naver_jan2(adapter):
    """네이버 2025년 1월 2일 가격 정보 조회 테스트"""
    # Given
    ticker = "035420"  # 네이버
    date_str = "20250102"
    
    # When
    result = adapter.get_price_info(ticker, date_str)
    
    # Then
    assert result is not None
    assert result.ticker == ticker
    assert result.close_price > 0
    assert result.high_52w > 0
    assert result.all_time_high > 0


def test_get_price_info_invalid_ticker(adapter):
    """잘못된 티커 조회 시 None 반환 테스트"""
    # Given
    ticker = "000000"  # 존재하지 않는 티커
    date_str = "20250102"
    
    # When
    result = adapter.get_price_info(ticker, date_str)
    
    # Then
    assert result is None, "존재하지 않는 티커는 None을 반환해야 합니다"


def test_get_price_info_weekend(adapter):
    """주말 날짜 조회 시 처리 테스트"""
    # Given
    ticker = "005930"
    date_str = "20250104"  # 2025년 1월 4일 (토요일)
    
    # When
    result = adapter.get_price_info(ticker, date_str)
    
    # Then
    # 주말에는 데이터가 없으므로 None이 반환되어야 함
    assert result is None, "주말에는 데이터가 없으므로 None을 반환해야 합니다"


def test_stock_price_info_properties():
    """StockPriceInfo 프로퍼티 테스트"""
    from core.ports.price_data_port import StockPriceInfo
    
    # Given - 52주 신고가 도달
    info = StockPriceInfo(
        ticker="005930",
        close_price=60000,
        high_52w=60000,
        all_time_high=70000
    )
    
    # Then
    assert info.is_52w_high is True
    assert info.is_all_time_high is False
    assert info.is_near_52w_high is False  # 신고가 도달이므로 근접은 False
    assert info.is_near_all_time_high is False  # 70000의 90% = 63000, 60000 < 63000이므로 False
    
    # Given - 역사적 신고가 도달
    info2 = StockPriceInfo(
        ticker="005930",
        close_price=70000,
        high_52w=60000,
        all_time_high=70000
    )
    
    # Then
    assert info2.is_52w_high is True
    assert info2.is_all_time_high is True
    assert info2.is_near_52w_high is False  # 신고가 도달이므로 근접은 False
    assert info2.is_near_all_time_high is False  # 신고가 도달이므로 근접은 False
    
    # Given - 52주 신고가 근접 (90% 이상)
    info3 = StockPriceInfo(
        ticker="005930",
        close_price=55000,  # 60000의 91.7%
        high_52w=60000,
        all_time_high=70000
    )
    
    # Then
    assert info3.is_52w_high is False
    assert info3.is_near_52w_high is True  # 54000(90%) 이상이므로 True
    assert info3.is_all_time_high is False
    assert info3.is_near_all_time_high is False  # 63000(90%) 미만이므로 False
    
    # Given - 역사적 신고가 근접 (90% 이상)
    info4 = StockPriceInfo(
        ticker="005930",
        close_price=64000,  # 70000의 91.4%
        high_52w=60000,
        all_time_high=70000
    )
    
    # Then
    assert info4.is_52w_high is True  # 60000 이상
    assert info4.is_near_52w_high is False  # 신고가 도달이므로 근접은 False
    assert info4.is_all_time_high is False
    assert info4.is_near_all_time_high is True  # 63000(90%) 이상이므로 True
