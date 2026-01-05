"""삼성전자 가격 정보 조회 테스트"""

import sys
sys.path.insert(0, 'src')

from infra.adapters.pykrx_price_adapter import PykrxPriceAdapter

def main():
    print("=" * 60)
    print("삼성전자 (005930) 가격 정보 조회 - 2025년 1월 2일")
    print("=" * 60)
    
    adapter = PykrxPriceAdapter()
    info = adapter.get_price_info("005930", "20250102")
    
    if info:
        print(f"\n📊 종목코드: {info.ticker}")
        print(f"💰 종가: {info.close_price:,.0f}원")
        print(f"📈 52주 신고가: {info.high_52w:,.0f}원")
        print(f"🏆 역사적 신고가: {info.all_time_high:,.0f}원")
        print()
        print(f"{'🔥 52주 신고가 달성!' if info.is_52w_high else '   52주 신고가 미달성'}")
        print(f"{'⭐ 역사적 신고가 달성!' if info.is_all_time_high else '   역사적 신고가 미달성'}")
        print()
        
        # 추가 분석
        if info.close_price < info.high_52w:
            gap_52w = info.high_52w - info.close_price
            ratio_52w = (gap_52w / info.high_52w) * 100
            print(f"📉 52주 신고가 대비 {gap_52w:,.0f}원 하락 ({ratio_52w:.2f}%)")
        
        if info.close_price < info.all_time_high:
            gap_all = info.all_time_high - info.close_price
            ratio_all = (gap_all / info.all_time_high) * 100
            print(f"📉 역사적 신고가 대비 {gap_all:,.0f}원 하락 ({ratio_all:.2f}%)")
    else:
        print("❌ 가격 정보 조회 실패")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
