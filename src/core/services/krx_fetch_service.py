from typing import List, Optional
import datetime
import pandas as pd
import io
import warnings

from core.domain.models import KrxData, Market, Investor
from core.ports.krx_data_port import KrxDataPort

class KrxFetchService:
    """KRX 데이터 수집 및 표준화를 담당하는 헬퍼 서비스.

    Attributes:
        krx_port (KrxDataPort): KRX 데이터 포트 인터페이스.
    """

    def __init__(self, krx_port: KrxDataPort):
        """KrxFetchService 초기화.

        Args:
            krx_port (KrxDataPort): KRX 데이터 포트 인터페이스.
        """
        self.krx_port = krx_port

    def fetch_all_data(self, date_str: Optional[str] = None) -> List[KrxData]:
        """모든 타겟(시장/투자자)에 대해 데이터를 수집하고 가공합니다.

        Args:
            date_str (Optional[str]): 수집할 날짜 (YYYYMMDD). None이면 오늘 날짜를 사용합니다.

        Returns:
            List[KrxData]: 수집된 KrxData 객체 리스트.
        """
        if date_str is None:
            date_str = datetime.date.today().strftime('%Y%m%d')

        results: List[KrxData] = []
        targets = [
            (Market.KOSPI, Investor.FOREIGNER),
            (Market.KOSPI, Investor.INSTITUTIONS),
            (Market.KOSDAQ, Investor.FOREIGNER),
            (Market.KOSDAQ, Investor.INSTITUTIONS),
        ]

        print(f"[Service:KrxFetch] {date_str} 데이터 수집 시작...")

        def fetch_one(market: Market, investor: Investor) -> Optional[KrxData]:
            try:
                # 1. 원본 데이터 수집
                raw_bytes = self.krx_port.fetch_net_value_data(market, investor, date_str)
                
                # 2. 데이터 가공
                df = self._parse_and_filter_data(raw_bytes)
                
                if df.empty:
                    print(f"  -> ⚠️ {market.value} {investor.value} 데이터가 비어있습니다 (휴장일 등).")
                    return None

                # 3. KrxData 객체 생성
                krx_data = KrxData(
                    market=market,
                    investor=investor,
                    date_str=date_str,
                    data=df
                )
                print(f"  -> ✅ {market.value} {investor.value} 수집 및 가공 완료 ({len(df)}행)")
                return krx_data

            except Exception as e:
                print(f"  -> 🚨 {market.value} {investor.value} 처리 중 오류 발생: {e}")
                return None

        # 순차적으로 실행
        for market, investor in targets:
            result = fetch_one(market, investor)
            if result is not None:
                results.append(result)

        return results

    def _parse_and_filter_data(self, excel_bytes: bytes) -> pd.DataFrame:
        """KRX 원본 데이터를 파싱하고 순매수 상위 20개를 추출합니다.

        Args:
            excel_bytes (bytes): KRX에서 다운로드한 원본 바이트 데이터.

        Returns:
            pd.DataFrame: 가공된 DataFrame (종목코드, 종목명, 순매수_거래대금).
        """
        if not excel_bytes:
            return pd.DataFrame()

        # 1. 파싱
        df = self._parse_bytes_to_df(excel_bytes)
        if df.empty:
            return pd.DataFrame()

        # 2. 순매수 컬럼 식별
        sort_col = self._find_net_value_column(df)
        if sort_col is None:
            return pd.DataFrame()

        # 3. 필수 컬럼 확인
        required_cols = ['종목코드', '종목명', sort_col]
        if not all(col in df.columns for col in required_cols):
            print(f"  [Service:KrxFetch] 🚨 필수 컬럼({required_cols})이 DF에 없습니다.")
            return pd.DataFrame()

        # 4. 정렬 및 상위 30개 추출
        df_sorted = df.sort_values(by=sort_col, ascending=False)
        df_top30 = df_sorted.head(30).copy() 
        
        # 5. 최종 컬럼 선택 및 이름 변경
        return df_top30[required_cols].rename(columns={sort_col: '순매수_거래대금'})

    def _parse_bytes_to_df(self, excel_bytes: bytes) -> pd.DataFrame:
        """바이트 데이터를 DataFrame으로 파싱합니다.
        
        Args:
            excel_bytes (bytes): 엑셀 바이트 데이터.
            
        Returns:
            pd.DataFrame: 파싱된 DataFrame.
        """
        try:
            # 엑셀 파일 시그니처(PK) 확인
            if excel_bytes.startswith(b'PK'):
                return pd.read_excel(io.BytesIO(excel_bytes))
            else:
                # CSV 파싱 (KRX는 CP949 인코딩 사용, 에러 무시)
                return pd.read_csv(io.BytesIO(excel_bytes), encoding='cp949', encoding_errors='replace')
        except Exception as e:
            print(f"  [Service:KrxFetch] 🚨 데이터 파싱 중 오류: {e}")
            return pd.DataFrame()

    def _find_net_value_column(self, df: pd.DataFrame) -> Optional[str]:
        """순매수 거래대금 컬럼을 찾습니다.
        
        Args:
            df (pd.DataFrame): 대상 DataFrame.
            
        Returns:
            Optional[str]: 컬럼명, 없으면 None.
        """
        net_value_keywords = ['순매수', '거래대금']
        
        for col in df.columns:
            if all(keyword in str(col).lower() for keyword in net_value_keywords):
                return col
        
        # 키워드로 못 찾은 경우 마지막 숫자 컬럼 사용
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            sort_col = numeric_cols[-1]
            print(f"  [Service:KrxFetch] ⚠️ 순매수 컬럼을 찾을 수 없어 '{sort_col}' 기준으로 정렬.")
            return sort_col
            
        print("  [Service:KrxFetch] 🚨 유효한 숫자 컬럼이 없어 가공 실패.")
        return None
