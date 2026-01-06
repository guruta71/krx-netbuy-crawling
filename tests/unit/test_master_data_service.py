import pytest
import pandas as pd
from core.services.master_data_service import MasterDataService

def test_transform_to_excel_schema_excludes_ticker():
    """데이터 변환 시 종목코드(ticker)가 제외되는지 검증"""
    # Given
    service = MasterDataService()
    date_int = 20250101
    
    daily_df = pd.DataFrame({
        '종목명': ['삼성전자', 'SK하이닉스'],
        '종목코드': ['005930', '000660'],
        '순매수_거래대금': [1000, 2000]
    })
    
    # When
    result_df = service.transform_to_excel_schema(daily_df, date_int)
    
    # Then
    assert '종목코드' not in result_df.columns
    assert '종목' in result_df.columns
    assert '금액' in result_df.columns
    assert '일자' in result_df.columns
    
    # 데이터 검증
    assert len(result_df) == 2
    assert result_df.iloc[0]['종목'] == '삼성전자'
    assert result_df.iloc[0]['금액'] == 1000

def test_excel_columns_definition():
    """엑셀 컬럼 정의에 종목코드가 없는지 검증"""
    service = MasterDataService()
    assert '종목코드' not in service.excel_columns
    assert service.excel_columns == ['일자', '종목', '금액']
