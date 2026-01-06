
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import os
from pathlib import Path

# Adjust path to find src
import sys
sys.path.append(os.path.abspath("src"))

from core.services.krx_fetch_service import KrxFetchService
from core.domain.models import Market, Investor

class TestRawOverwrite:
    def test_overwrite_existing_raw_file(self):
        """
        Scenario: use_raw=True, Raw file exists.
        Expected: Loads raw file, processes it, and SAVES (overwrites) it. KRX Web fetch is NOT called.
        """
        # 1. Setup
        mock_krx_port = MagicMock()
        mock_storage_port = MagicMock()
        
        # Raw file exists
        mock_storage_port.path_exists.return_value = True
        
        # Mock Raw Data (Valid Excel bytes)
        # Create a dummy excel in memory
        dummy_df = pd.DataFrame({
            '종목코드': ['005930', '000660'],
            '종목명': ['삼성전자', 'SK하이닉스'],
            '순매수_거래대금': [1000, 500]
        })
        
        # Use a real bytes implementation for storage.get_file return
        with io.BytesIO() as b:
            # KrxHttpAdapter often saves as CP949 CSV or Excel. Let's start with Excel.
            dummy_df.to_excel(b, index=False)
            mock_storage_port.get_file.return_value = b.getvalue()
        
        service = KrxFetchService(
            krx_port=mock_krx_port,
            storage_port=mock_storage_port,
            use_raw=True
        )

        # 2. Execution
        # We need to mock _parse_and_filter_data OR ensure _parse_bytes_to_df works with our dummy bytes
        # Since we provided valid excel bytes, _parse_bytes_to_df should work.
        # But _find_net_value_column logic needs '순매수' keyword. '순매수_거래대금' has it.
        
        result = service.fetch_all_data("20260106")
        
        # 3. Verification
        
        # A. Should NOT call KRX fetch
        mock_krx_port.fetch_net_value_data.assert_not_called()
        
        # B. Should call storage.get_file 4 times (for 4 targets)
        # However, our dummy mock returns the same bytes for all calls.
        assert mock_storage_port.get_file.call_count == 4
        
        # C. Should call storage.save_dataframe_excel 4 times (Overwrite)
        assert mock_storage_port.save_dataframe_excel.call_count == 4
        
        # Check arguments for one of the calls
        args, _ = mock_storage_port.save_dataframe_excel.call_args_list[0]
        df_saved, path_saved = args
        
        print(f"Saved Path: {path_saved}")
        assert "raw/" in path_saved
        assert "순매수.xlsx" in path_saved
        assert not df_saved.empty
        assert "삼성전자" in df_saved["종목명"].values

    def test_web_fetch_when_raw_missing(self):
        """
        Scenario: use_raw=True, Raw file does NOT exist.
        Expected: Calls KRX Web fetch, processes, and SAVES (creates) it.
        """
        # 1. Setup
        mock_krx_port = MagicMock()
        mock_storage_port = MagicMock()
        
        # Raw file missing
        mock_storage_port.path_exists.return_value = False
        
        # KRX fetch returns valid data
        dummy_df = pd.DataFrame({
            '종목코드': ['005930'],
            '종목명': ['삼성전자'],
            '순매수_거래대금': [1000]
        })
        with io.BytesIO() as b:
            dummy_df.to_excel(b, index=False)
            mock_krx_port.fetch_net_value_data.return_value = b.getvalue()
            
        service = KrxFetchService(
            krx_port=mock_krx_port,
            storage_port=mock_storage_port,
            use_raw=True
        )
        
        # 2. Execution
        service.fetch_all_data("20260106")
        
        # 3. Verification
        # Should call KRX fetch
        assert mock_krx_port.fetch_net_value_data.call_count == 4
        
        # Should SAVE (Overwrite/Create)
        assert mock_storage_port.save_dataframe_excel.call_count == 4

import io
if __name__ == "__main__":
    # Manually run the test functions
    t = TestRawOverwrite()
    print("Running test_overwrite_existing_raw_file...")
    t.test_overwrite_existing_raw_file()
    print("PASSED")
    
    print("Running test_web_fetch_when_raw_missing...")
    t.test_web_fetch_when_raw_missing()
    print("PASSED")
