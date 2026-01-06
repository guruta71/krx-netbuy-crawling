"""
로컬 파일 시스템 저장소 구현

StoragePort를 구현하여 로컬 파일 시스템에 데이터를 저장합니다.
"""
import os
from pathlib import Path
from typing import Optional
import pandas as pd
import openpyxl

from core.ports.storage_port import StoragePort


class LocalStorageAdapter(StoragePort):
    """로컬 파일 시스템 저장소 Adapter.

    StoragePort를 구현하여 로컬 파일 시스템에 데이터를 저장합니다.

    Attributes:
        base_path (Path): 기본 저장 경로.
    """
    
    def __init__(self, base_path: str = "output"):
        """LocalStorageAdapter 초기화.

        Args:
            base_path (str): 기본 저장 경로 (기본값: "output").
        """
        self.base_path = Path(base_path)
        self.ensure_directory("")  # 기본 경로 생성
        print(f"[LocalStorage] 초기화 완료 (Base: {self.base_path.absolute()})")
    
    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 Excel 파일로 저장합니다.

        Args:
            df (pd.DataFrame): 저장할 DataFrame.
            path (str): 저장 경로 (base_path 상대 경로).
            **kwargs: to_excel 옵션.

        Returns:
            bool: 성공 여부.
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            df.to_excel(full_path, **kwargs)
            print(f"[LocalStorage] [OK] Excel 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] [Error] Excel 저장 실패 ({path}): {e}")
            return False
    
    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 CSV 파일로 저장합니다.

        Args:
            df (pd.DataFrame): 저장할 DataFrame.
            path (str): 저장 경로 (base_path 상대 경로).
            **kwargs: to_csv 옵션.

        Returns:
            bool: 성공 여부.
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            df.to_csv(full_path, **kwargs)
            print(f"[LocalStorage] [OK] CSV 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] [Error] CSV 저장 실패 ({path}): {e}")
            return False
    
    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        """openpyxl Workbook을 저장합니다.

        Args:
            book (openpyxl.Workbook): 저장할 Workbook 객체.
            path (str): 저장 경로 (base_path 상대 경로).

        Returns:
            bool: 성공 여부.
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            book.save(full_path)
            print(f"[LocalStorage] [OK] Workbook 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] [Error] Workbook 저장 실패 ({path}): {e}")
            return False
    
    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        """Excel Workbook을 로드합니다.

        Args:
            path (str): 로드할 파일 경로 (base_path 상대 경로).

        Returns:
            Optional[openpyxl.Workbook]: 로드된 Workbook 객체, 실패 시 None.
        """
        try:
            full_path = self.base_path / path
            return openpyxl.load_workbook(full_path)
        except FileNotFoundError:
            print(f"[LocalStorage] [Warn] 파일 없음: {path}")
            return None
        except Exception as e:
            print(f"[LocalStorage] [Error] Workbook 로드 실패 ({path}): {e}")
            return None
    
    def path_exists(self, path: str) -> bool:
        """경로가 존재하는지 확인합니다.

        Args:
            path (str): 확인할 경로 (base_path 상대 경로).

        Returns:
            bool: 존재 여부.
        """
        full_path = self.base_path / path
        return full_path.exists()
    
    def ensure_directory(self, path: str) -> bool:
        """디렉토리가 없으면 생성합니다.

        Args:
            path (str): 생성할 디렉토리 경로 (base_path 상대 경로).

        Returns:
            bool: 성공 여부.
        """
        try:
            if path == "":
                # 기본 경로 생성
                self.base_path.mkdir(parents=True, exist_ok=True)
            else:
                full_path = self.base_path / path
                full_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            print(f"[LocalStorage] [Error] 디렉토리 생성 실패 ({path}): {e}")
            return False

    def load_dataframe(self, path: str, sheet_name: str = None, **kwargs) -> pd.DataFrame:
        """Excel 파일에서 DataFrame을 로드합니다.
        
        Args:
            path (str): 파일 경로.
            sheet_name (str, optional): 시트 이름.
            **kwargs: 추가 옵션.
            
        Returns:
            pd.DataFrame: 로드된 DataFrame.
        """
        try:
            full_path = self.base_path / path
            if not full_path.exists():
                return pd.DataFrame()
            
            # sheet_name이 None이면 모든 시트를 dict로 반환하므로, 0(첫 번째 시트)으로 설정
            target_sheet = 0 if sheet_name is None else sheet_name
            return pd.read_excel(full_path, sheet_name=target_sheet, **kwargs)
        except Exception as e:
            print(f"[LocalStorage] [Error] DataFrame 로드 실패 ({path}): {e}")
            return pd.DataFrame()

    def get_file(self, path: str) -> Optional[bytes]:
        """파일의 내용을 바이트로 읽어옵니다.
        
        Args:
            path (str): 파일 경로.
            
        Returns:
            Optional[bytes]: 파일 내용, 실패 시 None.
        """
        try:
            full_path = self.base_path / path
            if not full_path.exists():
                return None
            
            with open(full_path, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f"[LocalStorage] [Error] 파일 읽기 실패 ({path}): {e}")
            return None

    def put_file(self, path: str, data: bytes) -> bool:
        """바이트 데이터를 파일로 저장합니다.
        
        Args:
            path (str): 파일 경로.
            data (bytes): 저장할 데이터.
            
        Returns:
            bool: 저장 성공 여부.
        """
        try:
            full_path = self.base_path / path
            self.ensure_directory(str(full_path.parent.relative_to(self.base_path)))
            
            with open(full_path, 'wb') as f:
                f.write(data)
            print(f"[LocalStorage] [OK] 파일 저장: {path}")
            return True
        except Exception as e:
            print(f"[LocalStorage] [Error] 파일 저장 실패 ({path}): {e}")
            return False
