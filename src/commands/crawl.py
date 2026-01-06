import typer
import datetime
from dotenv import load_dotenv
import os

# Services
from core.services.daily_routine_service import DailyRoutineService
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.master_data_service import MasterDataService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.services.ranking_data_service import RankingDataService

# Adapters
from infra.adapters.storage import LocalStorageAdapter
from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter
from infra.adapters.krx_http_adapter import KrxHttpAdapter
from infra.adapters.daily_excel_adapter import DailyExcelAdapter
from infra.adapters.watchlist_file_adapter import WatchlistFileAdapter
from infra.adapters.ranking_excel_adapter import RankingExcelAdapter
from infra.adapters.excel.master_workbook_adapter import MasterWorkbookAdapter
from infra.adapters.excel.master_sheet_adapter import MasterSheetAdapter
from infra.adapters.excel.master_pivot_sheet_adapter import MasterPivotSheetAdapter
from infra.adapters.pykrx_price_adapter import PykrxPriceAdapter

def crawl(
    date: str = typer.Argument(None, help="대상 날짜 (YYYYMMDD 형식, 기본값: 오늘)"),
    drive: bool = typer.Option(False, "--drive", "-d", help="Google Drive에도 저장할지 여부")
):
    """일일 크롤링 루틴을 실행합니다.

    KRX 데이터를 수집하고, 일별 리포트, 마스터 리포트, 순위 리포트 등을 생성하여 저장합니다.
    기본적으로 로컬에 저장하며, `--drive` 옵션 사용 시 Google Drive에도 저장합니다.

    Args:
        date (str): 대상 날짜 (YYYYMMDD). 기본값은 오늘 날짜.
        drive (bool): Google Drive 저장 여부.
    """
    # 1. 환경 변수 로드
    load_dotenv()
    
    # 2. 날짜 처리
    if date:
        target_date = date
        # 간단한 날짜 형식 검증
        if len(target_date) != 8 or not target_date.isdigit():
            typer.echo(f"🚨 [CLI] 잘못된 날짜 형식입니다: {target_date}. YYYYMMDD 형식을 사용해주세요.", err=True)
            raise typer.Exit(code=1)
    else:
        target_date = datetime.date.today().strftime('%Y%m%d')

    # 3. 기본 경로 및 설정
    BASE_OUTPUT_PATH = "output"
    TOKEN_FILE = "secrets/token.json"
    CLIENT_SECRET_FILE = "secrets/client_secret.json"
    
    # 4. StoragePort 인스턴스 생성
    # 모드에 따라 배타적으로 동작 (Local Only OR Drive Only) -> Hybrid Mode로 변경
    # --drive 옵션 시:
    #   Source: Google Drive (싱크를 맞추기 위해)
    #   Target: [Local, Google Drive] (양쪽 다 저장)
    
    # 항상 로컬 저장소는 초기화
    local_storage = LocalStorageAdapter(base_path=BASE_OUTPUT_PATH)
    
    save_storages = [local_storage]
    source_storage = local_storage

    if drive:
        # Google Drive Mode
        root_folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")
        try:
            if os.path.exists(TOKEN_FILE):
                print(f"[CLI] Google Drive 인증 (OAuth Token) 사용 ({TOKEN_FILE})")
                drive_storage = GoogleDriveAdapter(
                    token_file=TOKEN_FILE,
                    root_folder_id=root_folder_id,
                    client_secret_file=CLIENT_SECRET_FILE if os.path.exists(CLIENT_SECRET_FILE) else None
                )
                
                typer.echo(f"--- [CLI] Storage Mode: Hybrid (Source: Drive, Target: Local+Drive) ---")
                
                # Source를 Drive로 변경하여 최신 데이터를 가져옴
                source_storage = drive_storage
                
                # 저장 대상에 Drive 추가
                save_storages.append(drive_storage)
                
            else:
                typer.echo(f"🚨 [CLI] Google Drive 토큰 파일 없음 ({TOKEN_FILE})", err=True)
                typer.echo("`netbuy auth` 명령어를 실행하여 인증을 먼저 진행해주세요.", err=True)
                raise typer.Exit(code=1)
            
        except Exception as e:
            typer.echo(f"🚨 [CLI] Google Drive 초기화 실패: {e}", err=True)
            raise typer.Exit(code=1)
            
    else:
        # Local Mode (Default)
        typer.echo(f"--- [CLI] Storage Mode: Local Only ---")

    # 5. 어댑터(Adapters) 인스턴스 생성 및 의존성 주입
    # (Infra Layer)
    krx_adapter = KrxHttpAdapter()
    price_adapter = PykrxPriceAdapter()
    daily_adapter = DailyExcelAdapter(storages=save_storages, source_storage=source_storage)
    watchlist_adapter = WatchlistFileAdapter(storages=save_storages)
    
    # Master 관련 어댑터들
    master_sheet_adapter = MasterSheetAdapter()
    master_pivot_sheet_adapter = MasterPivotSheetAdapter()
    master_workbook_adapter = MasterWorkbookAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        sheet_adapter=master_sheet_adapter,
        pivot_sheet_adapter=master_pivot_sheet_adapter
    )

    # 6. 서비스(Services) 인스턴스 생성 및 의존성 주입
    # (Core Layer)
    fetch_service = KrxFetchService(krx_port=krx_adapter)
    master_data_service = MasterDataService()
    master_service = MasterReportService(
        source_storage=source_storage, 
        target_storages=save_storages,
        data_service=master_data_service,
        workbook_adapter=master_workbook_adapter
    )
    
    # Ranking 서비스 조립 (헥사고날 아키텍처)
    ranking_data_service = RankingDataService(top_n=30)
    ranking_report_adapter = RankingExcelAdapter(
        source_storage=source_storage, 
        target_storages=save_storages,
        price_port=price_adapter
    )
    ranking_service = RankingAnalysisService(
        data_service=ranking_data_service,
        report_port=ranking_report_adapter
    )
    
    routine_service = DailyRoutineService(
        fetch_service=fetch_service,
        daily_port=daily_adapter,
        master_port=master_service,
        ranking_port=ranking_service,
        watchlist_port=watchlist_adapter
    )

    # 7. 메인 루틴 실행
    try:
        routine_service.execute(date_str=target_date)
    except Exception as e:
        typer.echo(f"\n🚨 [CLI] Critical Error during execution: {e}", err=True)
        raise typer.Exit(code=1)
