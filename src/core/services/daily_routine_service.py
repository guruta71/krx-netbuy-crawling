from typing import Optional
from core.services.krx_fetch_service import KrxFetchService
from core.services.master_report_service import MasterReportService
from core.services.ranking_analysis_service import RankingAnalysisService
from core.ports.daily_report_port import DailyReportPort
from core.ports.watchlist_port import WatchlistPort

class DailyRoutineService:
    """일일 크롤링 및 리포트 업데이트 루틴을 총괄하는 오케스트레이션 서비스.

    Attributes:
        fetch_service (KrxFetchService): KRX 데이터 수집 서비스.
        daily_port (DailyReportPort): 일별 리포트 저장 포트.
        master_port (MasterReportService): 마스터 리포트 서비스.
        ranking_port (RankingAnalysisService): 순위 분석 서비스.
        watchlist_port (WatchlistPort): 관심종목 저장 포트.
    """

    def __init__(
        self,
        fetch_service: KrxFetchService,
        daily_port: DailyReportPort,
        master_port: MasterReportService,
        ranking_port: RankingAnalysisService,
        watchlist_port: WatchlistPort
    ):
        """DailyRoutineService 초기화.

        Args:
            fetch_service (KrxFetchService): KRX 데이터 수집 서비스.
            daily_port (DailyReportPort): 일별 리포트 저장 포트.
            master_port (MasterReportService): 마스터 리포트 서비스.
            ranking_port (RankingAnalysisService): 순위 분석 서비스.
            watchlist_port (WatchlistPort): 관심종목 저장 포트.
        """
        self.fetch_service = fetch_service
        self.daily_port = daily_port
        self.master_port = master_port
        self.ranking_port = ranking_port
        self.watchlist_port = watchlist_port

    def execute(self, date_str: Optional[str] = None):
        """전체 일일 루틴을 실행합니다.

        다음 단계를 순차적으로 실행합니다:
        0. 데이터 확보 (파일 로드 시도 -> 실패 시 웹 수집)
        1. 일별 리포트 저장 (수집 시에만)
        2. 마스터 리포트 업데이트
        3. 누적 상위종목 watchlist 저장
        4. 수급 순위표 업데이트
        5. 일별 관심종목 파일 저장

        Args:
            date_str (Optional[str]): 실행할 날짜 문자열 (YYYYMMDD). None일 경우 오늘 날짜를 사용합니다.
        """
        import datetime
        if date_str is None:
            date_str = datetime.date.today().strftime('%Y%m%d')

        print(f"\n=== [DailyRoutineService] 루틴 시작 (Date: {date_str}) ===")

        # Step 0: 데이터 확보 전략
        # 1. 먼저 로컬 파일 로드 시도
        data_list = self.daily_port.load_daily_reports(date_str)
        is_loaded_from_file = False

        if data_list:
            print(f"=== [DailyRoutineService] ✅ 기존 파일 발견 ({len(data_list)}건). KRX 수집을 건너뜁니다. ===")
            is_loaded_from_file = True
        else:
            # 2. 파일이 없으면 웹 수집 진행
            print(f"=== [DailyRoutineService] 파일 없음. KRX 웹 수집을 시작합니다. ===")
            data_list = self.fetch_service.fetch_all_data(date_str)
        
        if not data_list:
            print("=== [DailyRoutineService] 🚨 데이터 확보 실패 (수집/로드 불가). 루틴을 종료합니다. ===")
            return

        print(f"\n=== [DailyRoutineService] 데이터 확보 완료 ({len(data_list)}건). 리포트 작업 시작... ===")

        print("\n--- [Step 1] 일별 관심종목 파일 저장 (Prioritized) ---")
        self.watchlist_port.save_watchlist(data_list)

        print("\n--- [Step 2] 일별 리포트 저장 ---")
        self.daily_port.save_daily_reports(data_list)

        print("\n--- [Step 3] 마스터 리포트 업데이트 ---")
        # Master Report Update
        top_stocks_map = self.master_port.update_reports(data_list)

        print("\n--- [Step 4] 누적 상위종목 watchlist 저장 ---")
        if top_stocks_map:
            self.watchlist_port.save_cumulative_watchlist(top_stocks_map, date_str)
        else:
            print("  [DailyRoutineService] ⚠️ 누적 상위종목 데이터가 없습니다")

        print("\n--- [Step 5] 수급 순위표 업데이트 ---")
        self.ranking_port.update_ranking_report(data_list)

        print("\n=== [DailyRoutineService] 모든 루틴이 완료되었습니다. ===")
