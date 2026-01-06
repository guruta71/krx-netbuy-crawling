# infra/adapters/krx_http_adapter.py
import cloudscraper
import datetime
from typing import Optional
from playwright.sync_api import sync_playwright

from core.ports.krx_data_port import KrxDataPort
from core.domain.models import Market, Investor

class KrxHttpAdapter(KrxDataPort):
    """Cloudscraper를 사용한 KRX 데이터 어댑터
    
    Playwright를 사용하여 세션 쿠키를 획득하고,
    이후 데이터 다운로드는 순수 HTTP 요청으로 처리하는 하이브리드 방식
    """
    


    def __init__(self):
        """KrxHttpAdapter 초기화"""
        super().__init__()
        self.scraper = cloudscraper.create_scraper()
        self.otp_url = 'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        self.download_url = 'https://data.krx.co.kr/comm/fileDn/download_excel/download.cmd'
        # Playwright와 동일한 User-Agent 설정
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        self.scraper.headers.update({'User-Agent': self.user_agent})
        
        # 로그인 정보 (사용자 요청에 따라 하드코딩)
        self.username = 'zeya9643'
        self.password = 'chlwltjr43!'

    def _get_otp_code_via_playwright(self, otp_params: dict) -> tuple[str, dict]:
        """Playwright를 사용하여 로그인 후, UI 상의 다운로드 버튼을 클릭하고 
        발생하는 OTP 생성 요청을 가로채서 OTP 코드를 획득합니다.
        
        Args:
            otp_params: (참고용) 파라미터.
            
        Returns:
            tuple[str, dict]: (OTP 코드, 쿠키 딕셔너리)
        """
        print("  [KrxHttp] Playwright로 로그인 및 OTP 획득 시도...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=self.user_agent
            )
            page = context.new_page()
            
            # OTP 코드를 저장할 변수
            captured_otp = {"code": None}
            
            # 응답 핸들러
            def handle_response(response):
                if "generate.cmd" in response.url and response.ok:
                    try:
                        code = response.text().strip()
                        if len(code) > 10 and "LOGOUT" not in code:
                            print(f"  [KrxHttp] OTP 응답 감지됨 ({len(code)} 글자)")
                            captured_otp["code"] = code
                        else:
                            print(f"  [KrxHttp] 유효하지 않은 OTP 응답: {code[:20]}...")
                    except:
                        pass
            
            page.on("response", handle_response)
            
            try:
                # 1. 로그인 수행
                login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd?locale=ko_KR"
                page.goto(login_url, wait_until='networkidle', timeout=30000)
                
                # iframe 찾기
                frame = page.frame_locator("#COMS001_FRAME")
                
                print("  [KrxHttp] 로그인 정보 입력 중...")
                frame.locator("#mbrId").fill(self.username)
                frame.locator("input[title='비밀번호']").fill(self.password)
                
                # 로그인 버튼 클릭
                frame.locator(".jsLoginBtn").click()
                print("  [KrxHttp] 로그인 버튼 클릭. 대기 중...")
                
                # 로그인 완료 대기 (메인 페이지로 리다이렉트되거나 특정 요소가 나타날 때까지)
                # 안전하게 3초 대기
                page.wait_for_timeout(3000)
                
                # 2. 통계 페이지 이동
                # viewName을 지정하여 해당 서비스 모듈을 로드
                stat_url = "https://data.krx.co.kr/contents/MDC/MDCCOM02005.jsp?viewName=MDCSTAT02401"
                print(f"  [KrxHttp] 통계 페이지 이동: {stat_url}")
                page.goto(stat_url, wait_until='networkidle', timeout=30000)
                
                # 3. 다운로드 버튼 클릭
                # 버튼이 로드될 때까지 잠시 대기
                page.wait_for_timeout(2000)
                
                # .CI-MDI-UNIT-DOWNLOAD 클래스가 여러 개일 수 있으므로 첫 번째 것 사용하거나, 
                # 정확한 위치를 특정해야 함. 보통 상단 툴바에 있음.
                download_btn = page.locator(".CI-MDI-UNIT-DOWNLOAD").first
                
                if download_btn.is_visible():
                    download_btn.click()
                    print("  [KrxHttp] 다운로드 버튼 클릭함")
                    
                    # 팝업 메뉴(Excel/CSV)가 뜨는 경우 'Excel' 선택
                    # 보통 'csv'와 'excel' 클래스나 텍스트를 가진 버튼이 뜸
                    # 여기서는 단순히 클릭 후 대기 (바로 요청이 가는 경우도 있음)
                    
                    # 혹시 메뉴가 뜨는지 확인 (예: .cmd-down-excel)
                    excel_btn = page.locator(".cmd-down-excel, button:has-text('Excel'), button:has-text('CSV')") # CSV여도 OTP는 같음
                    if excel_btn.count() > 0 and excel_btn.first.is_visible():
                         print("  [KrxHttp] 엑셀/CSV 메뉴 감지됨. 클릭 시도.")
                         excel_btn.first.click()
                         
                    page.wait_for_timeout(2000) 
                else:
                    print("  [KrxHttp] 경고: 다운로드 버튼을 찾을 수 없음. 페이지 로드 상태 확인 필요.")
                    # 스크린샷 찍어서 확인해볼 수도 있음 (디버깅용)
                
                # 4. OTP 코드 획득 대기
                for _ in range(10): # 최대 5초 대기
                    if captured_otp["code"]:
                        break
                    page.wait_for_timeout(500)
                
                otp_code = captured_otp["code"]
                
                if not otp_code:
                     raise ConnectionError("로그인 후에도 OTP 코드를 캡처하지 못했습니다. (UI 변경 또는 로딩 지연 가능성)")

                print(f"  [KrxHttp] OTP 코드 획득 성공 (길이: {len(otp_code)})")
                
                # 5. 쿠키 추출
                cookies = context.cookies()
                cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
                
                return otp_code, cookie_dict
                
            except Exception as e:
                print(f"  [KrxHttp] Playwright 작업 중 오류: {e}")
                raise
            finally:
                browser.close()

    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """Cloudscraper를 사용하여 데이터(Excel Bytes)를 가져옵니다.

        Args:
            market (Market): 시장 구분 (KOSPI, KOSDAQ).
            investor (Investor): 투자자 구분 (외국인, 기관).
            date_str (Optional[str]): 대상 날짜 (YYYYMMDD).

        Returns:
            bytes: 다운로드된 엑셀 파일의 바이너리 데이터.
        """
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
        
        print(f"  [KrxHttp] {target_date} {market.value} {investor.value} 데이터 수집 중...")
        
        try:
            # 1. OTP 파라미터 생성
            otp_params = self._create_otp_params(market, investor, target_date)
            
            # 2. OTP 및 세션 쿠키 획득 (Playwright)
            otp_code, session_cookies = self._get_otp_code_via_playwright(otp_params)
            
            if 'LOGOUT' in otp_code or len(otp_code) < 10:
                 raise ConnectionError(f"잘못된 OTP 응답: {otp_code}")
            
            # 3. 데이터 다운로드 (HTTP - Cloudscraper)
            self.scraper.cookies.update(session_cookies)
            self.scraper.headers.update({
                'Referer': 'https://data.krx.co.kr/contents/MDC/MDCCOM02005.jsp'
            })
            
            print(f"  [KrxHttp] 파일 다운로드 중...")
            download_response = self.scraper.post(
                self.download_url,
                data={'code': otp_code}
            )
            
            print(f"  [KrxHttp] [디버그] 다운로드 응답 상태: {download_response.status_code}")
            print(f"  [KrxHttp] [디버그] Content-Type: {download_response.headers.get('content-type', 'N/A')}")
            
            file_bytes = download_response.content
            
            if len(file_bytes) == 0:
                print(f"  [KrxHttp] 경고: 다운로드된 파일이 비어있습니다 (0 bytes)")
                print(f"       -> 날짜: {target_date}, 시장: {market.value}, 투자자: {investor.value}")
                print(f"       -> 휴장일이거나 데이터가 없는 날짜일 수 있습니다")
            
            print(f"  [KrxHttp] 다운로드 성공 ({len(file_bytes)} bytes)")
            return file_bytes
            
        except Exception as e:
            print(f"  [KrxHttp] 오류: 데이터 수집 실패: {e}")
            raise
            

    
    def _create_otp_params(self, market: Market, investor: Investor, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 파라미터를 생성합니다.
        
        Args:
            market: 시장 구분
            investor: 투자자 구분
            target_date: 대상 날짜 (YYYYMMDD)
            
        Returns:
            dict: OTP 요청 파라미터
        """
        params = {
            'locale': 'ko_KR',
            'invstTpCd': '',
            'strtDd': target_date,
            'endDd': target_date,
            'share': '1',
            'money': '3',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
        }
        
        if market == Market.KOSPI:
            params['mktId'] = 'STK'
        elif market == Market.KOSDAQ:
            params['mktId'] = 'KSQ'
            params['segTpCd'] = 'ALL'
        else:
            raise ValueError(f"Unsupported market: {market}")
        
        if investor == Investor.INSTITUTIONS:
            params['invstTpCd'] = '7050'
        elif investor == Investor.FOREIGNER:
            params['invstTpCd'] = '9000'
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
        
        return params