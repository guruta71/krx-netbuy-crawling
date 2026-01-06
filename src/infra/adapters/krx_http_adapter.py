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
        
        # 세션 캐싱
        self.cached_cookies: Optional[dict] = None
        self.cached_user_agent: Optional[str] = None

    def _get_session_cookies_via_playwright(self) -> tuple[dict, str]:
        """Playwright를 사용하여 KRX 사이트에 접속하고 세션 쿠키와 User-Agent를 획득합니다.
        
        Returns:
            tuple[dict, str]: (쿠키 딕셔너리, User-Agent 문자열)
        """
        print("  [KrxHttp] Playwright로 세션 초기화 (쿠키 획득)...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=self.user_agent
            )
            page = context.new_page()
            
            try:
                # 0. Root 페이지 접속 (쿠키 초기화 및 보안/세션 쿠키 획득용)
                print("  [KrxHttp] Root 페이지 접속...")
                page.goto("https://data.krx.co.kr/", wait_until='networkidle', timeout=30000)

                # 1. 로그인 (안정적인 세션을 위해 수행)
                login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd?locale=ko_KR"
                page.goto(login_url, wait_until='networkidle', timeout=30000)
                
                try:
                    frame = page.frame_locator("#COMS001_FRAME")
                    if frame.locator("#mbrId").is_visible():
                        print("  [KrxHttp] 로그인 정보 입력...")
                        frame.locator("#mbrId").fill(self.username)
                        frame.locator("input[title='비밀번호']").fill(self.password)
                        frame.locator(".jsLoginBtn").click()
                        print("  [KrxHttp] 로그인 버튼 클릭. 메인 페이지 이동 대기...")
                        
                        # 명시적으로 URL 변경 대기 (최대 30초)
                        try:
                            # index.cmd로 이동하거나, 혹은 로그인이 이미 되어서 메인으로 갔을 수도 있음
                            page.wait_for_url("**/index.cmd", timeout=30000)
                            print(f"  [KrxHttp] 로그인 성공! URL: {page.url}")
                        except Exception as e:
                            print(f"  [KrxHttp] 로그인 이동 대기 시간 초과/실패: {e}")
                            print(f"  [KrxHttp] 현재 URL: {page.url}")
                            # 실패해도 일단 진행해볼 수 있으나, 보통 실패함.
                            
                except Exception as e:
                    print(f"  [KrxHttp] 로그인 시도 중 예외 (이미 로그인됨?): {e}")

                # 2. 통계 페이지 이동 (필수 쿠키 획득용)
                stat_url = "https://data.krx.co.kr/contents/MDC/MDCCOM02005.jsp?viewName=MDCSTAT02401"
                page.goto(stat_url, wait_until='networkidle', timeout=30000)
                page.wait_for_timeout(3000)
                
                # 3. 쿠키 및 User-Agent 추출
                cookies = context.cookies()
                cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
                
                user_agent = page.evaluate("navigator.userAgent")
                
                print(f"  [KrxHttp] 세션 쿠키 획득 완료 ({len(cookie_dict)}개): {list(cookie_dict.keys())}")
                return cookie_dict, user_agent
                
            except Exception as e:
                print(f"  [KrxHttp] Playwright 세션 획득 실패: {e}")
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
        
        Playwright로 세션을 맺은 후, HTTP 요청으로 OTP 발급 및 다운로드를 수행합니다.
        (세션 캐싱 및 재시도 로직 포함)
        """
        if date_str is None:
            target_date = datetime.date.today().strftime('%Y%m%d')
        else:
            target_date = date_str
        
        print(f"  [KrxHttp] {target_date} {market.value} {investor.value} 데이터 수집 시작")
        
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                # 1. 세션 쿠키 확인 및 획득
                if not self.cached_cookies:
                    self.cached_cookies, self.cached_user_agent = self._get_session_cookies_via_playwright()
                
                # 2. Cloudscraper 설정
                self.scraper.cookies.clear() # 이전 쿠키 제거
                self.scraper.cookies.update(self.cached_cookies)
                self.scraper.headers.update({
                    'User-Agent': self.cached_user_agent,
                    'Referer': 'https://data.krx.co.kr/contents/MDC/MDCCOM02005.jsp?viewName=MDCSTAT02401',
                    'Origin': 'https://data.krx.co.kr',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                })
                
                # 3. OTP 발급 요청
                otp_params = self._create_otp_params(market, investor, target_date)
                otp_response = self.scraper.post(self.otp_url, data=otp_params)
                otp_code = otp_response.text.strip()
                
                if len(otp_code) < 10 or 'LOGOUT' in otp_code:
                     if attempt < max_retries:
                         print(f"  [KrxHttp] 세션 만료/LOGOUT 감지. 세션 재설정 후 재시도합니다...")
                         self.cached_cookies = None # 캐시 초기화
                         continue # 재시도
                     else:
                        if '<html' in otp_code:
                             print(f"  [KrxHttp] OTP 응답이 HTML 입니다: {otp_code[:100]}...")
                        raise ConnectionError(f"OTP 발급 실패: {otp_code[:50]}...")
                
                print(f"  [KrxHttp] OTP 발급 성공")

                # 4. 파일 다운로드 요청
                download_response = self.scraper.post(
                    self.download_url,
                    data={'code': otp_code}
                )
                
                file_bytes = download_response.content
                
                if len(file_bytes) == 0:
                    print(f"  [KrxHttp] 경고: 0 바이트 파일 다운로드됨")
                else:
                    print(f"  [KrxHttp] 다운로드 완료 ({len(file_bytes)} bytes)")
                
                return file_bytes
                
            except Exception as e:
                print(f"  [KrxHttp] 데이터 수집 중 오류: {e}")
                if attempt < max_retries and self.cached_cookies is not None:
                     print("  [KrxHttp] 예외 발생으로 인한 재시도...")
                     self.cached_cookies = None
                     continue
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