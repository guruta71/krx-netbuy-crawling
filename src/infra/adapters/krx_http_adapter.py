# infra/adapters/krx_http_adapter.py
import datetime
import os
import time
import json
from typing import Optional
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext

from core.ports.krx_data_port import KrxDataPort
from core.domain.models import Market, Investor

class KrxHttpAdapter(KrxDataPort):
    """KrxDataPort의 구현체 (Pure Playwright Adapter).

    Playwright만 사용하여 로그인, OTP 발급, 파일 다운로드를 수행합니다.
    세션 불일치 문제를 해결하기 위해 브라우저 컨텍스트를 유지합니다.

    Attributes:
        otp_url (str): OTP 발급 URL.
        download_url (str): 데이터 다운로드 URL.
        session_file (str): 세션(쿠키/스토리지) 저장 파일 경로.
    """
    
    def __init__(self):
        """KrxHttpAdapter 초기화."""
        super().__init__()
        
        # 환경 변수 로드
        self.otp_url = os.getenv('KRX_OTP_URL')
        self.download_url = os.getenv('KRX_DOWNLOAD_URL')

        if not self.otp_url or not self.download_url:
            raise EnvironmentError("KRX_OTP_URL or KRX_DOWNLOAD_URL is not set in environment variables.")
        
        self.session_file = "krx_session.json"
        
    def _login_if_needed(self, context: BrowserContext) -> bool:
        """필요한 경우 로그인을 수행합니다.
        
        Args:
            context (BrowserContext): 브라우저 컨텍스트.
            
        Returns:
            bool: 로그인 성공(또는 이미 로그인됨) 여부.
        """
        page = context.new_page()
        try:
            # 세션 유효성 확인을 위해 로그인 후 접근 가능한 페이지 접속 시도
            # 타겟 메뉴: 투자자별 순매수 상위 (MDC0201020303)
            target_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303"
            print(f"  [KrxHttp] 세션 유효성 확인 중: {target_url}")
            
            response = page.goto(target_url, timeout=30000)
            page.wait_for_load_state('networkidle')
            
            # 확실한 로그인 체크: '로그아웃' 버튼이 있는지 확인
            # KRX는 로그인 시 상단에 '로그아웃' 버튼이 표시됨
            try:
                # 짧게 대기하며 '로그아웃' 텍스트 찾기
                logout_btn = page.get_by_text("로그아웃").first
                if logout_btn.is_visible():
                    print("  [KrxHttp] ✅ 세션이 유효합니다 (로그아웃 버튼 확인됨)")
                    return True
            except Exception:
                pass
            
            print("  [KrxHttp] ⚠️ 세션이 유효하지 않음 (로그아웃 버튼 없음). 로그인 시도 중...")
            
            # 로그인 절차 수행
            login_url = 'https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd'
            page.goto(login_url)
            page.wait_for_load_state('networkidle')
            
            username = os.getenv('KRX_USERNAME')
            password = os.getenv('KRX_PASSWORD')
            
            if not username or not password:
                print("  [KrxHttp] 🚨 경고: KRX 인증 정보를 찾을 수 없습니다.")
                return False

            # 로그인 프레임 찾기
            target_frame = None
            for frame in page.frames:
                if frame.locator('input[name="mbrId"]').count() > 0:
                    target_frame = frame
                    break
            
            if not target_frame:
                print("  [KrxHttp] 🚨 오류: 로그인 프레임을 찾을 수 없습니다.")
                return False
                
            target_frame.fill('input[name="mbrId"]', username)
            target_frame.fill('input[name="pw"]', password)
            
            # 엔터키로 로그인 시도
            target_frame.press('input[name="pw"]', 'Enter')
            
            # 로그인 완료 대기
            time.sleep(3)
            page.wait_for_load_state('networkidle')
            
            # 세션 갱신을 위해 메인/타겟 페이지 이동 (세션 쿠키가 확실히 셋팅되도록)
            page.goto(target_url)
            page.wait_for_load_state('networkidle')
            
            # 세션 저장 (다음 실행 시 재사용)
            context.storage_state(path=self.session_file)
            print("  [KrxHttp] ✅ 로그인 성공. 세션이 저장되었습니다.")
            return True
            
        except Exception as e:
            print(f"  [KrxHttp] 🚨 로그인 프로세스 실패: {e}")
            return False
        finally:
            page.close()

    def fetch_net_value_data(
        self, 
        market: Market, 
        investor: Investor, 
        date_str: Optional[str] = None
    ) -> bytes:
        """Playwright를 사용하여 데이터(Excel Bytes)를 가져옵니다.

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
            
        print(f"  [KrxHttp] {target_date} {market.value} {investor.value} 데이터 수집 중 (Playwright)...")

        with sync_playwright() as p:
            # 브라우저 실행
            browser = p.chromium.launch(headless=True)
            
            # 세션 로드 시도
            if os.path.exists(self.session_file):
                context = browser.new_context(
                    storage_state=self.session_file,
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
            else:
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
            
            try:
                # 로그인 체크 및 수행 (필요 시 세션 갱신)
                login_success = self._login_if_needed(context)
                if not login_success:
                    raise ConnectionError("로그인에 실패했습니다.")
                
                page = context.new_page()
                
                # API 호출을 위해 타겟 페이지로 이동
                print(f"  [KrxHttp] 타겟 페이지로 이동 중...")
                page.goto("https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303")
                page.wait_for_load_state('networkidle')

                # [DEBUG] 로그인 직후 화면 캡처
                screenshot_path = os.path.join(os.getcwd(), "debug_login_after.png")
                page.screenshot(path=screenshot_path)
                print(f"  [KrxHttp] [디버그] 스크린샷 저장: {screenshot_path}")

                # OTP 요청 (Browser Context 내에서 JS fetch 실행)
                otp_payload = self._create_otp_params(market, investor, target_date)
                
                print(f"  [KrxHttp] OTP 발급 요청 중 (Playwright Request API)...")
                
                # Context의 Request API를 사용하여 쿠키 포함
                # IMPORTANT: page.request가 아닌 context.request를 사용해야 쿠키가 포함됨
                response = context.request.post(
                    'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd',
                    data=otp_payload,
                    headers={
                        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303',
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                )
                
                otp_code = response.text()
                
                # [DEBUG] OTP 응답 상세 정보 출력
                print(f"  [KrxHttp] [디버그] OTP 응답 상태 코드: {response.status}")
                print(f"  [KrxHttp] [디버그] OTP 응답 길이: {len(otp_code)} 문자")
                print(f"  [KrxHttp] [디버그] OTP 코드: '{otp_code}'")
                
                # OTP 응답 검증
                if 'LOGOUT' in otp_code or len(otp_code) < 10:
                     # [DEBUG] 실패 시 화면 캡처
                     fail_shot = os.path.join(os.getcwd(), "debug_otp_fail.png")
                     page.screenshot(path=fail_shot)
                     print(f"  [KrxHttp] [디버그] 실패 스크린샷 저장: {fail_shot}")
                     raise ConnectionError(f"잘못된 OTP 응답 (LOGOUT?): {otp_code[:50]}")


                # 파일 다운로드 요청 (직접 POST 요청)
                print(f"  [KrxHttp] OTP로 파일 다운로드 중 (Direct POST)...")
                
                # Context의 Request API를 사용하여 쿠키 포함
                # IMPORTANT: page.request가 아닌 context.request를 사용해야 쿠키가 포함됨
                download_url = f'https://data.krx.co.kr/comm/fileDn/download_excel/download.cmd?code={otp_code}'
                download_response = context.request.post(
                    download_url,
                    headers={
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Origin': 'https://data.krx.co.kr'
                    }
                )
                
                # [DEBUG] 응답 상세 정보 확인
                print(f"  [KrxHttp] [디버그] 다운로드 응답 상태 코드: {download_response.status}")
                
                # [DEBUG] 응답 헤더 확인
                headers = download_response.headers
                print(f"  [KrxHttp] [디버그] Content-Type: {headers.get('content-type', 'N/A')}")
                print(f"  [KrxHttp] [디버그] Content-Length: {headers.get('content-length', 'N/A')}")
                print(f"  [KrxHttp] [디버그] Content-Disposition: {headers.get('content-disposition', 'N/A')}")
                
                # 파일 내용 읽기
                file_bytes = download_response.body()
                
                # [DEBUG] 파일이 비어있을 때 응답 내용 확인
                if len(file_bytes) == 0:
                    print(f"  [KrxHttp] ⚠️ 경고: 다운로드된 파일이 비어있습니다 (0 bytes)")
                    print(f"       → 날짜: {target_date}, 시장: {market.value}, 투자자: {investor.value}")
                    print(f"       → 휴장일이거나 데이터가 없는 날짜일 수 있습니다")
                else:
                    # 파일 크기가 작으면 (HTML 오류 메시지일 가능성) 내용 출력
                    if len(file_bytes) < 1000:
                        try:
                            content_preview = file_bytes.decode('utf-8', errors='ignore')[:500]
                            print(f"  [KrxHttp] [디버그] 응답 내용 미리보기: {content_preview}")
                        except:
                            pass
                
                print(f"  [KrxHttp] ✅ 다운로드 성공 ({len(file_bytes)} bytes)")
                
                return file_bytes
                
            except Exception as e:
                print(f"  [KrxHttp] 🚨 오류: Playwright 데이터 수집 실패: {e}")
                raise
            finally:
                context.close()
                browser.close()

    def _create_otp_params(self, market: Market, investor: Investor, target_date: str) -> dict:
        """KRX OTP 발급을 위한 요청 페이로드를 생성합니다."""
        
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
            raise ValueError(f"Unsupported market ID: {market}")

        if investor == Investor.INSTITUTIONS:
            params['invstTpCd'] = '7050'
        elif investor == Investor.FOREIGNER:
            params['invstTpCd'] = '9000'
        else:
            raise ValueError(f"Unsupported investor type: {investor}")
            
        return params