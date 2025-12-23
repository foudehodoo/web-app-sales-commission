# app/services/crm_service.py

import requests
import hashlib
import json
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# تنظیمات لاگ
logger = logging.getLogger(__name__)

# --- تنظیمات اتصال به CRM (طبق اطلاعات ارسالی شما) ---
CRM_API_URL = "https://foudehco.crm24.io"
CRM_USERNAME = "rezafoodeh@gmail.com"
CRM_ACCESS_KEY = "6CC2sDjhvHpaiGNp"  # همان crm_auth

# متغیرهای کش برای جلوگیری از درخواست‌های تکراری و بالا بردن سرعت
_CACHED_ACCOUNTS = []
_CACHED_USERS = []
_LAST_FETCH_TIME = 0
CACHE_DURATION = 3600  # کش تا یک ساعت معتبر است


def _get_session():
    """ایجاد نشست (Session) با قابلیت تلاش مجدد خودکار در صورت قطعی"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_challenge(session):
    """مرحله اول احراز هویت: دریافت توکن چالش"""
    url = f"{CRM_API_URL}/webservice.php"
    params = {'operation': 'getchallenge', 'username': CRM_USERNAME}

    # تایم‌اوت را ۱۰ ثانیه می‌گذاریم که اگر سرور پاسخ نداد برنامه فریز نشود
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def connect_to_crm():
    """
    اتصال به CRM و دریافت sessionName
    این تابع مراحل ساخت توکن MD5 و لاگین را انجام می‌دهد.
    """
    session = _get_session()
    try:
        # 1. دریافت Challenge Token
        challenge_data = _get_challenge(session)
        if not challenge_data.get('success'):
            logger.error("CRM Challenge Failed: Success flag is False")
            return None, None

        token = challenge_data['result']['token']

        # 2. ساخت کلید لاگین (md5(token + accessKey))
        # ترکیب توکن دریافتی و کلید ثابت شما، سپس هش کردن آن
        key_hash = hashlib.md5(
            (token + CRM_ACCESS_KEY).encode('utf-8')).hexdigest()

        payload = {
            'operation': 'login',
            'username': CRM_USERNAME,
            'accessKey': key_hash
        }

        # ارسال درخواست لاگین
        login_resp = session.post(
            f"{CRM_API_URL}/webservice.php", data=payload, timeout=10)
        login_data = login_resp.json()

        if login_data.get('success'):
            logger.info("Connected to CRM successfully.")
            return session, login_data['result']['sessionName']
        else:
            logger.error(f"CRM Login Failed: {login_data.get('error')}")
            return None, None

    except Exception as e:
        logger.error(f"CRM Connection Error: {e}")
        return None, None


def fetch_module_data(module_name: str, force_refresh=False):
    """
    دریافت تمام رکوردها (مثل Accounts یا Users)
    با قابلیت صفحه‌بندی (Pagination) چون CRM‌ها داده‌ها را ۱۰۰ تا ۱۰۰ تا می‌دهند.
    """
    global _CACHED_ACCOUNTS, _CACHED_USERS, _LAST_FETCH_TIME

    # بررسی کش: اگر دیتا تازه است (کمتر از یک ساعت)، همان قبلی را برگردان
    if not force_refresh and (time.time() - _LAST_FETCH_TIME < CACHE_DURATION):
        if module_name == 'Accounts' and _CACHED_ACCOUNTS:
            logger.info("Returning Accounts from CACHE")
            return _CACHED_ACCOUNTS
        if module_name == 'Users' and _CACHED_USERS:
            logger.info("Returning Users from CACHE")
            return _CACHED_USERS

    session, session_name = connect_to_crm()
    if not session:
        logger.warning("Could not connect to CRM to fetch data.")
        return []

    all_records = []
    offset = 0
    limit = 100
    url = f"{CRM_API_URL}/webservice.php"

    logger.info(f"Start fetching {module_name} from CRM...")

    while True:
        # کوئری SQL-like مخصوص Vtiger
        query = f"SELECT * FROM {module_name} LIMIT {offset},{limit};"
        params = {
            'operation': 'query',
            'sessionName': session_name,
            'query': query
        }
        try:
            resp = session.get(url, params=params, timeout=20)
            data = resp.json()

            if not data.get('success') or not data.get('result'):
                # پایان داده‌ها یا خطا
                break

            records = data['result']
            if not records:
                break

            all_records.extend(records)
            offset += limit

            # وقفه کوتاه (0.2 ثانیه) برای اینکه سرور CRM ما را بلاک نکند (Rate Limiting)
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"Error in fetching {module_name}: {e}")
            break

    logger.info(
        f"Finished fetching {module_name}. Total records: {len(all_records)}")

    # بروزرسانی متغیرهای کش
    if module_name == 'Accounts':
        _CACHED_ACCOUNTS = all_records
    elif module_name == 'Users':
        _CACHED_USERS = all_records

    _LAST_FETCH_TIME = time.time()
    return all_records
