import re
import jdatetime
import pandas as pd
from datetime import datetime

# ------------------ توابع تاریخ ------------------ #


def parse_jalali_or_gregorian(value):
    """
    ورودی: تاریخ به صورت شمسی مثل 1404/08/01 یا 1404-08-01 یا حتی datetime میلادی.
    خروجی: pandas.Timestamp میلادی یا NaT
    """
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value)
    s = str(value).strip()
    if not s:
        return pd.NaT
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        # اگر سال >= 1300 است، فرض می‌کنیم شمسی است
        if year >= 1300:
            try:
                jd = jdatetime.date(year, month, day)
                g = jd.togregorian()
                return pd.Timestamp(g.year, g.month, g.day)
            except Exception:
                return pd.NaT
        else:
            # احتمالاً میلادی است
            return pd.to_datetime(s, errors="coerce")
    # بقیهٔ فرمت‌ها را به pandas می‌سپاریم (میلادی)
    return pd.to_datetime(s, errors="coerce")


def to_jalali_str(ts):
    """
    تبدیل Timestamp میلادی به رشته تاریخ شمسی yyyy/mm/dd برای نمایش.
    """
    if pd.isna(ts):
        return ""
    if not isinstance(ts, (pd.Timestamp, datetime)):
        try:
            ts = pd.to_datetime(ts)
        except Exception:
            return str(ts)
    d = ts.date()
    try:
        jd = jdatetime.date.fromgregorian(date=d)
        return f"{jd.year:04d}/{jd.month:02d}/{jd.day:02d}"
    except Exception:
        return str(ts.date())

# ------------------ نرمال‌سازی اسم ------------------ #


def normalize_persian_name(s) -> str:
    """
    نرمال‌سازی اسم فارسی:
    - ي/ی و ك/ک و ... → معادل فارسی
    - حذف حرکات
    - یکسان‌سازی فاصله‌ها
    """
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    if not s:
        return ""
    replacements = {
        "ي": "ی",
        "ك": "ک",
        "ۀ": "ه",
        "ة": "ه",
        "ؤ": "و",
        "إ": "ا",
        "أ": "ا",
        "ٱ": "ا",
        "ئ": "ی",
        "‌": " ",   # نیم‌فاصله
    }
    for src, dst in replacements.items():
        s = s.replace(src, dst)
    # حذف حرکات
    s = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", s)
    # علائم به فاصله
    for ch in ["،", ",", "-", "_", "ـ"]:
        s = s.replace(ch, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()


def name_key_for_matching(s: str) -> str:
    """
    کلیدی که برای تطبیق استفاده می‌کنیم:
    - نرمال‌سازی فارسی
    - حذف تمام فاصله‌ها → چهارراهستانی == چهار راهستانی
    """
    norm = normalize_persian_name(s)
    return norm.replace(" ", "")

# ------------------ توابع کد و عدد ------------------ #


def canonicalize_code(value):
    """
    تبدیل کد عددی (مثلاً 13 یا 13.0 یا '13 ') به رشته تمیز.
    اگر قابل تبدیل به عدد نباشد، همان رشته را برمی‌گرداند.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    s_no_comma = s.replace(",", "")
    try:
        f = float(s_no_comma)
        if f.is_integer():
            return str(int(f))
    except Exception:
        return s
    return s


def format_number(value):
    """
    فرمت کردن عدد به صورت سه رقم سه رقم (مثلاً 1,000,000).
    برای استفاده در جینجا و نمایش گزارشات.
    """
    if value is None:
        return "0"
    try:
        return "{:,.0f}".format(float(value))
    except (ValueError, TypeError):
        return str(value)
