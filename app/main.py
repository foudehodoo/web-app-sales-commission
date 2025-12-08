from __future__ import annotations
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.services.sales_excel_loader import load_sales_excel
from app.services.payments_excel_loader import load_payments_excel
from app.services.checks_excel_loader import load_checks_excel

from datetime import datetime
import jdatetime
from fastapi import FastAPI, UploadFile, File, Request
import pandas as pd
import re
import os
import json

# ------------------ تنظیمات فایل‌های پیکربندی ------------------ #

DEFAULT_GROUP_CONFIG_PATH = "group_config.xlsx"
PRODUCT_GROUP_MAP_PATH = "product_group_map.xlsx"


def load_default_group_config(path: str = DEFAULT_GROUP_CONFIG_PATH) -> dict:
    """
    خواندن تنظیمات پیش‌فرض گروه‌ها از یک اکسل:
    ستون‌ها: Group, Percent, DueDays, IsCash
    - Group : اسم گروه کالا (مثلاً "نقدی ۲٪ هفت روزه")
    - Percent : درصد پورسانت (به صورت انسانی: 2 یعنی 2٪)
    - DueDays : مهلت تسویه (روز)
    - IsCash : 0/1 یا True/False
    خروجی: دیکشنری
        group_name -> {percent, due_days, is_cash}
    که percent به صورت ضریب (0.02) برمی‌گردد.
    """
    if not os.path.exists(path):
        return {}

    df = pd.read_excel(path)

    cfg: dict[str, dict] = {}

    for _, row in df.iterrows():
        key = str(row.get("Group", "")).strip()
        if not key:
            continue

        # درصد (در اکسل به صورت درصد انسانی ذخیره شده است)
        percent_val = 0.0
        p = row.get("Percent")
        if pd.notna(p):
            try:
                percent_val = float(p) / 100.0
            except ValueError:
                percent_val = 0.0

        # مهلت تسویه
        due_days_val = None
        d = row.get("DueDays")
        if pd.notna(d):
            try:
                due_days_val = int(float(d))
            except ValueError:
                due_days_val = None

        # نقدی بودن
        is_cash_val = bool(row.get("IsCash"))

        cfg[key] = {
            "percent": percent_val,
            "due_days": due_days_val,
            "is_cash": is_cash_val,
        }

    return cfg


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


def load_product_group_map(path: str = PRODUCT_GROUP_MAP_PATH) -> pd.DataFrame:
    """
    خواندن مپ کد کالا → نام گروه کالا از اکسل.
    ستون‌ها: ProductCode, ProductName, Group
    """
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ProductCode", "ProductName", "Group"])

    df = pd.read_excel(path)

    for c in ["ProductCode", "ProductName", "Group"]:
        if c not in df.columns:
            df[c] = None

    # نرمال‌سازی کد کالا
    df["ProductCode"] = df["ProductCode"].map(
        lambda v: canonicalize_code(v) if pd.notna(v) else None
    )

    return df[["ProductCode", "ProductName", "Group"]]


def save_product_group_map(df: pd.DataFrame, path: str = PRODUCT_GROUP_MAP_PATH) -> None:
    """
    ذخیره‌ی مپ کد کالا → گروه در اکسل.
    """
    cols = ["ProductCode", "ProductName", "Group"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df_out = df[cols].copy()
    df_out.to_excel(path, index=False)


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


# ------------------ کانفیگ برنامه ------------------ #

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    "group_col": None,
    "group_config": None,
    "sales_result": None,
    "payments_result": None,
}

BASE_CSS = """
<style>
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Tahoma, sans-serif;
    direction: rtl;
    margin: 0;
    min-height: 100vh;

    /* گرادیانت چندلایه  */
    background:
        radial-gradient(circle at 0% 0%, rgba(59, 130, 246, 0.35) 0, transparent 55%),
        radial-gradient(circle at 100% 0%, rgba(236, 72, 153, 0.28) 0, transparent 55%),
        radial-gradient(circle at 0% 100%, rgba(16, 185, 129, 0.25) 0, transparent 55%),
        linear-gradient(135deg, #eef2ff, #f9fafb 40%, #fdf2ff 100%);
}

.container {
    max-width: 1150px;
    margin: 32px auto;
    background: rgba(255, 255, 255, 0.92);   /* نیمه‌شفاف برای افکت شیشه‌ای */
    padding: 24px 32px 32px;
    border-radius: 24px;
    box-shadow: 0 28px 80px rgba(15, 23, 42, 0.28);
    border: 1px solid rgba(148, 163, 184, 0.35);
    backdrop-filter: blur(18px);             /* اگر مرورگر پشتیبانی کند 🤌 */
}

h1 {
    margin-top: 0;
    color: #111827;
    font-size: 22px;
}
h2 {
    color: #111827;
    font-size: 18px;
    margin-top: 24px;
}
p {
    color: #374151;
    font-size: 13px;
}
button {
    background: linear-gradient(135deg, #2563eb, #1d4ed8);
    color: #ffffff;
    border: none;
    border-radius: 999px;
    padding: 9px 18px;
    font-size: 13px;
    cursor: pointer;
    box-shadow: 0 6px 14px rgba(37, 99, 235, 0.35);
    transition: transform 0.15s ease, box-shadow 0.15s ease, background 0.15s ease;
}
button:hover {
    background: linear-gradient(135deg, #1d4ed8, #1e40af);
    transform: translateY(-1px);
    box-shadow: 0 10px 22px rgba(37, 99, 235, 0.45);
}
label {
    font-weight: 600;
    font-size: 13px;
}
input[type="file"],
input[type="number"],
input[type="text"],
select {
    width: 100%;
    padding: 7px 9px;
    border-radius: 10px;
    border: 1px solid #d1d5db;
    font-size: 13px;
    box-sizing: border-box;
    transition: border-color 0.15s, box-shadow 0.15s, background 0.15s;
    background-color: #f9fafb;
}
input[type="file"]:focus,
input[type="number"]:focus,
input[type="text"]:focus,
select:focus {
    outline: none;
    border-color: #2563eb;
    box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.18);
    background-color: #ffffff;
}
.form-row {
    margin-bottom: 14px;
}
small {
    font-size: 11px;
    color: #6b7280;
}

/* ---------------- نوار بالای صفحه (سه تب اصلی) ---------------- */

.navbar {
    display: flex;
    gap: 8px;
    margin-bottom: 18px;
    border-radius: 999px;
    background: #f3f4ff;
    padding: 4px;
}
.navbar a {
    flex: 0 0 auto;
    padding: 7px 14px;
    border-radius: 999px;
    font-size: 13px;
    color: #4b5563;
    text-decoration: none;
    transition: background 0.15s ease, color 0.15s ease, box-shadow 0.15s ease;
}
.navbar a:hover {
    background: #e5e7ff;
    color: #111827;
}
.navbar a.active {
    background: linear-gradient(135deg, #2563eb, #7c3aed);
    color: #ffffff;
    box-shadow: 0 6px 16px rgba(37, 99, 235, 0.45);
}

/* ---------------- کارت‌های راهنمای صفحه اصلی ---------------- */

.summary-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 14px;
    margin: 18px 0 10px;
}
.summary-card {
    position: relative;
    background: rgba(248, 250, 252, 0.92);
    border-radius: 18px;
    padding: 12px 14px 10px 14px;
    border: 1px solid rgba(226, 232, 240, 0.95);
    overflow: hidden;
    display: flex;
    flex-direction: column;
    gap: 6px;
    transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease, background 0.15s ease;
}
.summary-card::before {
    content: "";
    position: absolute;
    inset-inline-start: 0;
    top: 0;
    bottom: 0;
    width: 4px;
}
.summary-sales::before {
    background: linear-gradient(180deg, #2563eb, #60a5fa);
}
.summary-payments::before {
    background: linear-gradient(180deg, #059669, #34d399);
}
.summary-checks::before {
    background: linear-gradient(180deg, #d97706, #fbbf24);
}
.summary-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 18px 45px rgba(15, 23, 42, 0.22);
    border-color: rgba(148, 163, 184, 0.7);
    background: rgba(255, 255, 255, 0.98);
}
.summary-card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
}
.summary-title {
    display: flex;
    align-items: center;
    gap: 8px;
}
.summary-icon {
    width: 28px;
    height: 28px;
    border-radius: 999px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    background: #e5edff;
}
.summary-sales .summary-icon {
    background: #e0ecff;
}
.summary-payments .summary-icon {
    background: #dcfce7;
}
.summary-checks .summary-icon {
    background: #fef3c7;
}
.summary-title-main {
    font-size: 13px;
    font-weight: 700;
    color: #111827;
}
.summary-title-sub {
    font-size: 11px;
    color: #6b7280;
}
.summary-card-body {
    margin-top: 4px;
}
.hint-title {
    font-size: 11px;
    color: #4b5563;
    margin-bottom: 4px;
}
.hint-note {
    font-size: 11px;
    color: #9ca3af;
    margin-top: 4px;
}
.pill-row {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}
.badge-pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
    background: #eef2ff;
    color: #3730a3;
    white-space: nowrap;
}
.pill-section-title {
    font-size: 11px;
    font-weight: 600;
    margin-top: 2px;
    margin-bottom: 2px;
    color: #4b5563;
}
.pill-section {
    margin-top: 4px;
    margin-bottom: 4px;
}
.pill-button {
    border-radius: 999px;
    border: 0;
    padding: 3px 10px;
    font-size: 11px;
    background: #e5edff;
    color: #1d4ed8;
    cursor: pointer;
    box-shadow: none;
}
.pill-button:hover {
    background: #dbeafe;
    transform: none;
    box-shadow: none;
}
.hint-hidden {
    display: none;
}

/* ---------------- جدول‌ها ---------------- */

.table-wrapper {
    overflow-x: auto;
    margin-top: 8px;
}
.table-wrapper table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
}
.table-wrapper th,
.table-wrapper td {
    border: 1px solid #e5e7eb;
    padding: 6px 8px;
    text-align: right;
    white-space: nowrap;
}
.table-wrapper th {
    background: #e5f0ff;
    color: #111827;
    font-weight: 600;
}
.table-wrapper tr:nth-child(even) {
    background: #f9fafb;
}
.table-wrapper tr:hover td {
    background: #eef2ff;
}

/* ---------------- بج‌ها ---------------- */

.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
}
.badge-priority-cash {
    background: #ecfdf5;
    color: #047857;
    border: 1px solid #bbf7d0;
}
.badge-priority-normal {
    background: #eff6ff;
    color: #1d4ed8;
    border: 1px solid #bfdbfe;
}

/* ---------------- پیام‌های موفق/خطا ---------------- */

.message {
    padding: 8px 12px;
    border-radius: 10px;
    font-size: 12px;
    margin: 10px 0;
}
.message-success {
    background: #ecfdf5;
    border: 1px solid #6ee7b7;
    color: #065f46;
}
.message-error {
    background: #fef2f2;
    border: 1px solid #fecaca;
    color: #991b1b;
}

/* ---------------- تب‌های داخلی (اگر جایی استفاده شوند) ---------------- */

.tabs-container {
    margin-top: 24px;
}
.tab-header-row {
    display: flex;
    gap: 8px;
    border-bottom: 1px solid #e5e7eb;
    margin-bottom: 12px;
    padding-bottom: 2px;
}
.tab-btn {
    border: none;
    background: transparent;
    padding: 8px 14px;
    border-radius: 999px 999px 0 0;
    font-size: 12px;
    color: #6b7280;
    cursor: pointer;
    position: relative;
    transition: background 0.15s ease, color 0.15s ease;
}
.tab-btn:hover {
    color: #111827;
    background: #f3f4ff;
}
.tab-btn.active {
    color: #111827;
    background: #eef2ff;
    font-weight: 600;
}
.tab-btn.active::after {
    content: "";
    position: absolute;
    left: 10%;
    right: 10%;
    bottom: -1px;
    height: 2px;
    border-radius: 999px;
    background: linear-gradient(90deg, #2563eb, #7c3aed);
}
.tab-content {
    margin-top: 4px;
}
.tab-pane {
    display: none;
}
.tab-pane.active {
    display: block;
}
.tab-card {
    margin-top: 18px;
    background: #f9fafb;
    border-radius: 14px;
    border: 1px solid #e5e7eb;
    padding: 12px 14px;
}

/* -------------- دیباگ -------------- */

.debug-section {
    margin-top: 24px;
}

.debug-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}

.debug-title {
    font-size: 15px;
    font-weight: 600;
    color: #111827;
    display: flex;
    align-items: center;
    gap: 6px;
}

.debug-toggle-btn {
    background: #f3f4f6;
    color: #374151;
    border-radius: 999px;
    padding: 5px 12px;
    font-size: 11px;
    border: 1px solid #e5e7eb;
    cursor: pointer;
}

.debug-toggle-btn:hover {
    background: #e5e7eb;
}

.debug-panel {
    border-radius: 12px;
    border: 1px dashed #e5e7eb;
    padding: 10px 12px;
    background: #f9fafb;
    margin-bottom: 4px;
}

.debug-hidden {
    display: none;
}

/* ردیف‌هایی از چک‌ها که متناظر در پرداخت‌ها دارند */
.matched-check-row {
    background-color: #ecfdf3;
}

.matched-check-row:hover {
    background-color: #dcfce7;
}

/* ---------------- سایر ---------------- */

.footer-link {
    display: inline-block;
    margin-top: 16px;
    color: #2563eb;
    text-decoration: none;
    font-size: 13px;
}
.footer-link:hover {
    text-decoration: underline;
}
hr {
    border: none;
    border-top: 1px solid #e5e7eb;
    margin: 24px 0;
}
.checkbox-center {
    text-align: center;
}
/* --------- modal نمودار مشتری --------- */
.modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.45);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 50;
}
.modal-hidden {
    display: none;
}
.modal-card {
    background: #ffffff;
    border-radius: 18px;
    padding: 16px 18px 18px;
    width: 720px;
    max-width: 95vw;
    box-shadow: 0 24px 60px rgba(15, 23, 42, 0.25);
}
.modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}
.modal-title {
    font-size: 15px;
    font-weight: 700;
    color: #111827;
}
.modal-subtitle {
    font-size: 12px;
    color: #6b7280;
    margin-top: 2px;
}
.modal-close-btn {
    background: #f3f4f6;
    color: #374151;
    border-radius: 999px;
    border: 1px solid #e5e7eb;
    padding: 4px 9px;
    font-size: 12px;
    cursor: pointer;
}
.modal-close-btn:hover {
    background: #e5e7eb;
}
.modal-body {
    margin-top: 6px;
}
.modal-totals {
    margin-top: 10px;
    font-size: 12px;
    color: #374151;
}
.modal-totals strong {
    font-weight: 700;
}
/* -------- صفحه اصلی (آپلود فایل‌ها) -------- */

.hero-intro {
    margin-top: 4px;
    margin-bottom: 18px;
}

.hero-intro h1 {
    margin-bottom: 6px;
}

.hero-intro p {
    font-size: 13px;
    color: #4b5563;
}

.upload-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
    gap: 18px;
    align-items: flex-start;
    margin-top: 10px;
}

@media (max-width: 900px) {
    .upload-grid {
        grid-template-columns: 1fr;
    }
}

.upload-card {
    background: rgba(249, 250, 252, 0.94);
    border-radius: 18px;
    padding: 16px 16px 14px;
    border: 1px solid rgba(226, 232, 240, 0.95);
    box-shadow: 0 14px 40px rgba(15, 23, 42, 0.12);
}

.upload-card-light {
    background: rgba(255, 255, 255, 0.86);
    box-shadow: 0 10px 28px rgba(148, 163, 184, 0.20);
}

.upload-card-title {
    font-size: 15px;
    font-weight: 700;
    color: #111827;
    margin-bottom: 6px;
}

.upload-card-subtitle {
    font-size: 12px;
    color: #6b7280;
    margin-bottom: 10px;
}

</style>
"""


def build_nav(active: str) -> str:
    def cls(tab: str) -> str:
        return "active" if tab == active else ""
    return f'''
    <div class="navbar">
        <a href="/" class="{cls("main")}">محاسبه پورسانت</a>
        <a href="/group-config" class="{cls("config")}">تعریف گروه‌های کالا (پیش‌فرض)</a>
        <a href="/group-items" class="{cls("items")}">تخصیص کالا به گروه</a>
    </div>
    '''


# ------------------ توابع کمکی ------------------ #

def get_priority(product_group: str) -> str:
    """
    fallback: اگر تنظیمی نداشتیم، از روی نام گروه نقدی/عادی را حدس می‌زنیم.
    """
    text = str(product_group)
    if "نقدی" in text:
        return "cash"
    return "normal"


def build_name_code_mapping(sales_df: pd.DataFrame) -> dict[str, str]:
    """
    از روی جدول فروش، map می‌سازد:
        نام نرمال‌شده (بدون فاصله) → کد مشتری (استاندارد شده)
    فقط وقتی که آن نام دقیقاً به *یک* کد منجر شود.
    """
    if "CustomerName" not in sales_df.columns or "CustomerCode" not in sales_df.columns:
        return {}

    tmp = sales_df[["CustomerName", "CustomerCode"]].dropna()
    name_to_codes: dict[str, set[str]] = {}

    for _, row in tmp.iterrows():
        key = name_key_for_matching(row["CustomerName"])
        code = canonicalize_code(row["CustomerCode"])
        if not key or not code:
            continue
        name_to_codes.setdefault(key, set()).add(code)

    result: dict[str, str] = {}
    for key, codes in name_to_codes.items():
        if len(codes) == 1:
            result[key] = next(iter(codes))
    return result


def extract_customer_for_payment(
    row: pd.Series,
    checks_df: pd.DataFrame,
    name_code_map: dict[str, str] | None = None,
):
    """
    تشخیص کد مشتری برای هر پرداخت:
    ترتیب اعتماد:
    1) اگر نام مشتری را می‌توانیم به‌طور یکتا از روی فروش به کد وصل کنیم → همان
    2) اگر نوع پرداخت "Check" باشد → از روی فایل چک‌ها (CheckNumber → CustomerName → کد مشتری)
    3) برای بقیه‌ی پرداخت‌ها، اگر CustomerCode پر است → همان
    """
    stype = row.get("SourceType")
    code_raw = row.get("CustomerCode")
    name = row.get("CustomerName")
    desc_str = str(row.get("Description") or "")

    # 1) اگر از روی نام (در خود پرداخت) می‌توانیم مپ یکتا به کد مشتری پیدا کنیم
    if name_code_map is not None and pd.notna(name):
        key = name_key_for_matching(name)
        if key:
            mapped = name_code_map.get(key)
            if mapped:
                return canonicalize_code(mapped)

    # 2) اگر نوع پرداخت چک است، اولویت ۱۰۰٪ با فایل چک‌هاست
    if stype == "Check" and checks_df is not None and not checks_df.empty:
        candidates: list[str] = []

        # 2.a از ستون CheckNumber که در لودر پرداخت‌ها ساخته‌ایم
        if "CheckNumber" in row.index:
            check_val = row["CheckNumber"]
            if pd.notna(check_val):
                candidates.append(str(check_val))

        # 2.b از توضیحات (اگر عدد ۳ تا ۱۰ رقمی داخلش باشد)
        m = re.search(r"(\d{3,10})", desc_str)
        if m:
            candidates.append(m.group(1))

        # آماده‌سازی ستون شماره چک در دیتافریم چک‌ها
        chk_nums = None
        if "CheckNumber" in checks_df.columns:
            chk_nums = (
                checks_df["CheckNumber"]
                .astype(str)
                .str.replace(r"\\D", "", regex=True)
                .str.lstrip("0")
            )

        for cand in candidates:
            num = re.sub(r"\\D", "", str(cand))
            num = num.lstrip("0")
            if not num:
                continue

            if chk_nums is not None:
                matches = checks_df.loc[chk_nums == num]
            else:
                matches = pd.DataFrame()

            if not matches.empty:
                chk_row = matches.iloc[0]

                # اگر خود فایل چک‌ها CustomerCode داشته باشد:
                if "CustomerCode" in chk_row and pd.notna(chk_row["CustomerCode"]):
                    return canonicalize_code(chk_row["CustomerCode"])

                # در غیر این صورت، از روی "صاحب حساب" → map نام→کد فروش‌ها را چک می‌کنیم
                if name_code_map is not None and "CustomerName" in chk_row:
                    chk_name = chk_row["CustomerName"]
                    if pd.notna(chk_name):
                        key2 = name_key_for_matching(chk_name)
                        mapped2 = name_code_map.get(key2)
                        if mapped2:
                            return canonicalize_code(mapped2)

        # اگر برای ردیف‌های چک از فایل چک‌ها چیزی پیدا نکردیم،
        # بهتر است None برگردانیم تا در خروجی بفهمی این پرداخت بی‌صاحب مانده،
        # نه این‌که اشتباهی به کدی مثل "12/02" وصل شود.
        return None

    # 3) برای سایر انواع پرداخت (غیر از Check)، اگر CustomerCode داریم، همان را استفاده می‌کنیم
    if pd.notna(code_raw) and str(code_raw).strip() != "":
        return canonicalize_code(code_raw)

    return None


def prepare_payments(
    payments_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    sales_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم پرداخت‌ها و وصل کردن هر پرداخت به یک مشتری.
    """
    payments_df = payments_df.copy()

    # تاریخ
    if "PaymentDate" in payments_df.columns:
        payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
            parse_jalali_or_gregorian
        )

    # مبلغ
    if "Amount" not in payments_df.columns:
        raise ValueError(
            "در فایل پرداخت‌ها نتوانستم ستون مبلغ را پیدا کنم."
        )
    payments_df["Amount"] = payments_df["Amount"].astype(float)

    # ستون‌های کمکی
    if "CustomerCode" not in payments_df.columns:
        payments_df["CustomerCode"] = None
    if "CustomerName" not in payments_df.columns:
        payments_df["CustomerName"] = None

    # map نام→کد
    name_code_map = build_name_code_mapping(sales_df)

    payments_df["ResolvedCustomer"] = payments_df.apply(
        lambda row: extract_customer_for_payment(
            row, checks_df, name_code_map),
        axis=1,
    )
    payments_df["ResolvedCustomerKey"] = payments_df["ResolvedCustomer"].map(
        canonicalize_code
    )

    return payments_df


def prepare_sales(sales_df: pd.DataFrame, group_config: dict, group_col: str) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم فروش‌ها:
    - تبدیل تاریخ‌ها
    - تعیین CustomerKey استاندارد
    - محاسبه DueDate و Priority بر اساس تنظیمات گروه
    - تعیین درصد پورسانت
    """
    sales_df = sales_df.copy()

    if "InvoiceDate" not in sales_df.columns:
        raise ValueError("در فایل فروش ستونی به نام 'InvoiceDate' پیدا نشد.")
    sales_df["InvoiceDate"] = sales_df["InvoiceDate"].apply(
        parse_jalali_or_gregorian
    )

    # CustomerKey استاندارد برای وصل کردن به پرداخت‌ها
    if "CustomerCode" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerCode"].map(
            canonicalize_code)
    elif "CustomerName" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerName"].map(
            lambda v: name_key_for_matching(v) if pd.notna(v) else None
        )
    else:
        sales_df["CustomerKey"] = None

    # اگر DueDate داشتیم، تبدیل کنیم؛ اگر نه، بعداً حساب می‌کنیم
    if "DueDate" in sales_df.columns:
        sales_df["DueDate"] = sales_df["DueDate"].apply(
            parse_jalali_or_gregorian)
    else:
        sales_df["DueDate"] = pd.NaT

    def compute_due_date(row):
        invoice_date = row["InvoiceDate"]
        if pd.isna(invoice_date):
            return pd.NaT

        if not pd.isna(row["DueDate"]):
            return row["DueDate"]

        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        due_days = None
        if cfg is not None:
            due_days = cfg.get("due_days")

        if not due_days or due_days <= 0:
            base_priority = get_priority(row.get(group_col, ""))
            due_days = 7 if base_priority == "cash" else 90

        return invoice_date + pd.to_timedelta(due_days, unit="D")

    sales_df["DueDate"] = sales_df.apply(compute_due_date, axis=1)

    def compute_priority(row):
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        if cfg is not None:
            return "cash" if cfg.get("is_cash") else "normal"

        try:
            delta_days = (row["DueDate"] - row["InvoiceDate"]).days
            if delta_days <= 7:
                return "cash"
        except Exception:
            pass

        return get_priority(row.get(group_col, ""))

    sales_df["Priority"] = sales_df.apply(compute_priority, axis=1)
    sales_df["PriorityRank"] = (
        sales_df["Priority"].map(
            {"cash": 0, "normal": 1}).fillna(1).astype(int)
    )

    def row_percent(row):
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        if cfg is None:
            return 0.0
        return float(cfg.get("percent", 0.0))

    if "Amount" not in sales_df.columns:
        raise ValueError("در فایل فروش ستونی به نام 'Amount' پیدا نشد.")

    sales_df["CommissionPercent"] = sales_df.apply(row_percent, axis=1)
    sales_df["Amount"] = sales_df["Amount"].astype(float)
    sales_df["PaidAmount"] = 0.0
    sales_df["Remaining"] = sales_df["Amount"]
    sales_df["CommissionAmount"] = 0.0

    return sales_df


def compute_commissions(
    sales_raw: pd.DataFrame,
    payments_raw: pd.DataFrame,
    checks_raw: pd.DataFrame,
    group_config: dict,
    group_col: str,
):
    """
    هسته‌ی محاسبات:
    - آماده‌سازی فروش‌ها و پرداخت‌ها
    - تسویه فاکتورها طبق اولویت (نقدی → عادی، قدیمی → جدید)
    - محاسبه پورسانت
    """
    sales_df = prepare_sales(sales_raw, group_config, group_col)

    checks_df = (
        checks_raw.copy()
        if checks_raw is not None and not checks_raw.empty
        else pd.DataFrame()
    )
    payments_df = prepare_payments(payments_raw, checks_df, sales_df)

    # اگر پرداختی نداریم
    if payments_df.empty:
        salesperson_df = (
            sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
            .sum()
            .reset_index()
        )
        salesperson_df.rename(
            columns={"CommissionAmount": "TotalCommission"}, inplace=True
        )
        return sales_df, salesperson_df, payments_df

    # تسویه بر اساس CustomerKey استاندارد
    for cust_key, pay_group in payments_df.groupby("ResolvedCustomerKey"):
        if cust_key is None or (isinstance(cust_key, float) and pd.isna(cust_key)):
            continue
        if str(cust_key).strip() == "":
            continue

        cust_invoice_idx = sales_df.index[sales_df["CustomerKey"] == cust_key]
        if len(cust_invoice_idx) == 0:
            continue

        cust_invoice_idx = (
            sales_df.loc[cust_invoice_idx]
            .sort_values(["PriorityRank", "InvoiceDate"])
            .index
        )

        if "PaymentDate" in pay_group.columns:
            pay_group = pay_group.sort_values("PaymentDate")

        for _, p in pay_group.iterrows():
            remaining_payment = p["Amount"]
            pay_date = p.get("PaymentDate", None)

            for idx in cust_invoice_idx:
                if remaining_payment <= 0:
                    break

                remaining_invoice = sales_df.at[idx, "Remaining"]
                if remaining_invoice <= 0:
                    continue

                allocate = min(remaining_payment, remaining_invoice)

                in_due = True
                if isinstance(pay_date, (pd.Timestamp, datetime)):
                    in_due = bool(pay_date <= sales_df.at[idx, "DueDate"])

                if in_due:
                    percent = sales_df.at[idx, "CommissionPercent"]
                    sales_df.at[idx, "CommissionAmount"] += allocate * percent

                sales_df.at[idx, "PaidAmount"] += allocate
                sales_df.at[idx, "Remaining"] -= allocate
                remaining_payment -= allocate

    salesperson_df = (
        sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
        .sum()
        .reset_index()
    )
    salesperson_df.rename(
        columns={"CommissionAmount": "TotalCommission"}, inplace=True
    )

    return sales_df, salesperson_df, payments_df


def build_debug_names_html(sales_df: pd.DataFrame, payments_df: pd.DataFrame) -> str:
    """
    بخش دیباگ نام‌ها:
    - نام مشتری در فروش + نام نرمال‌شده
    - نام مشتری در پرداخت + کدهای تشخیص داده‌شده
    - نگاشت name_key → کد مشتری
    همه این‌ها داخل یک پنل تاشونده نمایش داده می‌شوند.
    """
    inner_parts: list[str] = []

    # ---- نام‌ها در فروش ----
    if "CustomerName" in sales_df.columns and "CustomerCode" in sales_df.columns:
        sales_view = sales_df[["CustomerCode", "CustomerName"]].dropna(
            how="all"
        ).copy()

        # تمیز کردن کد مشتری فقط برای نمایش (حذف .0 و ...)
        sales_view["CustomerCode"] = sales_view["CustomerCode"].map(
            lambda v: canonicalize_code(v) if pd.notna(v) else ""
        )

        sales_view["NormName"] = sales_view["CustomerName"].apply(
            normalize_persian_name
        )
        sales_view = sales_view.drop_duplicates().sort_values(
            ["CustomerCode", "CustomerName"]
        )

        inner_parts.append("<h3>🧾 دیباگ نام‌ها (فروش)</h3>")
        inner_parts.append('<div class="table-wrapper">')
        inner_parts.append(sales_view.to_html(index=False, border=0))
        inner_parts.append("</div>")
    else:
        inner_parts.append(
            "<p>در جدول فروش ستون‌های CustomerName / CustomerCode پیدا نشد.</p>"
        )

    # ---- نام‌ها در پرداخت‌ها ----
    if not payments_df.empty:
        cols = []
        for c in [
            "PaymentID",
            "CheckNumber",
            "CustomerCode",
            "CustomerName",
            "ResolvedCustomer",
            "ResolvedCustomerKey",
            "Amount",
        ]:
            if c in payments_df.columns:
                cols.append(c)

        if cols:
            pay_view = payments_df[cols].copy()
            pay_view = pay_view.head(200)

            # تمیز کردن کد مشتری فقط برای نمایش
            if "CustomerCode" in pay_view.columns:
                pay_view["CustomerCode"] = pay_view["CustomerCode"].map(
                    lambda v: canonicalize_code(v) if pd.notna(v) else ""
                )

            inner_parts.append("<h3>💳 دیباگ نام‌ها (پرداخت‌ها)</h3>")
            inner_parts.append(
                '<p style="font-size:12px;color:#6b7280;">'
                "ستون ResolvedCustomer/ResolvedCustomerKey نشان می‌دهد این ردیف به کدام کد مشتری وصل شده (اگر شده باشد).</p>"
            )
            inner_parts.append('<div class="table-wrapper">')
            inner_parts.append(pay_view.to_html(index=False, border=0))
            inner_parts.append("</div>")
    else:
        inner_parts.append("<p>هیچ پرداختی بعد از لود یافت نشد.</p>")

    # ---- نگاشت name_key → کد مشتری ----
    name_code_map = build_name_code_mapping(sales_df)
    if name_code_map:
        map_rows = []
        for key, code in sorted(name_code_map.items(), key=lambda x: x[1]):
            map_rows.append(
                {
                    "NameKey (برای تطبیق)": key,
                    "CustomerCode": code,
                }
            )
        map_df = pd.DataFrame(map_rows)

        inner_parts.append("<h3>🔗 نگاشت نام نرمال‌شده → کد مشتری</h3>")
        inner_parts.append(
            '<p style="font-size:12px;color:#6b7280;">'
            "این جدول نشان می‌دهد که هر نام نرمال‌شده به کدام کد مشتری در فروش‌ها وصل شده است.</p>"
        )
        inner_parts.append('<div class="table-wrapper">')
        inner_parts.append(map_df.to_html(index=False, border=0))
        inner_parts.append("</div>")

    inner_html = "\n".join(inner_parts)

    # رپر تاشونده
    html = f"""
    <div class="debug-section">
        <div class="debug-header">
            <div class="debug-title">🧪 دیباگ نام‌ها</div>
            <button type="button" class="debug-toggle-btn" data-toggle="debug" data-target="debug-names-panel">
                نمایش / مخفی کردن
            </button>
        </div>
        <div id="debug-names-panel" class="debug-panel debug-hidden">
            {inner_html}
        </div>
    </div>
    """
    return html


def build_debug_checks_html(checks_df, payments_df=None):
    """
    دیباگ چک‌ها:
    - نشان دادن شماره چک، مبلغ، صاحب حساب و ...
    - هایلایت کردن چک‌هایی که در پرداخت‌ها استفاده شده‌اند (با رنگ سبز)
    """
    if checks_df is None or checks_df.empty:
        return ""

    # ستون‌هایی که نمایش می‌دهیم
    cols = []
    for c in [
        "CheckNumber",
        "CustomerName",
        "Amount",
        "DueDate",
        "Status",
        "CheckSerial",
        "CheckIndex",
    ]:
        if c in checks_df.columns:
            cols.append(c)

    if not cols:
        return ""

    checks_view = checks_df[cols].copy().head(200)

    # ست شماره چک‌هایی که در پرداخت‌ها استفاده شده‌اند
    matched_numbers = set()
    if (
        payments_df is not None
        and not payments_df.empty
        and "CheckNumber" in payments_df.columns
        and "SourceType" in payments_df.columns
    ):
        ser = (
            payments_df.loc[payments_df["SourceType"]
                            == "Check", "CheckNumber"]
            .dropna()
            .astype(str)
        )
        ser_norm = ser.str.replace(r"\D", "", regex=True).str.lstrip("0")
        matched_numbers = set(v for v in ser_norm.tolist() if v)

    # ردیف‌های HTML
    rows_html = []

    for _, row in checks_view.iterrows():
        raw_val = row.get("CheckNumber", "")
        key = re.sub(r"\D", "", str(raw_val or "")).lstrip("0")
        is_matched = bool(key and key in matched_numbers)

        row_class = ' class="matched-check-row"' if is_matched else ""
        cell_html = []
        for col in cols:
            val = row.get(col, "")
            cell_html.append(f"<td>{val if pd.notna(val) else ''}</td>")

        rows_html.append(f"<tr{row_class}>" + "".join(cell_html) + "</tr>")

    table_html = [
        "<div class='table-wrapper'>",
        "<table>",
        "<thead><tr>",
        *[f"<th>{c}</th>" for c in cols],
        "</tr></thead>",
        "<tbody>",
        *rows_html,
        "</tbody></table></div>",
    ]

    inner = (
        '<p style="font-size:12px;color:#6b7280;">'
        "ردیف‌های سبز یعنی برای این شماره چک، پرداخت متناظر در فایل پرداخت‌ها پیدا شده است."
        "</p>"
        + "\n".join(table_html)
    )

    html = f"""
    <div class="debug-section">
        <div class="debug-header">
            <div class="debug-title">🧪 دیباگ چک‌ها</div>
            <button type="button" class="debug-toggle-btn" data-toggle="debug" data-target="debug-checks-panel">
                نمایش / مخفی کردن
            </button>
        </div>
        <div id="debug-checks-panel" class="debug-panel debug-hidden">
            {inner}
        </div>
    </div>
    """
    return html

# ------------------ UI: تب ۱ – محاسبه پورسانت ------------------ #


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    nav_html = build_nav("main")
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "محاسبه پورسانت فروش",
            "nav_html": nav_html,
            "base_css": BASE_CSS,
            # active_tab الان استفاده نمی‌شود؛ می‌تونی حذفش کنی
        },
    )


@app.post("/upload-all", response_class=HTMLResponse)
async def upload_all(
    sales_file: UploadFile = File(...),
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None),
):
    nav_html = build_nav("main")

    df_sales = load_sales_excel(sales_file.file)
    df_pay = load_payments_excel(payments_file.file)

    if checks_file is not None and checks_file.filename:
        df_chk = load_checks_excel(checks_file.file)
    else:
        df_chk = pd.DataFrame()

    # تشخیص ستون گروه کالا
    if "ProductCode" in df_sales.columns:
        group_col = "ProductCode"
    elif "ProductGroup" in df_sales.columns:
        group_col = "ProductGroup"
    else:
        html = f"""
        <html>
            <head>
                <meta charset="utf-8" />
                <title>خطا در فایل فروش‌ها</title>
                {BASE_CSS}
            </head>
            <body>
                <div class="container">
                    {nav_html}
                    <h1>خطا در فایل فروش‌ها</h1>
                    <p>در فایل فروش‌ها ستونی به نام <b>ProductCode</b> یا <b>ProductGroup</b> پیدا نشد.</p>
                    <p>لطفاً یکی از این ستون‌ها را به اکسل اضافه کن و دوباره امتحان کن.</p>
                    <a class="footer-link" href="/">بازگشت به صفحه آپلود</a>
                </div>
            </body>
        </html>
        """
        return HTMLResponse(content=html)

    groups = sorted(df_sales[group_col].dropna().unique())

    LAST_UPLOAD["sales"] = df_sales
    LAST_UPLOAD["payments"] = df_pay
    LAST_UPLOAD["checks"] = df_chk
    LAST_UPLOAD["group_col"] = group_col

    # 📥 خواندن تنظیمات پیش‌فرض گروه‌ها
    default_group_cfg = load_default_group_config()

    # 📥 خواندن مپ کد کالا → گروه
    prod_group_df = load_product_group_map()
    code_to_category: dict[str, str] = {}
    if not prod_group_df.empty:
        for _, row in prod_group_df.iterrows():
            code = canonicalize_code(row.get("ProductCode"))
            grp = str(row.get("Group") or "").strip()
            if code and grp:
                code_to_category[code] = grp

    # حدس ستون نام گروه/کالا برای نمایش
    name_col_candidates = [
        "ProductName",
        "ProductGroupName",
        "ProductGroupTitle",
        "نام کالا",
        "نام گروه کالا",
    ]
    group_name_col = None
    for c in name_col_candidates:
        if c in df_sales.columns and c != group_col:
            group_name_col = c
            break

    # آماده‌سازی داده برای جاوااسکریپت (منوی کشویی گروه کالا)
    js_cfg_map = {
        gname: {
            "percent": (cfg.get("percent") or 0) * 100,  # درصد انسانی برای UI
            "due_days": cfg.get("due_days"),
            "is_cash": bool(cfg.get("is_cash")),
        }
        for gname, cfg in default_group_cfg.items()
    }
    js_cfg_json = json.dumps(js_cfg_map, ensure_ascii=False)

    # ساخت ردیف‌های جدول مرحله ۲
    rows_html = ""
    for g in groups:
        # 🔑 مقدار اصلیِ کلید (برای منطق محاسبه) – همون چیزی که توی دیتافریم هست
        key_str = str(g)

        # 🎨 مقدار «خوشگل‌شده» فقط برای نمایش (حذف .0 و ...)
        pretty_str = canonicalize_code(g)
        if pretty_str is None:
            pretty_str = ""

        # پیدا کردن نام خوانا برای این گروه
        display_name = ""
        if group_name_col is not None:
            sample_rows = df_sales[df_sales[group_col] == g]
            if not sample_rows.empty:
                display_name = str(sample_rows.iloc[0][group_name_col])

        if display_name:
            display_text = f"{pretty_str} – {display_name}"
        else:
            # اگر canonical نشد، خود key_str را نشان بده
            display_text = pretty_str or key_str

        # انتخاب گروه پیش‌فرض (category) از روی مپ کالا→گروه (اگر group_col == ProductCode)
        category_for_code = None
        if group_col == "ProductCode":
            canon_code = canonicalize_code(g)
            if canon_code:
                category_for_code = code_to_category.get(canon_code)

        pre_cfg = None
        selected_category = ""

        # ۱) اگر از روی مپ کالا→گروه گروهی پیدا شد
        if category_for_code and category_for_code in default_group_cfg:
            selected_category = category_for_code
            pre_cfg = default_group_cfg[category_for_code]
        # ۲) اگر خود کلید (همون مقدار اصلی ستون) نام یکی از گروه‌های پیش‌فرض بود
        elif key_str in default_group_cfg:
            selected_category = key_str
            pre_cfg = default_group_cfg[key_str]

        # مقدار ورودی‌ها
        if pre_cfg:
            percent_value_attr = f'value="{(pre_cfg.get("percent") or 0) * 100:.2f}"'
            due_days_val = pre_cfg.get("due_days")
            due_days_value_attr = (
                f'value="{due_days_val}"' if due_days_val is not None else ""
            )
            checked_attr = "checked" if pre_cfg.get("is_cash") else ""
        else:
            percent_value_attr = ""
            due_days_value_attr = ""
            checked_attr = ""
            selected_category = selected_category or ""

        # منوی کشویی گروه کالا
        options_html = '<option value="">-- انتخاب کن --</option>'
        for cat_name, cfg in default_group_cfg.items():
            cat_percent = (cfg.get("percent") or 0) * 100
            cat_due = cfg.get("due_days")
            cat_is_cash = cfg.get("is_cash")
            label_parts = [cat_name]
            label_parts.append(f"{cat_percent:.2f}٪")
            if cat_due is not None:
                label_parts.append(f"{cat_due} روز")
            if cat_is_cash:
                label_parts.append("نقدی")
            option_label = " | ".join(label_parts)

            sel_attr = "selected" if cat_name == selected_category else ""
            options_html += f'<option value="{cat_name}" {sel_attr}>{option_label}</option>'

        rows_html += f"""
            <tr>
                <td>{display_text}</td>
                <td>
                    <!-- ⚠️ این مقدار hidden همان key_str است تا منطق group_config و prepare_sales به‌هم نخورد -->
                    <input type="hidden" name="group_name" value="{key_str}" />
                    <select name="group_category" onchange="onCategoryChange(this)">
                        {options_html}
                    </select>
                </td>
                <td>
                    <input type="number" step="0.01" name="group_percent"
                           placeholder="مثلاً 2 برای 2٪" {percent_value_attr} />
                </td>
                <td>
                    <input type="number" step="1" name="group_due_days"
                           placeholder="مثلاً 7، 30، 90" {due_days_value_attr} />
                </td>
                <td class="checkbox-center">
                    <input type="checkbox" name="cash_group" value="{key_str}" {checked_attr} />
                </td>
            </tr>
        """

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تنظیم گروه‌ها و پورسانت</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                {nav_html}
                <h1>تعریف تنظیمات پورسانت و مهلت تسویه برای گروه‌های کالایی</h1>
                <p>مرحله ۲ از ۲ – برای هر گروه (بر اساس ستون <b>{group_col}</b>) موارد زیر را پر کن:</p>
                <ul style="font-size:12px; color:#4b5563;">
                    <li>ستون <b>گروه کالا</b> از روی صفحهٔ «تعریف گروه‌های کالا (پیش‌فرض)» خوانده می‌شود.</li>
                    <li>با انتخاب هر گروه کالا، درصد پورسانت / مهلت تسویه / نقدی بودن به‌صورت خودکار پر می‌شود (امکان ویرایش دستی هم هست).</li>
                    <li>اگر در تب «تخصیص کالا به گروه» کد کالاها را به گروه‌ها داده باشی، اینجا به‌صورت خودکار پر می‌شود.</li>
                </ul>

                <form action="/calculate-commission" method="post">
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>کد/گروه کالا + نام</th>
                                <th>گروه کالا (from پیش‌فرض)</th>
                                <th>درصد پورسانت (%)</th>
                                <th>مهلت تسویه (روز)</th>
                                <th>اولویت نقدی</th>
                            </tr>
                            {rows_html}
                        </table>
                    </div>
                    <br/>
                    <button type="submit">محاسبه پورسانت</button>
                </form>

                <a class="footer-link" href="/">بازگشت به آپلود فایل‌ها</a>
            </div>

            <script>
                const CATEGORY_CONFIG = {js_cfg_json};

                function onCategoryChange(sel) {{
                    const code = sel.value;
                    if (!code) return;
                    const cfg = CATEGORY_CONFIG[code];
                    if (!cfg) return;
                    const row = sel.closest('tr');
                    if (!row) return;

                    const percentInput = row.querySelector('input[name="group_percent"]');
                    const dueInput = row.querySelector('input[name="group_due_days"]');
                    const cashCheckbox = row.querySelector('input[name="cash_group"]');

                    if (percentInput) {{
                        percentInput.value = cfg.percent != null ? cfg.percent : "";
                    }}
                    if (dueInput) {{
                        if (cfg.due_days != null && cfg.due_days !== undefined) {{
                            dueInput.value = cfg.due_days;
                        }} else {{
                            dueInput.value = "";
                        }}
                    }}
                    if (cashCheckbox) {{
                        cashCheckbox.checked = !!cfg.is_cash;
                    }}
                }}
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


# ------------------ /calculate-commission ------------------ #
DEBUG_TOGGLE_SCRIPT = """
<script>
document.addEventListener('DOMContentLoaded', function () {
    var buttons = document.querySelectorAll('[data-toggle="debug"]');
    buttons.forEach(function (btn) {
        btn.addEventListener('click', function () {
            var targetId = btn.getAttribute('data-target');
            var panel = document.getElementById(targetId);
            if (!panel) return;
            panel.classList.toggle('debug-hidden');
        });
    });
});
</script>
"""


@app.post("/calculate-commission", response_class=HTMLResponse)
async def calculate_commission(request: Request):
    nav_html = build_nav("main")

    if LAST_UPLOAD["sales"] is None or LAST_UPLOAD["payments"] is None:
        html = f"""
        <html>
            <head>
                <meta charset="utf-8" />
                <title>خطا</title>
                {BASE_CSS}
            </head>
            <body>
                <div class="container">
                    {nav_html}
                    <h1>خطا</h1>
                    <p>ابتدا باید فایل‌های اکسل را در مرحله قبل آپلود کنی.</p>
                    <a class="footer-link" href="/">بازگشت به آپلود فایل‌ها</a>
                </div>
            </body>
        </html>
        """
        return HTMLResponse(content=html)

    form = await request.form()
    group_names = form.getlist("group_name")
    categories = form.getlist("group_category")
    percents = form.getlist("group_percent")
    due_days_list = form.getlist("group_due_days")
    cash_groups = set(form.getlist("cash_group"))

    group_config: dict = {}
    for name, cat, p, dd in zip(group_names, categories, percents, due_days_list):
        key = str(name).strip()
        if not key:
            continue

        # درصد
        percent_val = 0.0
        p_str = str(p).strip()
        if p_str:
            p_str = p_str.replace(",", ".")
            try:
                percent_val = float(p_str) / 100.0
            except ValueError:
                percent_val = 0.0

        # مهلت تسویه
        due_days_val = None
        dd_str = str(dd).strip()
        if dd_str:
            try:
                due_days_val = int(float(dd_str))
            except ValueError:
                due_days_val = None

        is_cash = key in cash_groups

        group_config[key] = {
            "percent": percent_val,
            "due_days": due_days_val,
            "is_cash": is_cash,
            "category": str(cat).strip() if cat else None,
        }

    if not group_config:
        html = f"""
        <html>
            <head>
                <meta charset="utf-8" />
                <title>خطا</title>
                {BASE_CSS}
            </head>
            <body>
                <div class="container">
                    {nav_html}
                    <h1>خطا</h1>
                    <p>هیچ تنظیم معتبری برای گروه‌ها وارد نشده است.</p>
                    <a class="footer-link" href="javascript:history.back()">بازگشت</a>
                </div>
            </body>
        </html>
        """
        return HTMLResponse(content=html)

    df_sales = LAST_UPLOAD["sales"]
    df_pay = LAST_UPLOAD["payments"]
    df_chk = LAST_UPLOAD["checks"]
    group_col = LAST_UPLOAD["group_col"]

    LAST_UPLOAD["group_config"] = group_config

    sales_result, salesperson_result, payments_result = compute_commissions(
        df_sales, df_pay, df_chk, group_config, group_col
    )
    # 🔹 نتایج را برای استفاده در نمودار مشتری‌ها نگه می‌داریم
    LAST_UPLOAD["sales_result"] = sales_result
    LAST_UPLOAD["payments_result"] = payments_result

    # -------- خلاصه اعداد --------
    sales_rows = len(sales_result)
    sales_sum = sales_result["Amount"].sum(
    ) if "Amount" in sales_result.columns else 0

    pay_rows = len(payments_result)
    pay_sum = payments_result["Amount"].sum(
    ) if "Amount" in payments_result.columns else 0

    chk_rows = len(df_chk) if df_chk is not None and not df_chk.empty else 0
    chk_sum = df_chk["Amount"].sum(
    ) if chk_rows > 0 and "Amount" in df_chk.columns else 0

    total_commission = 0
    if "TotalCommission" in salesperson_result.columns:
        total_commission = float(
            salesperson_result["TotalCommission"].sum() or 0)

    # -------- آماده‌سازی جدول فاکتورها برای نمایش --------
    invoices_view = sales_result.copy()

    # تاریخ‌ها به شمسی
    for dt_col in ["InvoiceDate", "DueDate"]:
        if dt_col in invoices_view.columns:
            invoices_view[dt_col] = invoices_view[dt_col].map(to_jalali_str)

    # درصد به صورت انسانی (عدد درصد)
    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = (
            invoices_view["CommissionPercent"] * 100
        ).round(2)

    # نرمال‌سازی کدها فقط برای نمایش (حذف .0 و تبدیل به رشته تمیز)
    for col in ["InvoiceID", "CustomerCode", group_col]:
        if col in invoices_view.columns:
            invoices_view[col] = invoices_view[col].map(
                lambda v: canonicalize_code(v) if pd.notna(v) else ""
            )

    # 🔹 لینک‌دار کردن اسم مشتری برای نمایش نمودار
    if "CustomerName" in invoices_view.columns and "CustomerCode" in invoices_view.columns:
        def make_customer_link(row):
            name = row.get("CustomerName", "")
            code = row.get("CustomerCode", "")
            if pd.isna(name) or str(name).strip() == "":
                return ""
            return (
                f'<a href="#" class="customer-link" '
                f'data-customer-code="{code}" '
                f'data-customer-name="{name}">{name}</a>'
            )

        invoices_view["CustomerName"] = invoices_view.apply(
            make_customer_link, axis=1)

    # بج رنگی Priority
    if "Priority" in invoices_view.columns:
        def pri_badge(v):
            if v == "cash":
                return '<span class="badge badge-priority-cash">نقدی</span>'
            elif v == "normal":
                return '<span class="badge badge-priority-normal">عادی</span>'
            return ""
        invoices_view["Priority"] = invoices_view["Priority"].map(pri_badge)

    # تبدیل درصد به رشته با علامت ٪
    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = invoices_view["CommissionPercent"].map(
            lambda x: f"{x:.2f}٪"
        )

    # گرد کردن مبالغ
    for col in ["Amount", "PaidAmount", "Remaining", "CommissionAmount"]:
        if col in invoices_view.columns:
            invoices_view[col] = invoices_view[col].round(0).astype("int64")

    cols = []
    for c in [
        "InvoiceID",
        "CustomerCode",
        "CustomerName",
        group_col,
        "Priority",
        "InvoiceDate",
        "DueDate",
        "Amount",
        "PaidAmount",
        "Remaining",
        "CommissionPercent",
        "CommissionAmount",
    ]:
        if c in invoices_view.columns:
            cols.append(c)

    invoices_table_html = ""
    if cols:
        invoices_table_html = invoices_view[cols].to_html(
            index=False, border=0, escape=False
        )

    # جدول پورسانت به تفکیک فروشنده
    if "TotalCommission" in salesperson_result.columns:
        salesperson_result["TotalCommission"] = (
            salesperson_result["TotalCommission"].round(0).astype("int64")
        )
    salesperson_table_html = salesperson_result.to_html(index=False, border=0)

    # دیباگ
    debug_names_html = build_debug_names_html(sales_result, payments_result)
    debug_checks_html = build_debug_checks_html(df_chk, payments_result)

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>نتیجه محاسبه پورسانت</title>
            {BASE_CSS}
            <!-- Chart.js برای نمودار مشتری -->
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        </head>
        <body>
            <div class="container">
                {nav_html}
                <h1>نتیجه محاسبه پورسانت</h1>

                <div class="summary-grid">
                    <div class="summary-card summary-sales">
                        <div class="label">فروش‌ها</div>
                        <div class="value">تعداد ردیف‌ها: {sales_rows:,}</div>
                        <div class="value">جمع مبلغ فروش‌ها: {sales_sum:,.0f}</div>
                    </div>
                    <div class="summary-card summary-payments">
                        <div class="label">پرداخت‌ها</div>
                        <div class="value">تعداد ردیف‌ها: {pay_rows:,}</div>
                        <div class="value">جمع مبلغ پرداخت‌ها: {pay_sum:,.0f}</div>
                    </div>
                    <div class="summary-card summary-checks">
                        <div class="label">چک‌ها</div>
                        <div class="value">تعداد ردیف‌ها: {chk_rows:,}</div>
                        <div class="value">جمع مبلغ چک‌ها: {chk_sum:,.0f}</div>
                    </div>
                    <div class="summary-card summary-commission">
                        <div class="label">پورسانت کل</div>
                        <div class="value">{total_commission:,.0f}</div>
                    </div>
                </div>

                <hr/>

                <h2>جزئیات فاکتورها و پورسانت هر فاکتور</h2>
                <div class="table-wrapper">
                    {invoices_table_html}
                </div>

                {debug_names_html}
                {debug_checks_html}

                <hr/>

                <h2>پورسانت نهایی به تفکیک فروشنده</h2>
                <div class="table-wrapper">
                    {salesperson_table_html}
                </div>

                <a class="footer-link" href="/">شروع دوباره (آپلود فایل‌های جدید)</a>
            </div>

            <!-- مودال نمودار مشتری -->
            <div id="customer-modal" class="modal-backdrop modal-hidden">
                <div class="modal-card">
                    <div class="modal-header">
                        <div>
                            <div class="modal-title" id="modal-customer-title"></div>
                            <div class="modal-subtitle" id="modal-customer-subtitle"></div>
                        </div>
                        <button type="button" class="modal-close-btn" id="modal-close-btn">بستن</button>
                    </div>
                    <div class="modal-body">
                        <div style="height:260px;">
                            <canvas id="customer-chart"></canvas>
                        </div>
                        <div class="modal-totals">
                            جمع خرید: <strong id="total-amount"></strong>
                            &nbsp;|&nbsp;
                            جمع تسویه: <strong id="total-paid"></strong>
                            &nbsp;|&nbsp;
                            مانده: <strong id="total-remaining"></strong>
                        </div>
                    </div>
                </div>
            </div>

            {DEBUG_TOGGLE_SCRIPT}

            <script>
            (function() {{
                let chartInstance = null;

                function closeModal() {{
                    const modal = document.getElementById('customer-modal');
                    if (modal) modal.classList.add('modal-hidden');
                }}

                function openModal() {{
                    const modal = document.getElementById('customer-modal');
                    if (modal) modal.classList.remove('modal-hidden');
                }}

                // کلیک روی اسم مشتری
                document.addEventListener('click', function (ev) {{
                    const link = ev.target.closest('.customer-link');
                    if (!link) return;
                    ev.preventDefault();

                    const code = link.getAttribute('data-customer-code') || '';
                    const name = link.getAttribute('data-customer-name') || '';

                    if (!code) {{
                        alert('کد مشتری مشخص نیست.');
                        return;
                    }}

                    fetch('/customer-stats?customer_code=' + encodeURIComponent(code))
                        .then(r => r.json())
                        .then(data => {{
                            if (data.error) {{
                                alert(data.error);
                                return;
                            }}

                            document.getElementById('modal-customer-title').textContent =
                                data.customerName || name || 'مشتری بدون نام';
                            document.getElementById('modal-customer-subtitle').textContent =
                                'کد مشتری: ' + (data.customerCode || code);

                            document.getElementById('total-amount').textContent =
                                (data.totals.amount || 0).toLocaleString('fa-IR');
                            document.getElementById('total-paid').textContent =
                                (data.totals.paid || 0).toLocaleString('fa-IR');
                            document.getElementById('total-remaining').textContent =
                                (data.totals.remaining || 0).toLocaleString('fa-IR');

                            const points = data.points || [];
                            const labels = points.map(p => p.date || '');
                            const amount = points.map(p => p.amount || 0);
                            const paid = points.map(p => p.paid || 0);
                            const remaining = points.map(p => p.remaining || 0);

                            const canvas = document.getElementById('customer-chart');
                            if (!canvas) return;
                            const ctx = canvas.getContext('2d');

                            if (chartInstance) {{
                                chartInstance.destroy();
                            }}

                            chartInstance = new Chart(ctx, {{
                                type: 'line',
                                data: {{
                                    labels: labels,
                                    datasets: [
                                        {{ label: 'خرید', data: amount, tension: 0.2 }},
                                        {{ label: 'تسویه', data: paid, tension: 0.2 }},
                                        {{ label: 'مانده', data: remaining, tension: 0.2 }}
                                    ]
                                }},
                                options: {{
                                    responsive: true,
                                    maintainAspectRatio: false,
                                    interaction: {{ mode: 'index', intersect: false }},
                                    scales: {{
                                        y: {{
                                            ticks: {{
                                                callback: function(v) {{
                                                    try {{ return v.toLocaleString('fa-IR'); }} catch(e) {{ return v; }}
                                                }}
                                            }}
                                        }}
                                    }}
                                }}
                            }});

                            openModal();
                        }})
                        .catch(err => {{
                            console.error(err);
                            alert('خطا در دریافت اطلاعات مشتری.');
                        }});
                }});

                // بستن مودال با کلیک روی دکمه یا پس‌زمینه
                document.addEventListener('click', function (ev) {{
                    const modal = document.getElementById('customer-modal');
                    if (!modal || modal.classList.contains('modal-hidden')) return;

                    const closeBtn = document.getElementById('modal-close-btn');
                    if (ev.target === closeBtn || (closeBtn && closeBtn.contains(ev.target))) {{
                        closeModal();
                        return;
                    }}
                    if (ev.target === modal) {{
                        closeModal();
                        return;
                    }}
                }});

                // بستن با ESC
                document.addEventListener('keydown', function (ev) {{
                    if (ev.key === 'Escape') {{
                        closeModal();
                    }}
                }});
            }})();
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/customer-stats")
async def customer_stats(customer_code: str):
    """
    برگرداندن آمار خرید/تسویه/مانده برای یک مشتری مشخص،
    برای استفاده در نمودار.
    """
    sales_result = LAST_UPLOAD.get("sales_result")
    payments_result = LAST_UPLOAD.get("payments_result")

    if sales_result is None or payments_result is None:
        return JSONResponse(
            {"error": "ابتدا باید محاسبه پورسانت انجام شود."},
            status_code=400,
        )

    code_key = canonicalize_code(customer_code)

    # فاکتورهای مرتبط با این مشتری
    if "CustomerKey" in sales_result.columns:
        sales_rows = sales_result[sales_result["CustomerKey"]
                                  == code_key].copy()
    else:
        sales_rows = pd.DataFrame()

    # پرداخت‌های مرتبط با این مشتری
    if "ResolvedCustomerKey" in payments_result.columns:
        pay_rows = payments_result[payments_result["ResolvedCustomerKey"] == code_key].copy(
        )
    else:
        pay_rows = pd.DataFrame()

    # نقاط نمودار: بر اساس فاکتورها
    points = []
    if not sales_rows.empty:
        sales_rows = sales_rows.sort_values("InvoiceDate")
        for _, row in sales_rows.iterrows():
            inv_date = row.get("InvoiceDate")
            date_label = to_jalali_str(inv_date)

            amount = float(row.get("Amount") or 0)
            paid = float(row.get("PaidAmount") or 0)
            remaining = float(row.get("Remaining") or 0)

            points.append(
                {
                    "date": date_label,
                    "amount": amount,
                    "paid": paid,
                    "remaining": remaining,
                    "invoice_id": row.get("InvoiceID"),
                }
            )

    total_amount = sum(p["amount"] for p in points)
    total_paid = sum(p["paid"] for p in points)
    total_remaining = sum(p["remaining"] for p in points)

    # سعی می‌کنیم اسم مشتری را از روی اولین فاکتور پیدا کنیم
    customer_name = ""
    if not sales_rows.empty and "CustomerName" in sales_rows.columns:
        customer_name = str(sales_rows.iloc[0].get("CustomerName") or "")

    return JSONResponse(
        {
            "customerCode": code_key,
            "customerName": customer_name,
            "points": points,
            "totals": {
                "amount": total_amount,
                "paid": total_paid,
                "remaining": total_remaining,
            },
        }
    )

# ------------------ UI: تب ۲ – مدیریت پیش‌فرض گروه‌های کالا ------------------ #


@app.get("/group-config", response_class=HTMLResponse)
async def group_config_page():
    nav_html = build_nav("config")

    # خواندن داده‌های فعلی
    current_cfg = load_default_group_config()

    rows = list(current_cfg.items())
    rows_html = ""

    # فقط ردیف‌های موجود (دیگه ۵ سطر خالی اضافه نمی‌کنیم)
    for idx, (gname, cfg) in enumerate(rows):
        percent_human = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)
        due_str = "" if due_days is None else str(due_days)
        checked_attr = "checked" if is_cash else ""

        rows_html += f"""
        <tr>
            <td><input type="text" name="cfg_group" value="{gname}" /></td>
            <td><input type="number" step="0.01" name="cfg_percent" value="{percent_human:.2f}" /></td>
            <td><input type="number" step="1" name="cfg_due_days" value="{due_str}" /></td>
            <td class="checkbox-center">
                <input type="checkbox" name="cfg_is_cash" value="{idx}" {checked_attr} />
            </td>
        </tr>
        """

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تعریف گروه‌های کالا (پیش‌فرض)</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                {nav_html}

                <h1>تعریف گروه‌های کالا (پیش‌فرض)</h1>
                <p>
                    این صفحه مخصوص این است که یک‌بار گروه‌های کالا را با درصد پورسانت، مهلت تسویه و نقدی بودن تعریف کنی.
                    بعداً در صفحهٔ محاسبه پورسانت، این گروه‌ها در منوی کشویی «گروه کالا» استفاده می‌شوند.
                </p>

                <form action="/group-config" method="post">
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>نام گروه کالا</th>
                                <th>درصد پورسانت (%)</th>
                                <th>مهلت تسویه (روز)</th>
                                <th>نقدی؟</th>
                            </tr>
                            <tbody id="group-config-body">
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                    <br/>
                    <button type="button" onclick="addGroupRow()">➕ افزودن سطر جدید</button>
                    &nbsp;
                    <button type="submit">ذخیره پیش‌فرض‌ها در group_config.xlsx</button>
                </form>

                <a class="footer-link" href="/">بازگشت به محاسبه پورسانت</a>
            </div>

            <script>
                function addGroupRow() {{
                    const tbody = document.getElementById('group-config-body');
                    if (!tbody) return;
                    const idx = tbody.querySelectorAll('tr').length;
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td><input type="text" name="cfg_group" value="" placeholder="نام گروه کالا" /></td>
                        <td><input type="number" step="0.01" name="cfg_percent" value="" placeholder="مثلاً 2 برای 2٪" /></td>
                        <td><input type="number" step="1" name="cfg_due_days" value="" placeholder="مثلاً 7، 30، 90" /></td>
                        <td class="checkbox-center">
                            <input type="checkbox" name="cfg_is_cash" value="${{idx}}" />
                        </td>
                    `;
                    tbody.appendChild(row);
                }}
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.post("/group-config", response_class=HTMLResponse)
async def group_config_save(request: Request):
    nav_html = build_nav("config")

    form = await request.form()
    groups = form.getlist("cfg_group")
    percents = form.getlist("cfg_percent")
    due_days_list = form.getlist("cfg_due_days")
    cash_indices = set(form.getlist("cfg_is_cash"))

    rows_data = []
    for idx, (g, p, dd) in enumerate(zip(groups, percents, due_days_list)):
        g_key = str(g).strip()
        if not g_key:
            continue

        # درصد (به صورت انسانی: 2 یعنی 2٪)
        percent_val = 0.0
        p_str = str(p).strip()
        if p_str:
            p_str = p_str.replace(",", ".")
            try:
                percent_val = float(p_str)
            except ValueError:
                percent_val = 0.0

        # مهلت تسویه
        due_val = None
        dd_str = str(dd).strip()
        if dd_str:
            try:
                due_val = int(float(dd_str))
            except ValueError:
                due_val = None

        is_cash = str(idx) in cash_indices

        rows_data.append(
            {
                "Group": g_key,
                "Percent": percent_val,
                "DueDays": due_val,
                "IsCash": is_cash,
            }
        )

    if rows_data:
        df_out = pd.DataFrame(rows_data)
        df_out.to_excel(DEFAULT_GROUP_CONFIG_PATH, index=False)

        message_html = """
        <div class="message message-success">
            تنظیمات گروه‌های کالا با موفقیت در <code>group_config.xlsx</code> ذخیره شد ✅
        </div>
        """
    else:
        message_html = """
        <div class="message message-error">
            هیچ ردیف معتبری برای ذخیره وارد نشده است.
        </div>
        """

    # دوباره فرم را با داده‌های جدید نمایش بده
    current_cfg = load_default_group_config()
    rows = list(current_cfg.items())
    rows_html = ""
    for idx, (gname, cfg) in enumerate(rows):
        percent_human = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)
        due_str = "" if due_days is None else str(due_days)
        checked_attr = "checked" if is_cash else ""

        rows_html += f"""
        <tr>
            <td><input type="text" name="cfg_group" value="{gname}" /></td>
            <td><input type="number" step="0.01" name="cfg_percent" value="{percent_human:.2f}" /></td>
            <td><input type="number" step="1" name="cfg_due_days" value="{due_str}" /></td>
            <td class="checkbox-center">
                <input type="checkbox" name="cfg_is_cash" value="{idx}" {checked_attr} />
            </td>
        </tr>
        """

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تعریف گروه‌های کالا (پیش‌فرض)</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                {nav_html}

                <h1>تعریف گروه‌های کالا (پیش‌فرض)</h1>
                {message_html}

                <form action="/group-config" method="post">
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>نام گروه کالا</th>
                                <th>درصد پورسانت (%)</th>
                                <th>مهلت تسویه (روز)</th>
                                <th>نقدی؟</th>
                            </tr>
                            <tbody id="group-config-body">
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                    <br/>
                    <button type="button" onclick="addGroupRow()">➕ افزودن سطر جدید</button>
                    &nbsp;
                    <button type="submit">ذخیره پیش‌فرض‌ها در group_config.xlsx</button>
                </form>

                <a class="footer-link" href="/">بازگشت به محاسبه پورسانت</a>
            </div>

            <script>
                function addGroupRow() {{
                    const tbody = document.getElementById('group-config-body');
                    if (!tbody) return;
                    const idx = tbody.querySelectorAll('tr').length;
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td><input type="text" name="cfg_group" value="" placeholder="نام گروه کالا" /></td>
                        <td><input type="number" step="0.01" name="cfg_percent" value="" placeholder="مثلاً 2 برای 2٪" /></td>
                        <td><input type="number" step="1" name="cfg_due_days" value="" placeholder="مثلاً 7، 30، 90" /></td>
                        <td class="checkbox-center">
                            <input type="checkbox" name="cfg_is_cash" value="${{idx}}" />
                        </td>
                    `;
                    tbody.appendChild(row);
                }}
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


# ------------------ UI: تب ۳ – تخصیص کالا به گروه ------------------ #

# ------------------ UI: تب ۳ – تخصیص کالا به گروه ------------------ #

@app.get("/group-items", response_class=HTMLResponse)
async def group_items_page():
    nav_html = build_nav("items")

    # تنظیمات گروه‌های پیش‌فرض (برای ساخت منوی کشویی)
    default_group_cfg = load_default_group_config()

    # مپ فعلی کالا → گروه از روی فایل product_group_map.xlsx
    pg_map = load_product_group_map()
    code_to_group: dict[str, str] = {}
    if not pg_map.empty:
        for _, r in pg_map.iterrows():
            code = canonicalize_code(r.get("ProductCode"))
            grp = str(r.get("Group") or "").strip()
            if code and grp:
                code_to_group[code] = grp

    # گزینه‌های منوی کشویی گروه کالا (برای JS و ردیف‌های دستی)
    base_options_html = '<option value="">-- بدون گروه --</option>'
    for gname, cfg in default_group_cfg.items():
        percent = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)
        label_parts = [gname, f"{percent:.2f}٪"]
        if due_days is not None:
            label_parts.append(f"{due_days} روز")
        if is_cash:
            label_parts.append("نقدی")
        label = " | ".join(label_parts)
        base_options_html += f'<option value="{gname}">{label}</option>'

    # برای جاوااسکریپت (بدون خط جدید که داخل بک‌تیک راحت بنشیند)
    product_group_options_js = base_options_html.replace("\n", "")

    df_sales = LAST_UPLOAD["sales"]

    # آماده‌سازی ردیف‌ها
    rows_html = ""
    info_html = ""

    # اگر هنوز فایل فروش آپلود نشده
    if df_sales is None:
        info_html = """
        <p class="message message-error">
            هنوز هیچ فایل فروشی در تب «محاسبه پورسانت» آپلود نشده است.
            با این حال می‌توانی با دکمه «افزودن سطر جدید» در پایین جدول، کالاها را دستی اضافه کنی.
        </p>
        """
    else:
        # سعی می‌کنیم ستون کد و نام کالا را در فروش پیدا کنیم
        code_candidates = ["ProductCode", "کد کالا", "کد محصول", "ProductID"]
        name_candidates = ["ProductName", "نام کالا",
                           "شرح کالا", "شرح", "ProductGroupName"]

        code_col = None
        name_col = None

        for c in code_candidates:
            if c in df_sales.columns:
                code_col = c
                break

        for c in name_candidates:
            if c in df_sales.columns:
                name_col = c
                break

        if code_col is None:
            info_html = """
            <p class="message message-error">
                در فایل فروش، ستونی برای کد کالا پیدا نشد. لطفاً یکی از ستون‌ها را با نام‌هایی مثل
                <code>ProductCode</code>، <code>کد کالا</code> یا <code>کد محصول</code> ایجاد کن.
                همچنین می‌توانی کالاها را با دکمه «افزودن سطر جدید» به‌صورت دستی وارد کنی.
            </p>
            """
        else:
            info_html = f"""
            <p class="message">
                منبع لیست کالاها، آخرین فایل فروش آپلود‌شده است (ستون کد: <b>{code_col}</b>{'، نام: <b>' + name_col + '</b>' if name_col else ''}).<br/>
                اگر می‌خواهی موردی اضافه کنی که در فروش‌ها نیامده، می‌توانی از دکمهٔ «افزودن سطر جدید» استفاده کنی.
            </p>
            """

            df_items = df_sales.copy()
            df_items["__CodeKey__"] = df_items[code_col].map(
                lambda v: canonicalize_code(v) if pd.notna(v) else None
            )
            df_items = df_items[df_items["__CodeKey__"].notna()].copy()

            if name_col:
                df_items["__Name__"] = df_items[name_col].astype(str)
            else:
                df_items["__Name__"] = ""

            df_items = (
                df_items[["__CodeKey__", "__Name__"]]
                .drop_duplicates()
                .sort_values(["__CodeKey__"])
            )

            # برای هر کالای موجود در فروش، یک ردیف با منوی کشویی گروه
            for _, row in df_items.iterrows():
                code_key = str(row["__CodeKey__"])
                name_val = str(row["__Name__"] or "")

                current_group = code_to_group.get(code_key, "")

                # options منوی کشویی برای این کالا (با selected)
                options_html = '<option value="">-- بدون گروه --</option>'
                for gname, cfg in default_group_cfg.items():
                    percent = (cfg.get("percent") or 0) * 100
                    due_days = cfg.get("due_days")
                    is_cash = cfg.get("is_cash", False)
                    label_parts = [gname, f"{percent:.2f}٪"]
                    if due_days is not None:
                        label_parts.append(f"{due_days} روز")
                    if is_cash:
                        label_parts.append("نقدی")
                    label = " | ".join(label_parts)
                    sel_attr = "selected" if gname == current_group else ""
                    options_html += f'<option value="{gname}" {sel_attr}>{label}</option>'

                rows_html += f"""
                <tr>
                    <td>
                        <input type="text" name="prod_code" value="{code_key}" />
                    </td>
                    <td>
                        <input type="text" name="prod_name" value="{name_val}" />
                    </td>
                    <td>
                        <select name="prod_group">
                            {options_html}
                        </select>
                    </td>
                </tr>
                """

    # مپ فعلی کالا → گروه برای نمایش پایین صفحه
    if not pg_map.empty:
        map_html = """
        <div class="table-wrapper">
        """ + pg_map.to_html(index=False, border=0) + "</div>"
    else:
        map_html = "<p>فعلاً مپی برای کالاها ثبت نشده است.</p>"

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تخصیص کالا به گروه</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                {nav_html}

                <h1>تخصیص کالا به گروه</h1>
                <p>
                    در این تب، کد و نام کالاها را (از روی آخرین فایل فروش یا به‌صورت دستی) می‌بینی و برای هر کالا
                    یک «گروه کالا» از لیست پیش‌فرض‌ها انتخاب می‌کنی.
                    این مپ در <code>product_group_map.xlsx</code> ذخیره می‌شود و در محاسبهٔ پورسانت برای
                    پر کردن خودکار گروه کالا استفاده می‌شود.
                </p>

                {info_html}

                <form action="/group-items-save" method="post">
                    <div class="table-wrapper">
                        <table>
                            <thead>
                                <tr>
                                    <th>کد کالا</th>
                                    <th>نام کالا</th>
                                    <th>گروه کالا</th>
                                </tr>
                            </thead>
                            <tbody id="product-group-body">
                                {rows_html}
                            </tbody>
                        </table>
                    </div>
                    <br/>
                    <button type="button" onclick="addProductRow()">➕ افزودن سطر جدید</button>
                    &nbsp;
                    <button type="submit">ذخیره تخصیص‌ها در product_group_map.xlsx</button>
                </form>

                <hr/>

                <h2>مپ فعلی کالا → گروه</h2>
                {map_html}

                <a class="footer-link" href="/">بازگشت به محاسبه پورسانت</a>
            </div>

            <script>
                const PRODUCT_GROUP_OPTIONS = `{product_group_options_js}`;

                function addProductRow() {{
                    const tbody = document.getElementById('product-group-body');
                    if (!tbody) return;
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>
                            <input type="text" name="prod_code" value="" placeholder="کد کالا" />
                        </td>
                        <td>
                            <input type="text" name="prod_name" value="" placeholder="نام کالا (اختیاری)" />
                        </td>
                        <td>
                            <select name="prod_group">
                                ${'{'}PRODUCT_GROUP_OPTIONS{'}'}
                            </select>
                        </td>
                    `;
                    tbody.appendChild(row);
                }}
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.post("/group-items-save", response_class=HTMLResponse)
async def group_items_save(request: Request):
    nav_html = build_nav("items")

    form = await request.form()
    codes = form.getlist("prod_code")
    names = form.getlist("prod_name")
    groups = form.getlist("prod_group")

    new_rows = []
    for code, name, grp in zip(codes, names, groups):
        code_key = canonicalize_code(code)
        if not code_key:
            continue
        grp_name = str(grp).strip()
        if not grp_name:
            # اگر گروه انتخاب نشده، این ردیف را نادیده بگیر
            continue
        name_val = str(name).strip() if name is not None else ""
        new_rows.append(
            {
                "ProductCode": code_key,
                "ProductName": name_val,
                "Group": grp_name,
            }
        )

    df_new = pd.DataFrame(
        new_rows, columns=["ProductCode", "ProductName", "Group"])

    # خواندن مپ قبلی و merge
    df_old = load_product_group_map()
    if df_old.empty:
        df_all = df_new
    else:
        df_old = df_old.copy()
        if not df_new.empty:
            codes_set = set(df_new["ProductCode"])
            df_old = df_old[~df_old["ProductCode"].isin(codes_set)]
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            df_all = df_all.drop_duplicates(
                subset=["ProductCode"], keep="last")
        else:
            df_all = df_old

    if not df_all.empty:
        save_product_group_map(df_all)
        msg_html = """
        <div class="message message-success">
            تخصیص کالاها به گروه‌ها با موفقیت در <code>product_group_map.xlsx</code> ذخیره شد ✅
        </div>
        """
    else:
        msg_html = """
        <div class="message message-error">
            هیچ تخصیص معتبری برای ذخیره ثبت نشد.
        </div>
        """

    # برای نمایش، دوباره مپ را بخوانیم
    pg_map = load_product_group_map()
    if not pg_map.empty:
        map_html = """
        <div class="table-wrapper">
        """ + pg_map.to_html(index=False, border=0) + "</div>"
    else:
        map_html = "<p>فعلاً مپی برای کالاها ثبت نشده است.</p>"

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تخصیص کالا به گروه</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                {nav_html}

                <h1>تخصیص کالا به گروه</h1>
                {msg_html}

                <h2>مپ فعلی کالا → گروه</h2>
                {map_html}

                <a class="footer-link" href="/group-items">بازگشت به صفحهٔ تخصیص کالا</a>
                <br/>
                <a class="footer-link" href="/">بازگشت به محاسبه پورسانت</a>
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html)
