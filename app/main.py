from app.services.sales_excel_loader import load_sales_excel
from app.services.payments_excel_loader import load_payments_excel

from datetime import datetime
import jdatetime
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse
import pandas as pd
import re


def parse_jalali_or_gregorian(value):
    """
    ورودی: تاریخ به صورت شمسی مثل 1404/08/01 یا 1404-08-01 یا حتی datetime میلادی.
    خروجی: pandas.Timestamp میلادی یا NaT
    """
    if pd.isna(value):
        return pd.NaT

    # اگر از قبل datetime یا Timestamp است، همان را برگردان
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value)

    s = str(value).strip()
    if not s:
        return pd.NaT

    # پیدا کردن الگوی yyyy/mm/dd یا yyyy-mm-dd
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))

        # اگر سال >= 1300 است، فرض می‌کنیم شمسی است
        if year >= 1300:
            try:
                jd = jdatetime.date(year, month, day)
                g = jd.togregorian()  # datetime.date میلادی
                return pd.Timestamp(g.year, g.month, g.day)
            except Exception:
                return pd.NaT
        else:
            # احتمالاً میلادی است
            return pd.to_datetime(s, errors="coerce")

    # اگر فرمت چیز دیگری بود، به pandas بسپاریم (میلادی)
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


def normalize_name(name):
    """
    نرمال‌سازی نام فارسی برای مقایسه:
    - حذف فاصله‌های اضافه
    - یکسان‌سازی ي/ی و ك/ک
    - حروف کوچک
    """
    if pd.isna(name):
        return ""
    s = str(name).strip()
    if not s:
        return ""
    # یکسان‌سازی حروف
    s = s.replace("ي", "ی").replace("ك", "ک")
    # کوچک کردن
    s = s.lower()
    # جمع کردن فاصله‌های اضافی
    s = re.sub(r"\s+", " ", s)
    return s


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
    # حذف ویرگول‌های جداکننده هزارگان
    s_no_comma = s.replace(",", "")
    try:
        f = float(s_no_comma)
        # اگر به عدد صحیح می‌خورد، همان را برگردان
        if f.is_integer():
            return str(int(f))
    except Exception:
        # اگر اصلاً عدد نبود (مثل 13/01) همان رشته را برگردان
        return s
    return s


app = FastAPI()

# ذخیره آخرین اکسل‌های آپلود شده در حافظه (برای همین کاربر)
LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    # نام ستونی که برای گروه کالا استفاده می‌کنیم (ProductCode یا ProductGroup)
    "group_col": None,
    # تنظیمات گروه‌ها (پورسانت + مهلت + نقدی بودن)
    "group_config": None,
}

BASE_CSS = """
<style>
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Tahoma, sans-serif;
    direction: rtl;
    background: linear-gradient(135deg, #eff6ff, #f9fafb);
    margin: 0;
}
.container {
    max-width: 1100px;
    margin: 32px auto;
    background: #ffffff;
    padding: 24px 32px;
    border-radius: 16px;
    box-shadow: 0 18px 40px rgba(15, 23, 42, 0.16);
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
}
button:hover {
    background: linear-gradient(135deg, #1d4ed8, #1e40af);
}
label {
    font-weight: 600;
    font-size: 13px;
}
input[type="file"],
input[type="number"] {
    width: 100%;
    padding: 6px 8px;
    border-radius: 8px;
    border: 1px solid #d1d5db;
    font-size: 13px;
    box-sizing: border-box;
    transition: border-color 0.15s, box-shadow 0.15s;
}
input[type="file"]:focus,
input[type="number"]:focus {
    outline: none;
    border-color: #2563eb;
    box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.18);
}
.form-row {
    margin-bottom: 14px;
}
small {
    font-size: 11px;
    color: #6b7280;
}
.summary-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin: 16px 0;
}
.summary-card {
    flex: 1 1 160px;
    background: #f9fafb;
    border-radius: 12px;
    padding: 10px 14px;
    border: 1px solid #e5e7eb;
    position: relative;
    overflow: hidden;
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
.summary-commission::before {
    background: linear-gradient(180deg, #7c3aed, #a855f7);
}
.summary-card .label {
    font-size: 11px;
    color: #6b7280;
}
.summary-card .value {
    font-weight: 600;
    margin-top: 4px;
    font-size: 13px;
    color: #111827;
}
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
.badge-pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 11px;
    background: #eef2ff;
    color: #3730a3;
}
.checkbox-center {
    text-align: center;
}
</style>
"""

# ------------------ توابع کمکی ------------------ #


def get_priority(product_group: str) -> str:
    """
    حالت پشتیبان: اگر هیچ تنظیمی برای گروه نداشتیم،
    از روی نام گروه، نقدی/عادی را حدس می‌زنیم.
    """
    text = str(product_group)
    if "نقدی" in text:
        return "cash"
    return "normal"


def build_sales_name_map(sales_df: pd.DataFrame) -> dict:
    """
    ساخت نگاشت نام مشتری نرمال‌شده -> CustomerKey
    برای استفاده در وصل کردن پرداخت‌ها.
    """
    name_map: dict = {}
    if "CustomerName" in sales_df.columns and "CustomerKey" in sales_df.columns:
        tmp = sales_df[["CustomerName", "CustomerKey"]
                       ].dropna().drop_duplicates()
        for _, row in tmp.iterrows():
            nm = normalize_name(row["CustomerName"])
            key = row["CustomerKey"]
            if nm and pd.notna(key):
                name_map[nm] = key
    return name_map


def extract_customer_for_payment(row, checks_df: pd.DataFrame, name_map: dict):
    """
    تشخیص کد مشتری برای هر پرداخت:
    1) اول از روی CustomerCode (اگر باشد)
    2) بعد از روی شماره چک در توضیحات (اگر در فایل چک موجود باشد)
    3) در نهایت از روی نام مشتری (واریز یا برداشت کننده) با تطبیق متنی
    """
    # 1) کد طرف حساب اگر هست
    code = None
    for col in ["CustomerCode", "PartyCode", "AccountCode"]:
        if col in row.index:
            code = canonicalize_code(row.get(col))
            if code:
                return code

    # 2) جستجوی شماره چک در توضیحات
    desc = str(row.get("Description") or "")
    m = re.search(r"(CHK-\d+)", desc)
    if m is not None and not checks_df.empty and "CheckNumber" in checks_df.columns:
        check_number = m.group(1)
        match = checks_df.loc[checks_df["CheckNumber"] == check_number]
        if not match.empty:
            chk_code = canonicalize_code(match.iloc[0].get("CustomerCode"))
            if chk_code:
                return chk_code

    # 3) تطبیق بر اساس نام واریز / برداشت کننده
    name_val = None
    # 👈 اینجا ستون‌های محتمل برای نام مشتری رو چک می‌کنیم
    for col in [
        "CustomerName",
        "PayerName",
        "واريز يا برداشت كننده",
        "واریز یا برداشت کننده",
    ]:
        if col in row.index and pd.notna(row.get(col)):
            name_val = row.get(col)
            break

    if name_val is not None:
        nm = normalize_name(name_val)
        if nm in name_map:
            return name_map[nm]

    return None


def prepare_payments(
    payments_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    sales_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم پرداخت‌ها و وصل کردن هر پرداخت به یک مشتری.
    - تبدیل تاریخ پرداخت (شمسی/میلادی) به Timestamp
    - پیدا کردن ستون مبلغ (حتی اگر اسمش Amount نباشد)
    - تشخیص مشتری هر پرداخت (بر اساس کد، چک، یا نام مشتری)
    """
    payments_df = payments_df.copy()

    # ---- تاریخ پرداخت ----
    if "PaymentDate" in payments_df.columns:
        payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
            parse_jalali_or_gregorian
        )

    # ---- پیدا کردن ستون مبلغ ----
    amount_col = None

    # 1) اگر خود ستون Amount هست، همونو می‌گیریم
    if "Amount" in payments_df.columns:
        amount_col = "Amount"
    else:
        # 2) سعی می‌کنیم از روی اسم ستون‌ها حدس بزنیم
        def norm_col(c: str) -> str:
            s = str(c).strip()
            s = s.replace("ي", "ی").replace("ك", "ک")
            s = re.sub(r"\s+", " ", s)
            return s.lower()

        for col in payments_df.columns:
            nc = norm_col(col)
            # هر ستونی که توی اسمش «مبلغ» یا چیزهای شبیه به این باشد
            if (
                "مبلغ" in nc
                or "بدهي" in nc
                or "بدهکار" in nc
                or "بستانکار" in nc
                or "پرداخت" in nc
                or "واریز" in nc
            ):
                amount_col = col
                break

    if amount_col is None:
        # اگر باز هم تشخیص ندادیم، به‌جای خطای مبهم، روشن توضیح بده
        cols_str = ", ".join(str(c) for c in payments_df.columns)
        raise ValueError(
            "در فایل پرداخت‌ها نتوانستم ستون مبلغ را پیدا کنم.\n"
            "یا ستونی به نام 'Amount' بساز، یا نام یکی از ستون‌ها را شامل واژه‌هایی مثل «مبلغ»، "
            "«بدهکار»، «بستانکار»، «پرداخت»، «واریز» بگذار.\n"
            f"ستون‌های فعلی فایل پرداخت‌ها:\n{cols_str}"
        )

    # تبدیل ستون تشخیص‌داده شده به Amount
    payments_df["Amount"] = pd.to_numeric(
        payments_df[amount_col], errors="coerce"
    ).fillna(0)

    # ---- ساخت map نام مشتری از روی فروش‌ها (اگر قبلاً نوشتی همون رو نگه دار) ----
    # اگر قبلاً build_sales_name_map را تعریف کرده‌ای، از همان استفاده کن.
    # اگر نداری، یک نسخه ساده:
    def normalize_name(value):
        s = str(value)
        s = s.replace("ي", "ی").replace("ك", "ک")
        s = re.sub(r"\s+", " ", s)
        return s.strip().lower()

    def canonicalize_code(v):
        if pd.isna(v):
            return None
        s = str(v).strip()
        if not s:
            return None
        # حذف فاصله و کاراکترهای غیرعددی
        cleaned = re.sub(r"\s+", "", s)
        return cleaned

    def build_sales_name_map(sales_df_inner: pd.DataFrame) -> dict:
        m = {}
        if "CustomerCode" not in sales_df_inner.columns:
            return m
        for _, r in sales_df_inner.iterrows():
            code = canonicalize_code(r.get("CustomerCode"))
            if not code:
                continue

            name_val = None
            for col in ["CustomerName", "نام مشتری", "نام مشتري"]:
                if col in sales_df_inner.columns and pd.notna(r.get(col)):
                    name_val = r.get(col)
                    break

            if name_val:
                nm = normalize_name(name_val)
                if nm and nm not in m:
                    m[nm] = code
        return m

    name_map = build_sales_name_map(sales_df)

    # ---- تشخیص مشتری هر پرداخت ----
    def extract_customer_for_payment(row):
        """
        1) اگر کد مشتری / حساب در خود ردیف پرداخت باشد، همان را می‌گیریم
        2) اگر نوع پرداخت از نوع Check باشد و در توضیحات شماره چک باشد، از فایل چک کد مشتری را می‌گیریم
        3) اگر نام «واريز يا برداشت كننده» / CustomerName وجود داشته باشد، با map نام‌ها match می‌کنیم
        """
        # 1) کد مشتری / حساب اگر موجود است
        for col in ["CustomerCode", "PartyCode", "AccountCode", "کد حساب"]:
            if col in row.index and pd.notna(row.get(col)):
                return canonicalize_code(row.get(col))

        # 2) بررسی شماره چک در توضیحات
        desc = str(row.get("Description") or "")
        m_chk = re.search(r"(CHK-\d+)", desc)
        if (
            m_chk is not None
            and not checks_df.empty
            and "CheckNumber" in checks_df.columns
        ):
            check_number = m_chk.group(1)
            match = checks_df.loc[checks_df["CheckNumber"] == check_number]
            if not match.empty:
                chk_code = canonicalize_code(match.iloc[0].get("CustomerCode"))
                if chk_code:
                    return chk_code

        # 3) match نام مشتری (واريز يا برداشت كننده)
        name_val = None
        for col in [
            "CustomerName",
            "PayerName",
            "واريز يا برداشت كننده",
            "واریز یا برداشت کننده",
        ]:
            if col in row.index and pd.notna(row.get(col)):
                name_val = row.get(col)
                break

        if name_val is not None:
            nm = normalize_name(name_val)
            if nm in name_map:
                return name_map[nm]

        return None

    payments_df["ResolvedCustomer"] = payments_df.apply(
        extract_customer_for_payment, axis=1
    )

    # فقط پرداخت‌هایی که مشتری‌شان مشخص شده است
    payments_df = payments_df[payments_df["ResolvedCustomer"].notna()]

    return payments_df


def prepare_sales(sales_df: pd.DataFrame, group_config: dict, group_col: str) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم فروش‌ها:
    - تبدیل تاریخ‌ها
    - تعیین CustomerKey استاندارد
    - تعیین DueDate بر اساس:
        * اگر در اکسل ستونی به نام DueDate باشد، همان (شمسی) مبناست
        * در غیر این صورت، از روی مهلت تسویه گروه (group_config) محاسبه می‌شود
    - تعیین نقدی/عادی بر اساس تنظیمات گروه (تیک نقدی) و در صورت نبود تنظیم، fallback
    - تعیین درصد پورسانت از روی group_config
    """
    sales_df = sales_df.copy()

    # تاریخ فاکتور
    if "InvoiceDate" not in sales_df.columns:
        raise ValueError("در فایل فروش ستونی به نام 'InvoiceDate' پیدا نشد.")
    sales_df["InvoiceDate"] = sales_df["InvoiceDate"].apply(
        parse_jalali_or_gregorian)

    # CustomerKey استاندارد برای وصل کردن به پرداخت‌ها
    if "CustomerCode" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerCode"].map(
            canonicalize_code)
    elif "CustomerName" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerName"].map(normalize_name)
    else:
        # اگر هیچ‌کدام نباشد، عملاً نمی‌توانیم وصل کنیم؛ ولی اجازه ادامه می‌دهیم
        sales_df["CustomerKey"] = None

    # اگر ستون DueDate داریم (شمسی)، تبدیلش کنیم؛ اگر نداریم، فعلاً NaT
    if "DueDate" in sales_df.columns:
        sales_df["DueDate"] = sales_df["DueDate"].apply(
            parse_jalali_or_gregorian)
    else:
        sales_df["DueDate"] = pd.NaT

    # محاسبه DueDate نهایی
    def compute_due_date(row):
        invoice_date = row["InvoiceDate"]
        if pd.isna(invoice_date):
            return pd.NaT

        # اگر در اکسل تاریخ سررسید داده شده، همان را مبنا بگیریم
        if not pd.isna(row["DueDate"]):
            return row["DueDate"]

        # در غیر این صورت، از تنظیمات گروه مهلت را برداریم
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        due_days = None
        if cfg is not None:
            due_days = cfg.get("due_days")

        # اگر در تنظیمات گروه مهلت مشخص نشده بود، fallback
        if not due_days or due_days <= 0:
            # اگر نام گروه شامل "نقدی" باشد، 7 روز؛ در غیر این صورت 90
            base_priority = get_priority(row.get(group_col, ""))
            due_days = 7 if base_priority == "cash" else 90

        return invoice_date + pd.to_timedelta(due_days, unit="D")

    sales_df["DueDate"] = sales_df.apply(compute_due_date, axis=1)

    # تعیین Priority از روی تنظیمات گروه (تیک نقدی)
    def compute_priority(row):
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        if cfg is not None:
            return "cash" if cfg.get("is_cash") else "normal"

        # fallback: اگر فاصله تاریخ فاکتور تا سررسید ≤ 7 روز باشد، نقدی
        try:
            delta_days = (row["DueDate"] - row["InvoiceDate"]).days
            if delta_days <= 7:
                return "cash"
        except Exception:
            pass

        return get_priority(row.get(group_col, ""))

    sales_df["Priority"] = sales_df.apply(compute_priority, axis=1)
    sales_df["PriorityRank"] = sales_df["Priority"].map(
        {"cash": 0, "normal": 1}).fillna(1).astype(int)

    # درصد پورسانت از روی تنظیمات گروه
    def row_percent(row):
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        if cfg is None:
            return 0.0
        return float(cfg.get("percent", 0.0))

    sales_df["CommissionPercent"] = sales_df.apply(row_percent, axis=1)

    # فیلدهای پولی و کمکی
    if "Amount" not in sales_df.columns:
        raise ValueError("در فایل فروش ستونی به نام 'Amount' پیدا نشد.")
    sales_df["Amount"] = sales_df["Amount"].astype(float)
    sales_df["PaidAmount"] = 0.0
    sales_df["Remaining"] = sales_df["Amount"]
    sales_df["CommissionAmount"] = 0.0

    return sales_df


def compute_commissions(sales_raw, payments_raw, checks_raw, group_config, group_col):
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

    # اگر پرداختی نداریم، فقط جدول پورسانت صفر برگردان
    if payments_df.empty:
        salesperson_df = (
            sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
            .sum()
            .reset_index()
        )
        salesperson_df.rename(
            columns={"CommissionAmount": "TotalCommission"}, inplace=True
        )
        return sales_df, salesperson_df, pd.DataFrame()

    # تسویه پرداخت‌ها به تفکیک CustomerKey
    for cust_key, pay_group in payments_df.groupby("ResolvedCustomer"):
        # فاکتورهای این مشتری
        cust_invoice_idx = sales_df.index[sales_df["CustomerKey"] == cust_key]
        if len(cust_invoice_idx) == 0:
            continue

        # مرتب‌سازی: اول نقدی، بعد عادی، بعد از قدیمی به جدید
        cust_invoice_idx = (
            sales_df.loc[cust_invoice_idx]
            .sort_values(["PriorityRank", "InvoiceDate"])
            .index
        )

        # پرداخت‌ها به ترتیب تاریخ
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

                # اگر پرداخت در مهلت مجاز این فاکتور بوده، پورسانت تعلق می‌گیرد
                if in_due:
                    percent = sales_df.at[idx, "CommissionPercent"]
                    sales_df.at[idx, "CommissionAmount"] += allocate * percent

                sales_df.at[idx, "PaidAmount"] += allocate
                sales_df.at[idx, "Remaining"] -= allocate
                remaining_payment -= allocate

    # جمع پورسانت به تفکیک فروشنده
    salesperson_df = (
        sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
        .sum()
        .reset_index()
    )
    salesperson_df.rename(
        columns={"CommissionAmount": "TotalCommission"}, inplace=True
    )

    # alloc_df فعلاً خالی (فعلاً نمودار نداریم)
    return sales_df, salesperson_df, pd.DataFrame()


# ------------------ UI مرحله ۱: آپلود اکسل‌ها ------------------ #

@app.get("/", response_class=HTMLResponse)
async def index():
    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>محاسبه پورسانت فروش</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                <h1>محاسبه پورسانت فروش</h1>
                <p>مرحله ۱ از ۲ – لطفاً فایل‌های اکسل فروش، پرداخت‌ها و در صورت وجود چک‌ها را انتخاب کن.</p>

                <div class="summary-grid">
                    <div class="summary-card summary-sales">
                        <div class="label">فایل فروش‌ها</div>
                        <div class="value">ستون‌های پیشنهادی:</div>
                        <div class="value" style="font-weight:400; font-size:12px;">
                            <span class="badge-pill">InvoiceID</span>
                            <span class="badge-pill">InvoiceDate</span>
                            <span class="badge-pill">DueDate</span>
                            <span class="badge-pill">CustomerCode</span>
                            <span class="badge-pill">CustomerName</span>
                            <span class="badge-pill">ProductGroup / ProductCode</span>
                            <span class="badge-pill">Amount</span>
                            <span class="badge-pill">Salesperson</span>
                        </div>
                    </div>
                    <div class="summary-card summary-payments">
                        <div class="label">فایل پرداخت‌ها</div>
                        <div class="value">ستون‌های پیشنهادی (پس از تبدیل):</div>
                        <div class="value" style="font-weight:400; font-size:12px;">
                            <span class="badge-pill">PaymentID</span>
                            <span class="badge-pill">PaymentDate</span>
                            <span class="badge-pill">Amount</span>
                            <span class="badge-pill">CustomerCode</span>
                            <span class="badge-pill">CustomerName</span>
                            <span class="badge-pill">Description</span>
                        </div>
                    </div>
                    <div class="summary-card summary-checks">
                        <div class="label">فایل چک‌ها (اختیاری)</div>
                        <div class="value">ستون‌های پیشنهادی:</div>
                        <div class="value" style="font-weight:400; font-size:12px;">
                            <span class="badge-pill">CheckNumber</span>
                            <span class="badge-pill">CustomerCode</span>
                            <span class="badge-pill">Amount</span>
                            <span class="badge-pill">BankName</span>
                            <span class="badge-pill">Description</span>
                        </div>
                    </div>
                </div>

                <form action="/upload-all" method="post" enctype="multipart/form-data">
                    <div class="form-row">
                        <label>فایل اکسل فروش‌ها</label><br/>
                        <input type="file" name="sales_file" accept=".xlsx,.xls" required />
                        <small>این فایل مبنای محاسبه پورسانت است.</small>
                    </div>

                    <div class="form-row">
                        <label>فایل اکسل پرداخت‌ها</label><br/>
                        <input type="file" name="payments_file" accept=".xlsx,.xls" required />
                        <small>پرداخت‌های نقدی و وصول چک‌ها در این فایل است.</small>
                    </div>

                    <div class="form-row">
                        <label>فایل اکسل چک‌ها (اختیاری)</label><br/>
                        <input type="file" name="checks_file" accept=".xlsx,.xls" />
                        <small>برای اتصال پرداخت‌های حاوی شماره چک به مشتری استفاده می‌شود.</small>
                    </div>

                    <button type="submit">مرحله بعد: تعریف تنظیمات گروه‌ها</button>
                </form>
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.post("/upload-all", response_class=HTMLResponse)
async def upload_all(
    sales_file: UploadFile = File(...),
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None),
):
    # ✅ فروش‌ها با لودر اختصاصی
    df_sales = load_sales_excel(sales_file.file)

    # ✅ پرداخت‌ها با لودر اختصاصی
    df_pay = load_payments_excel(payments_file.file)

    # چک‌ها (در صورت انتخاب)
    if checks_file is not None and checks_file.filename:
        df_chk = pd.read_excel(checks_file.file)
    else:
        df_chk = pd.DataFrame()

    # تشخیص ستون گروه کالا: ترجیحاً ProductCode، در غیر اینصورت ProductGroup
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

    # ذخیره در حافظه برای مرحله بعد
    LAST_UPLOAD["sales"] = df_sales
    LAST_UPLOAD["payments"] = df_pay
    LAST_UPLOAD["checks"] = df_chk
    LAST_UPLOAD["group_col"] = group_col

    # ساخت فرم تعریف تنظیمات برای هر گروه
    rows_html = ""
    for g in groups:
        g_str = str(g)
        rows_html += f"""
        <tr>
            <td>{g_str}</td>
            <td>
                <input type="hidden" name="group_name" value="{g_str}" />
                <input type="number" step="0.01" name="group_percent" placeholder="مثلاً 2 برای 2٪" />
            </td>
            <td>
                <input type="number" step="1" name="group_due_days" placeholder="مثلاً 7، 30، 90" />
            </td>
            <td class="checkbox-center">
                <input type="checkbox" name="cash_group" value="{g_str}" />
            </td>
        </tr>
        """

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تعریف تنظیمات گروه‌های کالایی</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                <h1>تعریف تنظیمات پورسانت و مهلت تسویه برای گروه‌های کالایی</h1>
                <p>مرحله ۲ از ۲ – برای هر گروه (بر اساس ستون <b>{group_col}</b>) موارد زیر را پر کن:</p>
                <ul style="font-size:12px; color:#4b5563;">
                    <li>درصد پورسانت (مثلاً 2 یعنی 2٪)</li>
                    <li>مهلت تسویه (بر حسب روز از تاریخ فاکتور)</li>
                    <li>تیک «اولویت نقدی» اگر می‌خواهی فاکتورهای این گروه زودتر از بقیه تسویه شوند.</li>
                </ul>

                <form action="/calculate-commission" method="post">
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>گروه کالا</th>
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
        </body>
    </html>
    """
    return HTMLResponse(content=html)


# ------------------ UI مرحله ۲: گرفتن تنظیمات و محاسبه ------------------ #

@app.post("/calculate-commission", response_class=HTMLResponse)
async def calculate_commission(request: Request):
    # چک کنیم که قبلاً اکسل‌ها آپلود شده باشند
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
    percents = form.getlist("group_percent")
    due_days_list = form.getlist("group_due_days")
    cash_groups = set(form.getlist("cash_group"))

    # ساخت دیکشنری تنظیمات گروه → {percent, due_days, is_cash}
    group_config: dict = {}
    for name, p, dd in zip(group_names, percents, due_days_list):
        key = str(name).strip()
        if not key:
            continue

        # درصد پورسانت
        percent_val = 0.0
        p_str = str(p).strip()
        if p_str:
            p_str = p_str.replace(",", ".")
            try:
                percent_val = float(p_str) / 100.0  # تبدیل به ضریب
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

    # محاسبه پورسانت و وضعیت فاکتورها
    sales_result, salesperson_result, _ = compute_commissions(
        df_sales, df_pay, df_chk, group_config, group_col
    )

    # خلاصه ساده
    sales_rows = len(df_sales)
    sales_sum = df_sales["Amount"].sum() if "Amount" in df_sales.columns else 0

    pay_rows = len(df_pay)
    pay_sum = df_pay["Amount"].sum() if "Amount" in df_pay.columns else 0

    chk_rows = len(df_chk) if df_chk is not None and not df_chk.empty else 0
    chk_sum = df_chk["Amount"].sum(
    ) if chk_rows > 0 and "Amount" in df_chk.columns else 0

    total_commission = 0
    if "TotalCommission" in salesperson_result.columns:
        total_commission = float(
            salesperson_result["TotalCommission"].sum() or 0)

    # آماده‌سازی جدول فاکتورها برای نمایش
    invoices_view = sales_result.copy()

    # تبدیل تاریخ‌ها به شمسی برای نمایش
    for dt_col in ["InvoiceDate", "DueDate"]:
        if dt_col in invoices_view.columns:
            invoices_view[dt_col] = invoices_view[dt_col].map(to_jalali_str)

    # درصد را به درصد انسانی تبدیل کنیم (۱ یعنی ۱٪)
    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = (
            invoices_view["CommissionPercent"] * 100).round(2)

    # بج رنگی برای نوع فروش
    if "Priority" in invoices_view.columns:
        def pri_badge(v):
            if v == "cash":
                return '<span class="badge badge-priority-cash">نقدی</span>'
            elif v == "normal":
                return '<span class="badge badge-priority-normal">عادی</span>'
            return ""
        invoices_view["Priority"] = invoices_view["Priority"].map(pri_badge)

    # قالب‌بندی ستون درصد پورسانت
    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = invoices_view["CommissionPercent"].map(
            lambda x: f"{x:.2f}٪"
        )

    for col in ["Amount", "PaidAmount", "Remaining", "CommissionAmount"]:
        if col in invoices_view.columns:
            invoices_view[col] = invoices_view[col].round(0).astype("int64")

    # تلاش می‌کنیم ستون‌های مهم را نشان دهیم، اگر وجود داشته باشند
    cols = []
    for c in ["InvoiceID", "CustomerCode", "CustomerName", group_col,
              "Priority", "InvoiceDate", "DueDate",
              "Amount", "PaidAmount", "Remaining",
              "CommissionPercent", "CommissionAmount"]:
        if c in invoices_view.columns:
            cols.append(c)

    invoices_table_html = ""
    if cols:
        invoices_table_html = invoices_view[cols].to_html(
            index=False, border=0, escape=False)

    # جدول پورسانت به تفکیک فروشنده
    if "TotalCommission" in salesperson_result.columns:
        salesperson_result["TotalCommission"] = (
            salesperson_result["TotalCommission"].round(0).astype("int64")
        )
    salesperson_table_html = salesperson_result.to_html(index=False, border=0)

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>نتیجه محاسبه پورسانت</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
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

                <hr/>

                <h2>پورسانت نهایی به تفکیک فروشنده</h2>
                <div class="table-wrapper">
                    {salesperson_table_html}
                </div>

                <a class="footer-link" href="/">شروع دوباره (آپلود فایل‌های جدید)</a>
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html)
