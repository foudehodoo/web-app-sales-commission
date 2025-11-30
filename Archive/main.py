from app.services.payments_excel_loader import load_payments_excel
import numpy as np
from datetime import datetime
import jdatetime
from app.services.sales_excel_loader import load_sales_excel
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


app = FastAPI()

# ذخیره آخرین اکسل‌های آپلود شده در حافظه (برای همین کاربر)
LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    # نام ستونی که برای گروه کالا استفاده می‌کنیم (ProductCode یا ProductGroup)
    "group_col": None,
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
</style>
"""

# ------------------ توابع کمکی ------------------ #


def get_priority(product_group: str) -> str:
    """حالت پشتیبان: اگر DueDate نداشتیم، از روی نام گروه، نقدی/عادی را حدس می‌زنیم."""
    text = str(product_group)
    if "نقدی" in text:
        return "cash"
    return "normal"


def extract_customer_for_payment(row, checks_df: pd.DataFrame):
    """تشخیص کد مشتری برای هر پرداخت، از روی CustomerCode یا شماره چک."""
    stype = row.get("SourceType")

    # 1) واریز مستقیم از حساب مشتری
    if stype == "CustomerAccount":
        code = row.get("CustomerCode")
        if pd.isna(code) or str(code).strip() == "":
            return None
        return str(code)

    # 2) پرداخت از محل وصول چک
    if stype == "Check":
        desc = str(row.get("Description") or "")
        # مثال: "وصول چک CHK-6001"
        m = re.search(r"(CHK-\d+)", desc)
        if not m:
            return None
        check_number = m.group(1)

        if "CheckNumber" not in checks_df.columns:
            return None

        match = checks_df.loc[checks_df["CheckNumber"] == check_number]
        if not match.empty:
            return str(match.iloc[0]["CustomerCode"])
        return None

    return None


def prepare_payments(payments_df: pd.DataFrame, checks_df: pd.DataFrame) -> pd.DataFrame:
    """آماده‌سازی دیتافریم پرداخت‌ها و وصل کردن هر پرداخت به یک مشتری."""
    payments_df = payments_df.copy()
    payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
        parse_jalali_or_gregorian)
    payments_df["Amount"] = payments_df["Amount"].astype(float)

    payments_df["ResolvedCustomer"] = payments_df.apply(
        lambda row: extract_customer_for_payment(row, checks_df), axis=1
    )

    # فقط پرداخت‌هایی که مشتری‌شان مشخص شده است
    payments_df = payments_df[payments_df["ResolvedCustomer"].notna()]
    return payments_df


def prepare_sales(sales_df: pd.DataFrame, commission_map: dict, group_col: str) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم فروش‌ها:
    - تعیین نقدی/عادی بر اساس InvoiceDate و DueDate (اگر موجود باشد)
    - تعیین درصد پورسانت از روی commission_map که مدیر سیستم داده
    """
    sales_df = sales_df.copy()
    sales_df["InvoiceDate"] = sales_df["InvoiceDate"].apply(
        parse_jalali_or_gregorian)

    # --- تعیین سررسید و اولویت نقدی/عادی --- #
    if "DueDate" in sales_df.columns:
        sales_df["DueDate"] = sales_df["DueDate"].apply(
            parse_jalali_or_gregorian)
        delta_days = (sales_df["DueDate"] - sales_df["InvoiceDate"]).dt.days

        sales_df["Priority"] = delta_days.apply(
            lambda d: "cash" if d <= 7 else "normal"
        )
    else:
        # اگر ستونی که به عنوان گروه کالا انتخاب شده وجود دارد، از همان برای تشخیص نقدی/عادی استفاده کن
        if group_col in sales_df.columns:
            sales_df["Priority"] = sales_df[group_col].apply(get_priority)
        else:
            # اگر هیچ ستونی برای گروه نداریم، همه را عادی فرض می‌کنیم
            sales_df["Priority"] = "normal"

        sales_df["DueDays"] = sales_df["Priority"].map(
            {"cash": 7, "normal": 90})
        sales_df["DueDate"] = sales_df["InvoiceDate"] + pd.to_timedelta(
            sales_df["DueDays"], unit="D"
        )

    # برای مرتب‌سازی: اول نقدی (0)، بعد عادی (1)
    sales_df["PriorityRank"] = sales_df["Priority"].map(
        {"cash": 0, "normal": 1})

    # --- تعیین درصد پورسانت از روی گروه کالا --- #
    def row_percent(row):
        key = row.get(group_col)
        return float(commission_map.get(key, 0.0))

    sales_df["CommissionPercent"] = sales_df.apply(row_percent, axis=1)

    # فیلدهای پولی و کمکی
    sales_df["Amount"] = sales_df["Amount"].astype(float)
    sales_df["PaidAmount"] = 0.0
    sales_df["Remaining"] = sales_df["Amount"]
    sales_df["CommissionAmount"] = 0.0

    return sales_df


def compute_commissions(sales_raw, payments_raw, checks_raw, commission_map, group_col):
    """
    هسته‌ی محاسبات:
    - آماده‌سازی فروش‌ها و پرداخت‌ها
    - تسویه فاکتورها طبق اولویت (نقدی → عادی، قدیمی → جدید)
    - محاسبه پورسانت
    """
    sales_df = prepare_sales(sales_raw, commission_map, group_col)

    checks_df = (
        checks_raw.copy()
        if checks_raw is not None and not checks_raw.empty
        else pd.DataFrame()
    )
    payments_df = prepare_payments(payments_raw, checks_df)

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

    # تسویه پرداخت‌ها به تفکیک مشتری
    for cust, pay_group in payments_df.groupby("ResolvedCustomer"):
        # فاکتورهای این مشتری
        cust_invoice_idx = sales_df.index[sales_df["CustomerCode"] == cust]
        if len(cust_invoice_idx) == 0:
            continue

        # مرتب‌سازی: اول نقدی، بعد عادی، بعد از قدیمی به جدید
        cust_invoice_idx = (
            sales_df.loc[cust_invoice_idx]
            .sort_values(["PriorityRank", "InvoiceDate"])
            .index
        )

        # پرداخت‌ها به ترتیب تاریخ
        pay_group = pay_group.sort_values("PaymentDate")

        for _, p in pay_group.iterrows():
            remaining_payment = p["Amount"]
            pay_date = p["PaymentDate"]

            for idx in cust_invoice_idx:
                if remaining_payment <= 0:
                    break

                remaining_invoice = sales_df.at[idx, "Remaining"]
                if remaining_invoice <= 0:
                    continue

                allocate = min(remaining_payment, remaining_invoice)

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
                            <span class="badge-pill">ProductGroup</span>
                            <span class="badge-pill">Amount</span>
                            <span class="badge-pill">Salesperson</span>
                        </div>
                    </div>
                    <div class="summary-card summary-payments">
                        <div class="label">فایل پرداخت‌ها</div>
                        <div class="value">ستون‌های پیشنهادی:</div>
                        <div class="value" style="font-weight:400; font-size:12px;">
                            <span class="badge-pill">PaymentID</span>
                            <span class="badge-pill">PaymentDate</span>
                            <span class="badge-pill">Amount</span>
                            <span class="badge-pill">SourceType</span>
                            <span class="badge-pill">CustomerCode</span>
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
                        <small>برای اتصال پرداخت‌های نوع «Check» به مشتری استفاده می‌شود.</small>
                    </div>

                    <button type="submit">مرحله بعد: تعریف درصد پورسانت</button>
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

    # ✅ پرداخت‌ها با لودر اختصاصی جدید
    df_pay = load_payments_excel(payments_file.file)

    # چک‌ها فعلاً ساده:
    # چک‌ها (اختیاری و مقاوم)
    if checks_file is not None and getattr(checks_file, "filename", None):
        try:
            df_chk = pd.read_excel(checks_file.file)
        except Exception:
            # اگر فرمت درست نبود یا فایل خراب بود، نادیده بگیر و خالی در نظر بگیر
            df_chk = pd.DataFrame()
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

    # ساخت فرم تعریف درصدها
    rows_html = ""
    for g in groups:
        rows_html += f"""
        <tr>
            <td>{g}</td>
            <td>
                <input type="hidden" name="group_name" value="{g}" />
                <input type="number" step="0.01" name="group_percent" placeholder="مثلاً 2 برای 2٪" />
            </td>
        </tr>
        """

    html = f"""
    <html>
        <head>
            <meta charset="utf-8" />
            <title>تعریف درصد پورسانت</title>
            {BASE_CSS}
        </head>
        <body>
            <div class="container">
                <h1>تعریف درصد پورسانت برای گروه‌های کالایی</h1>
                <p>مرحله ۲ از ۲ – برای هر گروه (بر اساس ستون <b>{group_col}</b>) درصد پورسانت را وارد کن.</p>
                <p style="font-size:12px; color:#6b7280;">
                    مثال: عدد 2 یعنی 2٪ پورسانت (0.02)، عدد 0.5 یعنی نیم درصد (0.5٪).
                </p>

                <form action="/calculate-commission" method="post">
                    <div class="table-wrapper">
                        <table>
                            <tr>
                                <th>گروه کالا</th>
                                <th>درصد پورسانت (%)</th>
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


# ------------------ UI مرحله ۲: گرفتن درصدها و محاسبه ------------------ #

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

    # ساخت دیکشنری گروه → درصد پورسانت
    commission_map: dict[str, float] = {}
    for name, p in zip(group_names, percents):
        name = str(name)
        p_str = str(p).strip()
        if not p_str:
            continue

        p_str = p_str.replace(",", ".")
        try:
            val = float(p_str)
        except ValueError:
            # اگر کاربر چیز نامعتبر وارد کرد، ردش می‌کنیم
            continue

        # هر عددی که وارد می‌شود، "درصد" است.
        # 1  → 1%  → 0.01
        # 2  → 2%  → 0.02
        # 0.5 → 0.5% → 0.005
        val = val / 100.0

        # درصد منفی یا صفر را نادیده بگیریم (اختیاری)
        if val <= 0:
            continue

        commission_map[name] = val

    if not commission_map:
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
                    <p>هیچ درصد پورسانتی وارد نشده است.</p>
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

    # محاسبه پورسانت و وضعیت فاکتورها
    sales_result, salesperson_result, _ = compute_commissions(
        df_sales, df_pay, df_chk, commission_map, group_col
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
    # درصد را به درصد انسانی تبدیل کنیم (۱ یعنی ۱٪)
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
