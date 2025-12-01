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

        if year >= 1300:
            try:
                jd = jdatetime.date(year, month, day)
                g = jd.togregorian()
                return pd.Timestamp(g.year, g.month, g.day)
            except Exception:
                return pd.NaT
        else:
            return pd.to_datetime(s, errors="coerce")

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
    نرمال‌سازی ساده نام (فعلاً استفاده‌ی جدی ندارد ولی برای توسعه‌ی بعدی می‌ماند)
    """
    if pd.isna(name):
        return ""
    s = str(name).strip()
    if not s:
        return ""
    s = s.replace("ي", "ی").replace("ك", "ک")
    s = s.lower()
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
    s_no_comma = s.replace(",", "")
    try:
        f = float(s_no_comma)
        if f.is_integer():
            return str(int(f))
    except Exception:
        return s
    return s


app = FastAPI()

LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    "group_col": None,
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


def normalize_persian_name(s) -> str:
    """
    نرمال‌سازی اسم فارسی:
    - یکسان کردن حروف ي/ی و ك/ک و ...
    - حذف حرکات
    - یکسان کردن فاصله‌ها
    """
    if s is None:
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
        "‌": " ",
    }
    for src, dst in replacements.items():
        s = s.replace(src, dst)

    s = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", s)

    for ch in ["،", ",", "-", "_", "ـ"]:
        s = s.replace(ch, " ")

    s = re.sub(r"\s+", " ", s).strip()
    s = s.lower()
    return s


def build_name_code_mapping(sales_df: pd.DataFrame) -> dict[str, str]:
    """
    از روی جدول فروش، map می‌سازد:
        نام نرمال‌شده → کد مشتری
    فقط وقتی که آن نام دقیقاً به *یک* کد منجر شود.
    """
    if "CustomerName" not in sales_df.columns or "CustomerCode" not in sales_df.columns:
        return {}

    tmp = sales_df[["CustomerName", "CustomerCode"]].dropna()

    name_to_codes: dict[str, set[str]] = {}
    for _, row in tmp.iterrows():
        name = normalize_persian_name(row["CustomerName"])
        code = str(row["CustomerCode"]).strip()
        if not name or not code:
            continue
        if name not in name_to_codes:
            name_to_codes[name] = set()
        name_to_codes[name].add(code)

    result: dict[str, str] = {}
    for name, codes in name_to_codes.items():
        if len(codes) == 1:
            result[name] = next(iter(codes))

    return result


def extract_customer_for_payment(
    row,
    checks_df: pd.DataFrame,
    name_code_map: dict[str, str] | None = None,
):
    """
    تشخیص کد مشتری برای هر پرداخت:
    1) اگر CustomerCode پر بود، همان
    2) اگر نوع Check بود، از روی شماره چک در فایل چک‌ها
    3) در نهایت، اگر اسم مشتری داریم و در map نام→کد باشد، از روی اسم
    """
    stype = row.get("SourceType")
    code = row.get("CustomerCode")
    name = row.get("CustomerName")

    if pd.notna(code) and str(code).strip() != "":
        return str(code).strip()

    if stype == "Check":
        desc = str(row.get("Description") or "")
        m = re.search(r"(CHK-\d+)", desc)
        if m and "CheckNumber" in checks_df.columns:
            check_number = m.group(1)
            match = checks_df.loc[checks_df["CheckNumber"] == check_number]
            if not match.empty:
                return str(match.iloc[0]["CustomerCode"])

    if name_code_map is not None and pd.notna(name):
        norm = normalize_persian_name(name)
        if norm:
            mapped = name_code_map.get(norm)
            if mapped:
                return mapped

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

    if "PaymentDate" in payments_df.columns:
        payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
            parse_jalali_or_gregorian
        )

    if "Amount" not in payments_df.columns:
        raise ValueError("در فایل پرداخت‌ها ستونی به نام 'Amount' پیدا نشد.")
    payments_df["Amount"] = payments_df["Amount"].astype(float)

    if "CustomerCode" not in payments_df.columns:
        payments_df["CustomerCode"] = None
    if "CustomerName" not in payments_df.columns:
        payments_df["CustomerName"] = None

    name_code_map = build_name_code_mapping(sales_df)

    payments_df["ResolvedCustomer"] = payments_df.apply(
        lambda row: extract_customer_for_payment(
            row, checks_df, name_code_map
        ),
        axis=1,
    )

    return payments_df


def prepare_sales(sales_df: pd.DataFrame, group_config: dict, group_col: str) -> pd.DataFrame:
    """
    آماده‌سازی دیتافریم فروش‌ها.
    """
    sales_df = sales_df.copy()

    if "InvoiceDate" not in sales_df.columns:
        raise ValueError("در فایل فروش ستونی به نام 'InvoiceDate' پیدا نشد.")
    sales_df["InvoiceDate"] = sales_df["InvoiceDate"].apply(
        parse_jalali_or_gregorian
    )

    if "CustomerCode" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerCode"].map(
            canonicalize_code
        )
    elif "CustomerName" in sales_df.columns:
        sales_df["CustomerKey"] = sales_df["CustomerName"].map(normalize_name)
    else:
        sales_df["CustomerKey"] = None

    if "DueDate" in sales_df.columns:
        sales_df["DueDate"] = sales_df["DueDate"].apply(
            parse_jalali_or_gregorian
        )
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
    sales_df["PriorityRank"] = sales_df["Priority"].map(
        {"cash": 0, "normal": 1}
    ).fillna(1).astype(int)

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


def build_debug_names_html(sales_df: pd.DataFrame, payments_df: pd.DataFrame) -> str:
    """
    بخش دیباگ برای مقایسه‌ی نام‌ها در فروش و پرداخت.
    """
    parts: list[str] = []

    if "CustomerName" in sales_df.columns and "CustomerCode" in sales_df.columns:
        sales_view = sales_df[["CustomerCode", "CustomerName"]].dropna(
            how="all"
        ).copy()
        sales_view["NormName"] = sales_view["CustomerName"].apply(
            normalize_persian_name
        )
        sales_view = sales_view.drop_duplicates().sort_values(
            ["CustomerCode", "CustomerName"]
        )

        parts.append("<h2>🧪 دیباگ نام‌ها (فروش)</h2>")
        parts.append('<div class="table-wrapper">')
        parts.append(sales_view.to_html(index=False, border=0))
        parts.append("</div>")
    else:
        parts.append(
            "<p>در جدول فروش ستون‌های CustomerName / CustomerCode پیدا نشد.</p>"
        )

    if not payments_df.empty:
        cols = []
        for c in ["PaymentID", "CustomerCode", "CustomerName", "ResolvedCustomer", "Amount"]:
            if c in payments_df.columns:
                cols.append(c)
        if cols:
            pay_view = payments_df[cols].copy()
            if "CustomerName" in pay_view.columns:
                pay_view["NormName"] = pay_view["CustomerName"].apply(
                    normalize_persian_name
                )
            else:
                pay_view["NormName"] = ""
            pay_view = pay_view.drop_duplicates().head(200)

            parts.append("<h2>🧪 دیباگ نام‌ها (پرداخت‌ها)</h2>")
            parts.append(
                '<p style="font-size:12px;color:#6b7280;">'
                "ستون ResolvedCustomer نشان می‌دهد این ردیف به کدام کد مشتری وصل شده (اگر شده باشد).</p>"
            )
            parts.append('<div class="table-wrapper">')
            parts.append(pay_view.to_html(index=False, border=0))
            parts.append("</div>")
    else:
        parts.append("<p>هیچ پرداختی بعد از لود یافت نشد.</p>")

    name_code_map = build_name_code_mapping(sales_df)
    if name_code_map:
        map_rows = []
        for norm_name, code in sorted(name_code_map.items(), key=lambda x: x[1]):
            map_rows.append({"NormName": norm_name, "CustomerCode": code})
        map_df = pd.DataFrame(map_rows)

        parts.append(
            "<h2>🧪 نگاشت نام نرمال‌شده → کد مشتری (از روی فروش‌ها)</h2>"
        )
        parts.append(
            '<p style="font-size:12px;color:#6b7280;">'
            "اگر NormName پرداخت با یکی از NormNameهای این جدول برابر باشد، باید به همان CustomerCode وصل شود.</p>"
        )
        parts.append('<div class="table-wrapper">')
        parts.append(map_df.to_html(index=False, border=0))
        parts.append("</div>")
    else:
        parts.append(
            "<p>نتوانستم از روی فروش‌ها map نام→کد بسازم (هیچ اسم یکتایی وجود ندارد یا ستون‌ها ناقص است).</p>"
        )

    return "<hr/>" + "\n".join(parts)


def compute_commissions(
    sales_raw: pd.DataFrame,
    payments_raw: pd.DataFrame,
    checks_raw: pd.DataFrame | None,
    group_config: dict,
    group_col: str,
):
    sales_df = prepare_sales(sales_raw, group_config, group_col)

    checks_df = (
        checks_raw.copy()
        if checks_raw is not None and not checks_raw.empty
        else pd.DataFrame()
    )

    payments_df = prepare_payments(payments_raw, checks_df, sales_df)

    if "Salesperson" not in sales_df.columns:
        sales_df["Salesperson"] = None

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

    for cust, pay_group in payments_df.groupby("ResolvedCustomer"):
        if pd.isna(cust) or str(cust).strip() == "":
            continue
        cust_str = str(cust).strip()

        cust_invoice_idx = sales_df.index[
            sales_df["CustomerCode"].astype(str).str.strip() == cust_str
        ]
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
            remaining_payment = float(p["Amount"])
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
    df_sales = load_sales_excel(sales_file.file)
    df_pay = load_payments_excel(payments_file.file)

    if checks_file is not None and checks_file.filename:
        df_chk = pd.read_excel(checks_file.file)
    else:
        df_chk = pd.DataFrame()

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

    LAST_UPLOAD["sales"] = df_sales
    LAST_UPLOAD["payments"] = df_pay
    LAST_UPLOAD["checks"] = df_chk
    LAST_UPLOAD["group_col"] = group_col

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

    group_config: dict = {}
    for name, p, dd in zip(group_names, percents, due_days_list):
        key = str(name).strip()
        if not key:
            continue

        percent_val = 0.0
        p_str = str(p).strip()
        if p_str:
            p_str = p_str.replace(",", ".")
            try:
                percent_val = float(p_str) / 100.0
            except ValueError:
                percent_val = 0.0

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

    sales_result, salesperson_result, payments_result = compute_commissions(
        df_sales, df_pay, df_chk, group_config, group_col
    )

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
            salesperson_result["TotalCommission"].sum() or 0
        )

    invoices_view = sales_result.copy()

    for dt_col in ["InvoiceDate", "DueDate"]:
        if dt_col in invoices_view.columns:
            invoices_view[dt_col] = invoices_view[dt_col].map(to_jalali_str)

    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = (
            invoices_view["CommissionPercent"] * 100
        ).round(2)

    if "Priority" in invoices_view.columns:
        def pri_badge(v):
            if v == "cash":
                return '<span class="badge badge-priority-cash">نقدی</span>'
            elif v == "normal":
                return '<span class="badge badge-priority-normal">عادی</span>'
            return ""
        invoices_view["Priority"] = invoices_view["Priority"].map(pri_badge)

    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = invoices_view["CommissionPercent"].map(
            lambda x: f"{x:.2f}٪"
        )

    for col in ["Amount", "PaidAmount", "Remaining", "CommissionAmount"]:
        if col in invoices_view.columns:
            invoices_view[col] = invoices_view[col].round(0).astype("int64")

    cols = []
    for c in [
        "InvoiceID", "CustomerCode", "CustomerName", group_col,
        "Priority", "InvoiceDate", "DueDate",
        "Amount", "PaidAmount", "Remaining",
        "CommissionPercent", "CommissionAmount",
    ]:
        if c in invoices_view.columns:
            cols.append(c)

    invoices_table_html = ""
    if cols:
        invoices_table_html = invoices_view[cols].to_html(
            index=False, border=0, escape=False
        )

    if "TotalCommission" in salesperson_result.columns:
        salesperson_result["TotalCommission"] = (
            salesperson_result["TotalCommission"].round(0).astype("int64")
        )
    salesperson_table_html = salesperson_result.to_html(index=False, border=0)

    # 🔍 اینجا قبل از ساختن HTML، دیباگ را تولید می‌کنیم
    debug_names_html = build_debug_names_html(sales_result, payments_result)

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

                {debug_names_html}

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
