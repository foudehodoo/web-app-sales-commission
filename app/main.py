from __future__ import annotations
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, UploadFile, File, Request
import pandas as pd
import io
import os
import json

# ایمپورت سرویس‌ها
from app.services.sales_excel_loader import load_sales_excel
from app.services.payments_excel_loader import load_payments_excel
from app.services.checks_excel_loader import load_checks_excel
from app.services.customer_balances import (
    load_balances_from_excel,
    save_balances_to_db,
    load_balances_from_db,
    update_balances,
    normalize_name as normalize_balance_name,
)

# ایمپورت هلپرها
from app.services.helpers import (
    canonicalize_code,
    normalize_persian_name,
    name_key_for_matching,
    parse_jalali_or_gregorian,
    to_jalali_str,
    format_number
)

# ایمپورت سرویس پورسانت
from app.services.commission_service import (
    load_default_group_config,
    load_product_group_map,
    save_product_group_map,
    get_priority,
    load_blacklist_sets,
    load_product_blacklist_set,
    save_product_blacklist,
    load_allowed_marketers,
    save_marketers_list,
    prepare_sales,
    compute_commissions,
    build_name_code_map_from_balances,
    load_name_code_map_from_excel,
    extract_customer_for_payment,
    prepare_payments,
    build_name_code_mapping
)
# ------------------ تنظیمات فایل‌های پیکربندی ------------------ #

DEFAULT_GROUP_CONFIG_PATH = "group_config.xlsx"
PRODUCT_GROUP_MAP_PATH = "product_group_map.xlsx"
# مدیریت بلک لیست
BLACKLIST_FILE = "blacklist.xlsx"
MARKETERS_PATH = "marketers.xlsx"
PRODUCT_BLACKLIST_PATH = "product_blacklist.xlsx"
# نام فایل خروجی
OUTPUT_CODES_FILENAME = "customer_codes_generated.xlsx"


# ------------------ کانفیگ برنامه ------------------ #

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
templates_path = BASE_DIR / "templates"
static_path = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

templates = Jinja2Templates(directory=str(templates_path))
templates.env.filters["format_number"] = format_number

LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    "group_col": None,
    "group_config": None,
    "sales_result": None,
    "payments_result": None,
}
SESSION_SETTINGS = {
    "reactivation_days": 95  # مقدار پیش‌فرض
}

# ---------------------------------------------------------
#  گام ۱: افزودن ماژول‌های منطق زمانی و CRM
# ---------------------------------------------------------


class CRMConfigLoader:
    """مدیریت تنظیمات و هدرهای اتصال به CRM"""

    def __init__(self, settings_path="commission_settings.json", headers_path="headers.json"):
        self.settings_path = settings_path
        self.headers_path = headers_path
        self.settings = {}
        self.headers = {}
        self.load_configs()

    def load_configs(self):
        # بارگذاری تنظیمات پورسانت و قوانین زمانی
        if os.path.exists(self.settings_path):
            with open(self.settings_path, 'r', encoding='utf-8') as f:
                self.settings = json.load(f)

        # بارگذاری هدرها برای اتصال به CRM
        if os.path.exists(self.headers_path):
            with open(self.headers_path, 'r', encoding='utf-8') as f:
                self.headers = json.load(f)

    def get_max_gap_days(self):
        """تعداد روزهایی که اگر مشتری خرید نکند، دوباره مشتری جدید (طلایی) محسوب می‌شود"""
        return self.settings.get("max_gap_days", 90)  # پیش‌فرض ۹۰ روز


class TimeBasedCommissionLogic:
    """
    منطق محاسبات بر مبنای زمان:
    تشخیص می‌دهد آیا مشتری 'جدید' است یا 'قدیمی' یا 'بازگشتی'.
    """

    def __init__(self, historical_df: pd.DataFrame = None):
        # این دیتافریم شامل سوابق خرید سال‌های قبل (مثلاً ۱۴۰۳ و ۱۴۰۴ شهریور) است
        self.history = historical_df
        # تبدیل تاریخ‌ها به datetime برای مقایسه راحت‌تر
        if self.history is not None and not self.history.empty:
            # فرض بر این است که ستونی به نام 'Date' یا 'InvoiceDate' داریم
            date_col = next(
                (col for col in self.history.columns if 'date' in col.lower() or 'تاریخ' in col), None)
            customer_col = next(
                (col for col in self.history.columns if 'customer' in col.lower() or 'مشتری' in col), None)

            if date_col and customer_col:
                self.history[date_col] = pd.to_datetime(
                    self.history[date_col], errors='coerce')
                self.last_purchase_map = self.history.groupby(
                    customer_col)[date_col].max().to_dict()
            else:
                self.last_purchase_map = {}
        else:
            self.last_purchase_map = {}

    def get_customer_status(self, customer_name: str, current_invoice_date: pd.Timestamp, gap_threshold_days: int) -> dict:
        """
        وضعیت مشتری را برمی‌گرداند:
        - New: کلاً در سوابق نیست.
        - Reactivated: در سوابق هست، اما آخرین خریدش خیلی قدیمی است (بیشتر از حد مجاز).
        - Active: مشتری فعال و عادی.
        """
        if not self.last_purchase_map or customer_name not in self.last_purchase_map:
            return {"status": "New", "commission_multiplier": 1.5, "reason": "مشتری جدید (بدون سابقه)"}

        last_date = self.last_purchase_map[customer_name]

        # اگر تاریخ سابقه نامعتبر بود
        if pd.isna(last_date):
            return {"status": "New", "commission_multiplier": 1.5, "reason": "مشتری جدید (تاریخ نامعتبر)"}

        # محاسبه فاصله زمانی
        # هندل کردن تبدیل تاریخ شمسی به میلادی باید قبل از این تابع انجام شده باشد یا اینجا هندل شود
        days_diff = (current_invoice_date - last_date).days

        if days_diff > gap_threshold_days:
            return {
                "status": "Reactivated",
                "commission_multiplier": 1.2,
                "reason": f"بازگشت مشتری پس از {days_diff} روز (بیشتر از {gap_threshold_days} روز)"
            }

        return {"status": "Active", "commission_multiplier": 1.0, "reason": "مشتری فعال"}


# نمونه‌سازی اولیه (Global)
crm_config = CRMConfigLoader()


# --- روت‌های صفحه مدیریت بلک‌لیست ---

@app.get("/blacklist", response_class=HTMLResponse)
async def blacklist_page(request: Request):
    file_path = "blacklist.xlsx"
    data_records = []

    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path)

            # --- بخش منطق (Logic) ---
            if "CustomerCode" in df.columns:
                df["CustomerCode"] = df["CustomerCode"].apply(
                    lambda x: canonicalize_code(x) if pd.notna(x) else ""
                )
                df["CustomerCode"] = df["CustomerCode"].fillna(
                    "").astype(str).replace("nan", "")

            # تبدیل تاریخ‌ها
            if "Date Added" in df.columns:
                df["Date Added"] = df["Date Added"].fillna("")
            if "DateAdded" in df.columns:
                df["DateAdded"] = df["DateAdded"].fillna("")

            df = df.fillna("")
            data_records = df.to_dict(orient="records")

        except Exception as e:
            print(f"Error loading blacklist: {e}")
            data_records = []

    # --- بخش رندر (Render) ---
    return templates.TemplateResponse(
        # نام فایل به عنوان آرگومان اول (در نسخه‌های جدیدتر FastAPI/Starlette)
        "blacklist.html",
        {
            "request": request,
            "data_records": data_records,
            "title": "لیست سیاه",      # عنوان صفحه
            "active_tab": "blacklist"  # جایگزین nav_html شد
        }
    )


@app.post("/upload-blacklist")
async def upload_blacklist(request: Request, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        # فقط چک میکنیم فایل اکسل سالم باشد
        temp_df = pd.read_excel(io.BytesIO(contents))
        if "CustomerCode" not in temp_df.columns:
            # استفاده از تمپلیت خطا به جای HTML خام
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "message": "خطا: فایل اکسل باید ستون CustomerCode داشته باشد.",
                    "back_url": "/blacklist"
                }
            )

        # ذخیره روی فایل اصلی
        with open(BLACKLIST_FILE, "wb") as f:
            f.write(contents)

        return RedirectResponse(url="/blacklist", status_code=303)
    except Exception as e:
        # استفاده از تمپلیت خطا برای اکسپشن‌ها
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": f"خطا در آپلود: {e}",
                "back_url": "/blacklist"
            }
        )

# ------------------ توابع کمکی ------------------ #

# ------------------ UI: مرحله جدید - دریافت فایل‌های پرداخت و چک ------------------


@app.get("/upload-payments-checks")
async def upload_payments_checks_page(request: Request):
    """
    صفحه جدید برای دریافت فایل‌های پرداخت و چک و ساخت اکسل کدها.
    """
    # در اینجا فقط نام تب فعال را به تمپلیت می‌فرستیم تا کلاس active را بگیرد
    return templates.TemplateResponse(
        "upload_payments_checks.html",
        {
            "request": request,
            "active_tab": "main"  # این متغیر در navbar.html استفاده می‌شود
        }
    )


@app.post("/process-payments-checks")
async def process_payments_checks(
    request: Request,
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None)
):
    try:
        # 1. بارگذاری فایل‌ها
        df_pay = load_payments_excel(payments_file.file)
        df_chk = pd.DataFrame()
        if checks_file and checks_file.filename:
            df_chk = load_checks_excel(checks_file.file)

        # 2. ساخت مپ نام به کد از دیتابیس مانده‌ها
        name_code_map_from_balances = build_name_code_map_from_balances()

        # 3. آماده‌سازی پرداخت‌ها
        # نکته: آرگومان سوم که قبلا اشتباه بود، با مپ صحیح جایگزین شد
        payments_df, unresolved_items = prepare_payments(
            df_pay, df_chk, name_code_map_from_balances
        )

        # 4. ساخت دیتافریم برای نمایش و دانلود
        result_data = []

        # پردازش مواردی که کد پیدا شد
        resolved_df = payments_df[payments_df["ResolvedCustomer"].notna()].copy(
        )
        if not resolved_df.empty:
            grouped = resolved_df.groupby("ResolvedCustomer").agg({
                "CustomerName": "first",
                "Amount": "sum"
            }).reset_index()
            for _, row in grouped.iterrows():
                result_data.append({
                    "CustomerName": row["CustomerName"], "TotalAmount": row["Amount"],
                    "CustomerCode": row["ResolvedCustomer"], "Status": "کد یافت شد ✅"
                })

        # پردازش مواردی که کد پیدا نشد (Unresolved)
        if unresolved_items:
            unresolved_df = pd.DataFrame(unresolved_items)
            grouped_unresolved = unresolved_df.groupby(
                "Name").agg({"Amount": "sum"}).reset_index()
            for _, row in grouped_unresolved.iterrows():
                result_data.append({
                    "CustomerName": row["Name"], "TotalAmount": row["Amount"],
                    "CustomerCode": "", "Status": "کد یافت نشد ❌"
                })

        df_result = pd.DataFrame(result_data)
        LAST_UPLOAD["payments_codes_preview"] = df_result

        # ساخت HTML جدول
        table_html = "<p>داده‌ای برای نمایش وجود ندارد.</p>"
        if not df_result.empty:
            table_html = df_result.to_html(
                index=False, border=0, classes="data-table")

        # رندر کردن تمپلیت به جای ساخت HTML در پایتون
        return templates.TemplateResponse(
            "process_payments_checks_result.html",
            {
                "request": request,
                "active_tab": "main",
                "table_html": table_html,
                "has_results": not df_result.empty,  # یک boolean برای نمایش شرطی دکمه دانلود
            }
        )

    except Exception as e:
        # استفاده مجدد از تمپلیت خطا
        print(f"Error processing payments/checks: {e}")
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": f"خطا در پردازش فایل‌ها: {str(e)}",
                "back_url": "/upload-payments-checks"
            }
        )


@app.get("/download-codes-excel")
async def download_codes_excel(request: Request):
    """
    دانلود فایل اکسل حاوی کدهای استخراج شده.
    در صورت عدم وجود داده، صفحه خطا را با استفاده از تمپلیت نمایش می‌دهد.
    """
    df_result = LAST_UPLOAD.get("payments_codes_preview")

    # بلوک خطا: از تمپلیت error.html استفاده می‌کنیم
    if df_result is None or df_result.empty:
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": "خطا: داده‌ای برای دانلود وجود ندارد. لطفاً ابتدا فایل‌ها را برای استخراج کدها پردازش کنید.",
                "back_url": "/upload-payments-checks"  # لینک بازگشت به صفحه آپلود
            }
        )

    # بلوک موفقیت: این بخش بدون تغییر باقی می‌ماند
    # ایجاد یک فایل در حافظه
    output = io.BytesIO()

    # استفاده از ExcelWriter برای نوشتن
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_result.to_excel(writer, index=False, sheet_name='Codes')

    output.seek(0)

    # ارسال فایل به کاربر
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=customer_codes_extracted.xlsx"}
    )

# ------------------ UI: تب ۱ – محاسبه پورسانت ------------------ #


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    نمایش صفحه اصلی برنامه.
    این تابع از تمپلیت index.html استفاده می‌کند و تب فعال را برای navbar مشخص می‌کند.
    """
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "سیستم جامع فروش",  # عنوان را هم می‌توانیم بهبود دهیم
            "active_tab": "main"  # فقط این متغیر را برای کنترل navbar ارسال می‌کنیم
        },
    )

# ------------------ UI: تب ۴ – مدیریت مانده مشتریان ------------------


@app.get("/customer-balances", response_class=HTMLResponse)
async def customer_balances_page(request: Request):
    # بارگذاری داده‌ها از دیتابیس (یا فایل JSON/Excel بسته به پیاده‌سازی شما)
    current_data = load_balances_from_db()

    processed_data = []
    if current_data:
        for item in current_data:
            # استخراج ایمن داده‌ها
            code = item.get("CustomerCode", "")
            # تبدیل به عدد صحیح اگر اعشار صفر دارد (مثلاً 1001.0 -> 1001)
            display_code = int(float(code)) if code and str(
                code) != 'nan' else ""

            name = item.get("OriginalName", item.get("CustomerName", ""))
            balance = item.get("Balance", 0)

            # آماده‌سازی داده برای نمایش راحت‌تر در تمپلیت
            processed_data.append({
                "raw_code": code,           # برای استفاده در توابع JS
                "display_code": display_code,  # برای نمایش در جدول
                "name": name,
                "balance": balance,
                "balance_fmt": f"{balance:,.0f}",  # فرمت سه رقم سه رقم
                "color": "red" if balance < 0 else "green"
            })

    return templates.TemplateResponse(
        "customer_balances.html",
        {
            "request": request,
            "balances": processed_data,
            "title": "مدیریت مانده حساب مشتریان",
            "active_tab": "balances"
        }
    )


@app.post("/upload-balances", response_class=HTMLResponse)
async def upload_balances(request: Request):
    form = await request.form()
    file = form.get("balances_file")

    # بررسی انتخاب فایل
    if not file or not file.filename:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "فایلی انتخاب نشده است.",
            "back_link": "/customer-balances"
        })

    # استفاده از سرویس برای خواندن فایل
    new_items = load_balances_from_excel(file.file)

    if not new_items:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "نتوانستیم داده‌ای از فایل استخراج کنیم. ساختار فایل را بررسی کنید.",
            "back_link": "/customer-balances"
        })

    # به‌روزرسانی دیتابیس
    update_balances(new_items)

    # ریدایرکت به صفحه نمایش
    return RedirectResponse(url="/customer-balances", status_code=303)


@app.post("/edit-balance")
async def edit_balance(request: Request):
    form = await request.form()
    old_name = form.get("old_name")  # نام نرمال شده برای پیدا کردن ردیف قدیمی
    new_code = form.get("code")
    new_name = form.get("name")
    new_balance_str = form.get("balance")

    current_data = load_balances_from_db()

    # پیدا کردن و آپدیت آیتم
    updated_data = []
    found = False
    for item in current_data:
        if item.get("CustomerName") == old_name:
            found = True
            # نرمال‌سازی نام جدید
            norm_name = normalize_balance_name(new_name)
            try:
                bal = float(new_balance_str)
            except ValueError:
                bal = 0

            updated_data.append({
                "CustomerCode": str(new_code).strip(),
                "CustomerName": norm_name,
                "OriginalName": str(new_name).strip(),
                "Balance": bal
            })
        else:
            updated_data.append(item)

    if found:
        save_balances_to_db(updated_data)

    return JSONResponse(content={"status": "ok"})


@app.post("/add-balance")
async def add_balance(request: Request):
    form = await request.form()
    code = form.get("code")
    name = form.get("name")
    balance_str = form.get("balance")

    norm_name = normalize_balance_name(name)
    try:
        bal = float(balance_str)
    except ValueError:
        bal = 0

    new_item = {
        "CustomerCode": str(code).strip(),
        "CustomerName": norm_name,
        "OriginalName": str(name).strip(),
        "Balance": bal
    }

    update_balances([new_item])
    return JSONResponse(content={"status": "ok"})


@app.post("/delete-balance")
async def delete_balance(request: Request):
    form = await request.form()
    # دریافت کد و نام از فرم
    code = form.get("customer_code")
    name = form.get("customer_name")

    if not code and not name:
        return JSONResponse(content={"status": "error", "message": "کد یا نام ارسال نشده است"}, status_code=400)

    current_data = load_balances_from_db()
    new_data = []
    found = False

    for item in current_data:
        item_code = str(item.get("CustomerCode", ""))
        item_name = item.get("CustomerName", "")

        # اولویت با حذف بر اساس کد مشتری است (دقیق‌تر)
        should_delete = False
        if code:
            if item_code == str(code):
                should_delete = True
        elif name:
            # اگر کد نبود، با نام مقایسه کن (فقط به عنوان فال‌بک)
            if item_name == name:
                should_delete = True

        if should_delete:
            found = True
        else:
            new_data.append(item)

    if found:
        save_balances_to_db(new_data)
        return JSONResponse(content={"status": "ok"})
    else:
        return JSONResponse(content={"status": "error", "message": "موردی یافت نشد"}, status_code=404)


@app.post("/clear-balances")
async def clear_balances():
    """
    مسیر مربوط به دکمه «حذف تمام مانده‌ها».
    یک دیتافریم خالی با ستون‌های صحیح می‌سازیم تا تابع save_balances_to_db خطا ندهد.
    """
    # ساخت یک دیتافریم خالی با ستون‌های مورد نیاز برای جلوگیری از خطای sort_values
    empty_df = pd.DataFrame(
        columns=["CustomerCode", "CustomerName", "OriginalName", "Balance"])
    save_balances_to_db(empty_df)
    return JSONResponse(content={"status": "ok"})


@app.post("/upload-all", response_class=HTMLResponse)
async def upload_all(
    request: Request,
    sales_file: UploadFile = File(...),
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None),
    history_file: UploadFile | None = File(None)
):
    # 1. دریافت تنظیمات روزهای فعال‌سازی
    form = await request.form()
    reactivation_days_str = form.get("reactivation_days")
    if reactivation_days_str:
        try:
            reactivation_days = int(reactivation_days_str)
        except ValueError:
            reactivation_days = 90
    else:
        reactivation_days = SESSION_SETTINGS.get("reactivation_days", 90)

    # 2. بارگذاری فایل‌های اصلی
    df_sales = load_sales_excel(sales_file.file)
    df_pay = load_payments_excel(payments_file.file)

    if checks_file is not None and checks_file.filename:
        df_chk = load_checks_excel(checks_file.file)
    else:
        df_chk = pd.DataFrame()

    # 3. بارگذاری فایل سوابق (History)
    history_found = False
    if history_file is not None and history_file.filename:
        try:
            df_history = pd.read_excel(history_file.file)
            # نرمال‌سازی نام ستون‌ها (حذف ی و ک عربی)
            df_history.columns = df_history.columns.str.replace(
                'ي', 'ی', regex=True)
            df_history.columns = df_history.columns.str.replace(
                'ك', 'ک', regex=True)

            # نرمال‌سازی محتوا
            obj_cols = df_history.select_dtypes(include=['object']).columns
            for col in obj_cols:
                df_history[col] = df_history[col].astype(
                    str).str.replace('ي', 'ی').str.replace('ك', 'ک')

            if not df_history.empty:
                history_found = True
        except Exception as e:
            print(f"Error loading history file: {e}")
            df_history = pd.DataFrame()
    else:
        df_history = pd.DataFrame()

    # 4. تشخیص ستون گروه کالا
    if "ProductCode" in df_sales.columns:
        group_col = "ProductCode"
    elif "ProductGroup" in df_sales.columns:
        group_col = "ProductGroup"
    else:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "در فایل فروش‌ها ستونی به نام ProductCode یا ProductGroup پیدا نشد. لطفاً یکی از این ستون‌ها را اضافه کنید.",
            "back_link": "/"
        })

    groups = sorted(df_sales[group_col].dropna().unique())

    # 5. ذخیره در متغیر سراسری (State)
    LAST_UPLOAD["sales"] = df_sales
    LAST_UPLOAD["payments"] = df_pay
    LAST_UPLOAD["checks"] = df_chk
    LAST_UPLOAD["history"] = df_history
    LAST_UPLOAD["group_col"] = group_col

    # 6. خواندن پیکربندی‌ها برای نگاشت کالاها
    default_group_cfg = load_default_group_config()
    prod_group_df = load_product_group_map()

    code_to_category: dict[str, str] = {}
    if not prod_group_df.empty:
        for _, row in prod_group_df.iterrows():
            code = canonicalize_code(row.get("ProductCode"))
            grp = str(row.get("Group") or "").strip()
            if code and grp:
                code_to_category[code] = grp

    # 7. حدس ستون نام کالا برای نمایش زیباتر
    name_col_candidates = [
        "ProductName", "ProductGroupName", "ProductGroupTitle",
        "نام کالا", "نام گروه کالا"
    ]
    group_name_col = None
    for c in name_col_candidates:
        if c in df_sales.columns and c != group_col:
            group_name_col = c
            break

    # 8. آماده‌سازی داده پیکربندی برای ارسال به JS (جهت پر کردن خودکار فیلدها)
    js_cfg_map = {
        gname: {
            "percent": (cfg.get("percent") or 0) * 100,
            "due_days": cfg.get("due_days"),
            "is_cash": bool(cfg.get("is_cash")),
        }
        for gname, cfg in default_group_cfg.items()
    }
    js_cfg_json = json.dumps(js_cfg_map, ensure_ascii=False)

    # 9. ساخت لیست ردیف‌های جدول برای ارسال به Template
    group_rows = []

    for g in groups:
        key_str = str(g)
        pretty_str = canonicalize_code(g)
        if pretty_str is None:
            pretty_str = ""

        # یافتن نام نمایشی (مثلاً: 1001 - یخچال فریز)
        display_name = ""
        if group_name_col is not None:
            sample_rows = df_sales[df_sales[group_col] == g]
            if not sample_rows.empty:
                display_name = str(sample_rows.iloc[0][group_name_col])

        if display_name:
            display_text = f"{pretty_str} – {display_name}"
        else:
            display_text = pretty_str or key_str

        # منطق پیدا کردن تنظیمات پیش‌فرض (از مپ کالا یا نام گروه)
        category_for_code = None
        if group_col == "ProductCode":
            canon_code = canonicalize_code(g)
            if canon_code:
                category_for_code = code_to_category.get(canon_code)

        pre_cfg = None
        selected_category = ""

        # اولویت ۱: تنظیمات اختصاصی کد کالا
        if category_for_code and category_for_code in default_group_cfg:
            selected_category = category_for_code
            pre_cfg = default_group_cfg[category_for_code]
        # اولویت ۲: تنظیمات هم‌نام با کد گروه
        elif key_str in default_group_cfg:
            selected_category = key_str
            pre_cfg = default_group_cfg[key_str]

        # مقادیر اولیه برای اینپوت‌ها
        pre_percent = ""
        pre_due_days = ""
        pre_is_cash = False

        if pre_cfg:
            val = (pre_cfg.get("percent") or 0) * 100
            pre_percent = f"{val:.2f}"

            dd = pre_cfg.get("due_days")
            if dd is not None:
                pre_due_days = dd

            pre_is_cash = pre_cfg.get("is_cash", False)

        # اضافه کردن داده‌های این ردیف به لیست
        group_rows.append({
            "key_str": key_str,
            "display_text": display_text,
            "selected_category": selected_category,
            "pre_percent": pre_percent,
            "pre_due_days": pre_due_days,
            "pre_is_cash": pre_is_cash
        })

    # 10. رندر کردن تمپلیت
    return templates.TemplateResponse(
        "configure_groups.html",
        {
            "request": request,
            "active_tab": "main",
            "group_rows": group_rows,
            "default_group_cfg": default_group_cfg,
            "group_col": group_col,
            "history_found": history_found,
            "reactivation_days": reactivation_days,
            "js_cfg_json": js_cfg_json
        }
    )

# ------------------ /calculate-commission ------------------ #


@app.post("/calculate-commission", response_class=HTMLResponse)
async def calculate_commission(request: Request):
    """
    محاسبه پورسانت بر اساس تنظیمات گروه‌های وارد شده.
    """

    # =========== بررسی آپلود فایل‌ها ===========
    if LAST_UPLOAD["sales"] is None or LAST_UPLOAD["payments"] is None:
        return templates.TemplateResponse(
            "error_no_upload.html",
            {
                "request": request,
                "active_tab": "main",
                "title": "خطا"
            }
        )

    # =========== دریافت داده‌های فرم ===========
    form = await request.form()

    group_names = form.getlist("group_name")
    categories = form.getlist("group_category")
    percents = form.getlist("group_percent")
    due_days_list = form.getlist("group_due_days")
    cash_groups = set(form.getlist("cash_group"))
    use_chart = form.get("use_chart") == "1"
    apply_balances = form.get("apply_balances") == "1"

    # =========== خواندن مانده‌ها ===========
    balances_dict = {}
    if apply_balances:
        balances_dict = load_balances_from_db()
        print(
            f"DEBUG: Apply Balances is ON. Loaded {len(balances_dict)} customer balances.")

    # =========== پردازش تنظیمات گروه‌ها ===========
    group_config: dict = {}
    for name, cat, p, dd in zip(group_names, categories, percents, due_days_list):
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

        is_cash = (key in cash_groups)

        group_config[key] = {
            "percent": percent_val,
            "due_days": due_days_val,
            "is_cash": is_cash,
            "category": str(cat).strip() if cat else None,
        }

    # =========== بررسی خالی نبودن تنظیمات ===========
    if not group_config:
        return templates.TemplateResponse(
            "error_no_config.html",
            {
                "request": request,
                "active_tab": "main",
                "title": "خطا"
            }
        )

    # =========== دریافت دیتافریم‌های اصلی ===========
    df_sales = LAST_UPLOAD["sales"]
    df_pay = LAST_UPLOAD["payments"]
    df_chk = LAST_UPLOAD["checks"]
    group_col = LAST_UPLOAD["group_col"]
    LAST_UPLOAD["group_config"] = group_config

    # =========== خواندن reactivation_days ===========
    reactivation_days_str = form.get("reactivation_days")
    if reactivation_days_str is None:
        reactivation_days = SESSION_SETTINGS.get("reactivation_days", 90)
    else:
        try:
            reactivation_days = int(reactivation_days_str)
        except ValueError:
            reactivation_days = SESSION_SETTINGS.get("reactivation_days", 90)

    # =========== محاسبات اصلی ===========
    sales_result, salesperson_result, payments_result = compute_commissions(
        df_sales,
        df_pay,
        df_chk,
        group_config,
        group_col,
        reactivation_days=reactivation_days
    )

    LAST_UPLOAD["sales_result"] = sales_result
    LAST_UPLOAD["payments_result"] = payments_result

    # =========== داده‌های خلاصه ===========
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

    # =========== ساخت جدول فاکتورها ===========
    invoices_view = sales_result.copy()

    for dt_col in ["InvoiceDate", "DueDate"]:
        if dt_col in invoices_view.columns:
            invoices_view[dt_col] = invoices_view[dt_col].map(to_jalali_str)

    if "CommissionPercent" in invoices_view.columns:
        invoices_view["CommissionPercent"] = (
            invoices_view["CommissionPercent"] * 100).round(2)

    for col in ["InvoiceID", "CustomerCode", group_col]:
        if col in invoices_view.columns:
            invoices_view[col] = invoices_view[col].map(
                lambda v: canonicalize_code(v) if pd.notna(v) else ""
            )

    if "CustomerName" in invoices_view.columns and "CustomerCode" in invoices_view.columns:
        def make_customer_link(row):
            name = row.get("CustomerName", "")
            code = row.get("CustomerCode", "")
            if pd.isna(name) or str(name).strip() == "":
                return ""
            if not use_chart:
                return str(name)
            return (
                f'<a href="#" class="customer-link" '
                f'data-customer-code="{code}" '
                f'data-customer-name="{name}">{name}</a>'
            )
        invoices_view["CustomerName"] = invoices_view.apply(
            make_customer_link, axis=1)

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
        "InvoiceID", "CustomerCode", "CustomerName", group_col, "Priority",
        "InvoiceDate", "DueDate", "Amount", "PaidAmount", "Remaining",
        "CommissionPercent", "CommissionAmount",
    ]:
        if c in invoices_view.columns:
            cols.append(c)

    invoices_table_html = ""
    if cols:
        invoices_table_html = invoices_view[cols].to_html(
            index=False, border=0, escape=False, classes="data-table"
        )

    # =========== ساخت جدول فروشندگان ===========
    if "TotalCommission" in salesperson_result.columns:
        salesperson_result["TotalCommission"] = salesperson_result["TotalCommission"].round(
            0).astype("int64")

    salesperson_table_html = salesperson_result.to_html(
        index=False, border=0, classes="data-table")

    # # =========== بخش‌های Debug ===========
    # debug_names_html = build_debug_names_html(sales_result, payments_result)
    # debug_checks_html = build_debug_checks_html(df_chk, payments_result)

    # =========== رندر Template ===========
    return templates.TemplateResponse(
        "commission_results.html",
        {
            "request": request,
            "active_tab": "main",
            "title": "نتیجه محاسبه پورسانت",
            "use_chart": use_chart,
            "sales_rows": sales_rows,
            "sales_sum": sales_sum,
            "pay_rows": pay_rows,
            "pay_sum": pay_sum,
            "chk_rows": chk_rows,
            "chk_sum": chk_sum,
            "total_commission": total_commission,
            "invoices_table_html": invoices_table_html,
            "salesperson_table_html": salesperson_table_html,
            # "debug_names_html": debug_names_html,
            # "debug_checks_html": debug_checks_html,
        }
    )


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


@app.get("/group-config")
async def group_config_page(request: Request):
    """صفحه تعریف گروه‌های کالا (پیش‌فرض)"""
    current_cfg = load_default_group_config()

    # آماده‌سازی داده‌های فرم
    group_rows = []
    for idx, (gname, cfg) in enumerate(current_cfg.items()):
        percent_human = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)

        group_rows.append({
            "idx": idx,
            "name": gname,
            "percent": f"{percent_human:.2f}" if percent_human > 0 else "",
            "due_days": str(due_days) if due_days else "",
            "is_cash": is_cash
        })

    return templates.TemplateResponse(
        "group_config.html",
        {
            "request": request,
            "active_tab": "config",
            "title": "تعریف گروه‌های کالا (پیش‌فرض)",
            "group_rows": group_rows
        }
    )


@app.post("/group-config")
async def group_config_save(request: Request):
    """ذخیره تنظیمات گروه‌های کالا"""
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

        rows_data.append({
            "Group": g_key,
            "Percent": percent_val,
            "DueDays": due_val,
            "IsCash": is_cash,
        })

    # ذخیره داده‌ها
    success = False
    if rows_data:
        try:
            df_out = pd.DataFrame(rows_data)
            df_out.to_excel(DEFAULT_GROUP_CONFIG_PATH, index=False)
            success = True
        except Exception as e:
            success = False

    # آماده‌سازی داده‌ها برای نمایش مجدد
    current_cfg = load_default_group_config()
    group_rows = []
    for idx, (gname, cfg) in enumerate(current_cfg.items()):
        percent_human = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)

        group_rows.append({
            "idx": idx,
            "name": gname,
            "percent": f"{percent_human:.2f}" if percent_human > 0 else "",
            "due_days": str(due_days) if due_days else "",
            "is_cash": is_cash
        })

    return templates.TemplateResponse(
        "group_config.html",
        {
            "request": request,
            "active_tab": "config",
            "title": "تعریف گروه‌های کالا (پیش‌فرض)",
            "group_rows": group_rows,
            "success_message": "تنظیمات گروه‌های کالا با موفقیت ذخیره شد ✅" if success else None,
            "error_message": "هیچ ردیف معتبری برای ذخیره وارد نشده است." if not success and not rows_data else None
        }
    )


# ------------------ UI: تب ۳ – تخصیص کالا به گروه ------------------ #

@app.get("/group-items")
async def group_items_page(request: Request):
    # 1. بارگذاری تنظیمات و مپ فعلی
    default_group_cfg = load_default_group_config()
    pg_map = load_product_group_map()

    # 2. ساخت دیکشنری کد → گروه از مپ فعلی
    code_to_group: dict[str, str] = {}
    if not pg_map.empty:
        for _, r in pg_map.iterrows():
            code = canonicalize_code(r.get("ProductCode"))
            grp = str(r.get("Group") or "").strip()
            if code and grp:
                code_to_group[code] = grp

    # 3. آماده‌سازی گزینه‌های منوی کشویی گروه
    group_options = []
    for gname, cfg in default_group_cfg.items():
        percent = (cfg.get("percent") or 0) * 100
        due_days = cfg.get("due_days")
        is_cash = cfg.get("is_cash", False)
        label_parts = [gname, f"{percent:.2f}٪"]
        if due_days is not None:
            label_parts.append(f"{due_days} روز")
        if is_cash:
            label_parts.append("نقدی")
        group_options.append({
            "value": gname,
            "label": " | ".join(label_parts)
        })

    # 4. بررسی وجود فایل فروش
    df_sales = LAST_UPLOAD["sales"]
    product_rows = []
    info_message = None
    info_type = None

    if df_sales is None:
        # حالت: فایل فروش آپلود نشده
        info_message = "هنوز هیچ فایل فروشی در تب «محاسبه پورسانت» آپلود نشده است. با این حال می‌توانی با دکمه «افزودن سطر جدید» در پایین جدول، کالاها را دستی اضافه کنی."
        info_type = "error"
    else:
        # 5. جستجوی ستون کد و نام کالا
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
            # حالت: ستون کد کالا پیدا نشد
            info_message = 'در فایل فروش، ستونی برای کد کالا پیدا نشد. لطفاً یکی از ستون‌ها را با نام‌هایی مثل <code>ProductCode</code>، <code>کد کالا</code> یا <code>کد محصول</code> ایجاد کن. همچنین می‌توانی کالاها را با دکمه «افزودن سطر جدید» به‌صورت دستی وارد کنی.'
            info_type = "error"
        else:
            # 6. ساخت پیام اطلاعاتی
            name_display = f"، نام: <b>{name_col}</b>" if name_col else ""
            info_message = f'منبع لیست کالاها، آخرین فایل فروش آپلود‌شده است (ستون کد: <b>{code_col}</b>{name_display}).<br/>اگر می‌خواهی موردی اضافه کنی که در فروش‌ها نیامده، می‌توانی از دکمهٔ «افزودن سطر جدید» استفاده کنی.'
            info_type = "info"

            # 7. استخراج لیست کالاها از فایل فروش
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

            # 8. ساخت لیست ردیف‌ها برای نمایش
            for _, row in df_items.iterrows():
                code_key = str(row["__CodeKey__"])
                name_val = str(row["__Name__"] or "")
                current_group = code_to_group.get(code_key, "")
                product_rows.append({
                    "code": code_key,
                    "name": name_val,
                    "current_group": current_group
                })

    # 9. آماده‌سازی HTML مپ فعلی
    current_map_html = None
    if not pg_map.empty:
        current_map_html = pg_map.to_html(index=False, border=0, classes="")

    # 10. رندر تمپلیت
    return templates.TemplateResponse(
        "group_items.html",
        {
            "request": request,
            "active_tab": "items",
            "title": "تخصیص کالا به گروه",
            "group_options": group_options,
            "product_rows": product_rows,
            "info_message": info_message,
            "info_type": info_type,
            "current_map_html": current_map_html
        }
    )

# ------------------ UI: تب جدید - رفع اشکال کدهای مشتری ------------------


@app.get("/fix-unresolved", response_class=HTMLResponse)
async def fix_unresolved_page(request: Request):
    import os

    file_path = "customer_codes_bind.xlsx"

    # بررسی وجود فایل
    if not os.path.exists(file_path):
        current_dir = os.getcwd()
        error_message = f"""
            فایل اکسل <b>customer_codes_bind.xlsx</b> یافت نشد.
            <br>
            مسیر جاری: {current_dir}
            <br><br>
            لطفاً ابتدا به سربرگ <a href="/bind-codes" style="font-weight:bold; text-decoration:underline;">عطف کد به مشتری</a> بروید و فایل را تولید کنید.
        """
        return templates.TemplateResponse("fix_unresolved.html", {
            "request": request,
            "active_nav": "fix",
            "error_message": error_message
        })

    try:
        df_bind = pd.read_excel(file_path)

        # بررسی ستون‌ها
        required_cols = ["CustomerName", "CustomerCode", "Status"]
        missing_cols = [
            col for col in required_cols if col not in df_bind.columns]
        if missing_cols:
            return templates.TemplateResponse("error.html", {
                "request": request,
                "active_nav": "fix",
                "error_title": "خطا در ساختار فایل اکسل",
                "error_message": f"ستون‌های زیر یافت نشدند: {', '.join(missing_cols)}"
            })

        # خواندن لیست سیاه
        blacklist_set = set()
        blacklist_path = "blacklist.xlsx"
        if os.path.exists(blacklist_path):
            try:
                df_black = pd.read_excel(blacklist_path)
                if "CustomerName" in df_black.columns:
                    blacklist_set = set(
                        df_black["CustomerName"].apply(normalize_persian_name)
                    )
            except Exception as e:
                print(f"Error loading blacklist for UI: {e}")

        # جدا کردن یافت شده و یافت نشده
        unresolved_df = df_bind[df_bind["CustomerCode"] == "یافت نشد"].copy()
        resolved_df = df_bind[df_bind["CustomerCode"] != "یافت نشد"].copy()

        # آماده‌سازی لیست موارد یافت نشده
        unresolved_items = []
        for _, row in unresolved_df.iterrows():
            unresolved_items.append({
                "name": row.get("CustomerName", "")
            })

        # آماده‌سازی لیست موارد یافت شده
        resolved_items = []
        for _, row in resolved_df.iterrows():
            name = row.get("CustomerName", "")
            code = row.get("CustomerCode", "")
            norm_name = normalize_persian_name(name)
            is_blacklisted = norm_name in blacklist_set

            resolved_items.append({
                "name": name,
                "code": code,
                "is_blacklisted": is_blacklisted
            })

        return templates.TemplateResponse("fix_unresolved.html", {
            "request": request,
            "active_nav": "fix",
            "total_rows": len(df_bind),
            "unresolved_count": len(unresolved_df),
            "resolved_count": len(resolved_df),
            "unresolved_items": unresolved_items,
            "resolved_items": resolved_items
        })

    except Exception as e:
        print(f"DEBUG ERROR: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "active_nav": "fix",
            "error_title": "خطا در خواندن فایل اکسل",
            "error_message": str(e)
        })


@app.post("/manual-map-save")
async def manual_map_save(request: Request):
    try:
        body = await request.json()
        new_mappings = body

        file_path = "customer_codes_bind.xlsx"

        # خواندن فایل اکسل موجود
        if os.path.exists(file_path):
            df_existing = pd.read_excel(file_path)
        else:
            df_existing = pd.DataFrame(
                columns=["CustomerName", "CustomerCode",
                         "TotalAmount", "Status"]
            )

        # تبدیل داده‌های جدید به دیتافریم
        df_new = pd.DataFrame(new_mappings)
        df_new["Status"] = "کد یافت شد (دستی)"

        # حذف ردیف‌های قدیمی که نام مشتری‌شان در لیست جدید وجود دارد
        if not df_existing.empty and "CustomerName" in df_existing.columns:
            df_existing = df_existing[~df_existing["CustomerName"].isin(
                df_new["CustomerName"])]

        # ادغام دیتافریم قدیمی و جدید
        df_final = pd.concat([df_existing, df_new], ignore_index=True)

        # ذخیره در فایل اکسل
        df_final.to_excel(file_path, index=False)

        return JSONResponse(content={"status": "ok", "message": "فایل با موفقیت بروزرسانی شد."})

    except Exception as e:
        print(f"Error saving map: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/edit-resolved-item")
async def edit_resolved_item(request: Request):
    """
    ویرایش یک مشتری در لیست کدهای یافت شده (فایل customer_codes_bind.xlsx).
    """
    try:
        body = await request.json()
        old_name = body.get("old_name")
        new_name = body.get("new_name")
        new_code = body.get("new_code")

        if not old_name or not new_name or not new_code:
            return JSONResponse(content={"status": "error", "message": "اطلاعات ناقص است"}, status_code=400)

        file_path = "customer_codes_bind.xlsx"

        if os.path.exists(file_path):
            df = pd.read_excel(file_path)

            # پیدا کردن و ویرایش ردیف
            # فرض بر این است که old_name منحصر به فرد است یا اولین مورد را ویرایش می‌کنیم
            mask = (df["CustomerName"] == old_name)

            if not mask.any():
                return JSONResponse(content={"status": "error", "message": "مشتری یافت نشد"}, status_code=404)

            # به‌روزرسانی نام و کد
            df.loc[mask, "CustomerName"] = new_name
            df.loc[mask, "CustomerCode"] = new_code
            df.loc[mask, "Status"] = "کد یافت شد (ویرایش شده)"

            df.to_excel(file_path, index=False)
            return JSONResponse(content={"status": "ok"})
        else:
            return JSONResponse(content={"status": "error", "message": "فایل اکسل یافت نشد"}, status_code=404)

    except Exception as e:
        print(f"Error editing resolved item: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/delete-resolved-item")
async def delete_resolved_item(request: Request):
    """
    حذف یک مشتری از لیست کدهای یافت شده (فایل customer_codes_bind.xlsx).
    """
    try:
        body = await request.json()
        customer_name = body.get("customer_name")

        if not customer_name:
            return JSONResponse(content={"status": "error", "message": "نام مشتری ارسال نشده است"}, status_code=400)

        file_path = "customer_codes_bind.xlsx"

        if os.path.exists(file_path):
            df = pd.read_excel(file_path)

            # فیلتر کردن برای حذف ردیف مورد نظر
            initial_len = len(df)
            df = df[df["CustomerName"] != customer_name]

            if len(df) == initial_len:
                return JSONResponse(content={"status": "error", "message": "مشتری یافت نشد"}, status_code=404)

            df.to_excel(file_path, index=False)
            return JSONResponse(content={"status": "ok"})
        else:
            return JSONResponse(content={"status": "error", "message": "فایل اکسل یافت نشد"}, status_code=404)

    except Exception as e:
        print(f"Error deleting resolved item: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/group-items-save", response_class=HTMLResponse)
async def group_items_save(request: Request):
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
        new_rows.append({
            "ProductCode": code_key,
            "ProductName": name_val,
            "Group": grp_name,
        })

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

    # تعیین موفقیت یا عدم موفقیت
    success = not df_all.empty

    if success:
        save_product_group_map(df_all)

    # برای نمایش، دوباره مپ را بخوانیم
    pg_map = load_product_group_map()
    map_html = None
    if not pg_map.empty:
        map_html = pg_map.to_html(index=False, border=0, classes="data-table")

    return templates.TemplateResponse("group_items_save.html", {
        "request": request,
        "active_nav": "items",
        "success": success,
        "map_html": map_html
    })

# ------------------ UI: دانلود مستقیم اکسل کدها ------------------

# ------------------ UI: سربرگ جدید - عطف کد به مشتری ------------------

# ==========================================
# 1. Bind Codes Functions (عطف کد به مشتری)
# ==========================================


@app.get("/bind-codes", response_class=HTMLResponse)
async def bind_codes_page(request: Request):
    return templates.TemplateResponse(
        "bind_codes.html",
        {
            "request": request,
            "title": "عطف کد به مشتری",
            "active_tab": "bind"
        }
    )


@app.post("/process-bind-codes", response_class=HTMLResponse)
async def process_bind_codes(
    request: Request,
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None)
):
    try:
        # 1. بارگذاری فایل‌ها
        df_pay = load_payments_excel(payments_file.file)
        df_chk = pd.DataFrame()
        if checks_file and checks_file.filename:
            df_chk = load_checks_excel(checks_file.file)

        # لیست سیاه
        blacklist_set = set()
        blacklist_path = "blacklist.xlsx"
        if os.path.exists(blacklist_path):
            try:
                df_black = pd.read_excel(blacklist_path)
                if "CustomerName" in df_black.columns:
                    blacklist_set = set(
                        df_black["CustomerName"].apply(normalize_persian_name))
            except Exception as e:
                print(f"Error loading blacklist: {e}")

        # 2. ساخت مپ
        name_code_map_from_balances = build_name_code_map_from_balances()

        # 3. آماده‌سازی پرداخت‌ها
        payments_df, unresolved_items = prepare_payments(
            df_pay, df_chk, pd.DataFrame()
        )

        # فیلتر لیست سیاه
        resolved_df = payments_df[payments_df["ResolvedCustomer"].notna()].copy(
        )
        resolved_df = resolved_df[resolved_df["ResolvedCustomer"]
                                  != "یافت نشد"]

        if not resolved_df.empty:
            resolved_df = resolved_df[
                ~resolved_df["CustomerName"].apply(
                    lambda x: normalize_persian_name(x) in blacklist_set)
            ]

        if unresolved_items:
            unresolved_df = pd.DataFrame(unresolved_items)
            unresolved_df = unresolved_df[
                ~unresolved_df["Name"].apply(
                    lambda x: normalize_persian_name(x) in blacklist_set)
            ]
        else:
            unresolved_df = pd.DataFrame()

        # 4. ساخت دیتافریم نتیجه
        current_result_data = []

        if not resolved_df.empty:
            grouped = resolved_df.groupby("ResolvedCustomer").agg({
                "CustomerName": "first",
                "Amount": "sum"
            }).reset_index()
            for _, row in grouped.iterrows():
                current_result_data.append({
                    "CustomerName": row["CustomerName"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": row["ResolvedCustomer"],
                    "Status": "کد یافت شد"
                })

        if not unresolved_df.empty:
            grouped_unresolved = unresolved_df.groupby("Name").agg({
                "Amount": "sum"
            }).reset_index()
            for _, row in grouped_unresolved.iterrows():
                current_result_data.append({
                    "CustomerName": row["Name"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": "یافت نشد",
                    "Status": "کد یافت نشد"
                })

        df_current = pd.DataFrame(current_result_data)

        # 5. منطق ادغام
        output_filename = "customer_codes_bind.xlsx"
        df_existing = pd.DataFrame()
        if os.path.exists(output_filename):
            df_existing = pd.read_excel(output_filename)

        newly_added = []
        updated_codes = []

        if not df_current.empty:
            for _, row in df_current.iterrows():
                name = row["CustomerName"]
                new_code = row["CustomerCode"]

                if not df_existing.empty:
                    existing_row = df_existing[df_existing["CustomerName"] == name]
                else:
                    existing_row = pd.DataFrame()

                if existing_row.empty:
                    newly_added.append(name)
                    df_existing = pd.concat(
                        [df_existing, pd.DataFrame([row])], ignore_index=True)
                else:
                    old_code = existing_row.iloc[0]["CustomerCode"]
                    if old_code == "یافت نشد" and new_code != "یافت نشد":
                        updated_codes.append(
                            f"{name} (کد قبلی: یافت نشد -> کد جدید: {new_code})")
                        df_existing.loc[df_existing["CustomerName"]
                                        == name, "CustomerCode"] = new_code
                        df_existing.loc[df_existing["CustomerName"]
                                        == name, "Status"] = "کد یافت شد (بروزرسانی)"
                    elif old_code != "یافت نشد" and new_code != "یافت نشد" and old_code != new_code:
                        updated_codes.append(
                            f"{name} (کد قبلی: {old_code} -> کد جدید: {new_code})")
                        df_existing.loc[df_existing["CustomerName"]
                                        == name, "CustomerCode"] = new_code
                        df_existing.loc[df_existing["CustomerName"]
                                        == name, "Status"] = "کد تغییر یافت"

        df_existing.to_excel(output_filename, index=False)

        return templates.TemplateResponse(
            "bind_codes_result.html",
            {
                "request": request,
                "title": "نتیجه عطف کد",
                "active_tab": "bind",
                "newly_added": newly_added,
                "updated_codes": updated_codes
            }
        )

    except Exception as e:
        print(f"Error in bind codes: {e}")
        return HTMLResponse(content=f"<h1>خطا در پردازش: {str(e)}</h1>", status_code=500)


@app.get("/download-bind-file")
async def download_bind_file():
    output_filename = "customer_codes_bind.xlsx"
    if not os.path.exists(output_filename):
        return HTMLResponse(content="<h1>فایل یافت نشد. لطفاً ابتدا فایل را بسازید.</h1>")
    return FileResponse(
        output_filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=output_filename
    )


# ==========================================
# 2. Direct Download Functions (دانلود مستقیم)
# ==========================================

@app.post("/process-direct-download", response_class=HTMLResponse)
async def process_direct_download(
    request: Request,
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None)
):
    try:
        # 1. بارگذاری
        df_pay = load_payments_excel(payments_file.file)
        df_chk = pd.DataFrame()
        if checks_file and checks_file.filename:
            df_chk = load_checks_excel(checks_file.file)

        # 2. آماده‌سازی
        payments_df, unresolved_items = prepare_payments(
            df_pay, df_chk, pd.DataFrame()
        )

        # 3. ساخت دیتافریم
        result_data = []
        resolved_df = payments_df[payments_df["ResolvedCustomer"].notna()].copy(
        )
        resolved_df = resolved_df[resolved_df["ResolvedCustomer"]
                                  != "یافت نشد"]

        if not resolved_df.empty:
            grouped = resolved_df.groupby("ResolvedCustomer").agg({
                "CustomerName": "first",
                "Amount": "sum"
            }).reset_index()
            for _, row in grouped.iterrows():
                result_data.append({
                    "CustomerName": row["CustomerName"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": row["ResolvedCustomer"],
                    "Status": "کد یافت شد"
                })

        if unresolved_items:
            unresolved_df = pd.DataFrame(unresolved_items)
            grouped_unresolved = unresolved_df.groupby("Name").agg({
                "Amount": "sum"
            }).reset_index()
            for _, row in grouped_unresolved.iterrows():
                result_data.append({
                    "CustomerName": row["Name"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": "یافت نشد",
                    "Status": "کد یافت نشد"
                })

        df_result = pd.DataFrame(result_data)
        df_result.to_excel(OUTPUT_CODES_FILENAME, index=False)

        return templates.TemplateResponse(
            "direct_download_result.html",
            {
                "request": request,
                "title": "فایل اکسل ساخته شد",
                "active_tab": "main",
                "filename": OUTPUT_CODES_FILENAME
            }
        )

    except Exception as e:
        print(f"Error: {e}")
        return HTMLResponse(content=f"<h1>خطا در پردازش: {str(e)}</h1>", status_code=500)


@app.get("/download-generated-file")
async def download_generated_file():
    if not os.path.exists(OUTPUT_CODES_FILENAME):
        return HTMLResponse(content="<h1>فایل یافت نشد. لطفاً ابتدا فایل را بسازید.</h1>")

    return FileResponse(
        OUTPUT_CODES_FILENAME,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=OUTPUT_CODES_FILENAME
    )


# ==========================================
# 3. Marketers Functions (مدیریت بازاریاب‌ها)
# ==========================================

@app.get("/marketers", response_class=HTMLResponse)
async def marketers_page(request: Request):
    marketers_list = []
    if os.path.exists(MARKETERS_PATH):
        try:
            df = pd.read_excel(MARKETERS_PATH)
            col = next((c for c in df.columns if "marketer" in c.lower()
                       or "visitor" in c.lower() or "بازاریاب" in c), None)
            if col:
                marketers_list = df[col].dropna().tolist()
        except:
            pass

    return templates.TemplateResponse(
        "marketers.html",
        {
            "request": request,
            "title": "مدیریت بازاریاب‌ها",
            "active_tab": "marketers",
            "marketers_list": marketers_list
        }
    )


@app.post("/marketers/add")
async def add_marketer(request: Request):
    form = await request.form()
    new_name = form.get("new_marketer", "").strip()

    if new_name:
        current_list = []
        if os.path.exists(MARKETERS_PATH):
            try:
                df = pd.read_excel(MARKETERS_PATH)
                col = next((c for c in df.columns if "marketer" in c.lower(
                ) or "visitor" in c.lower() or "بازاریاب" in c), None)
                if col:
                    current_list = df[col].dropna().tolist()
            except:
                pass

        if new_name not in current_list:
            current_list.append(new_name)
            save_marketers_list(current_list)

    return RedirectResponse(url="/marketers", status_code=303)


@app.post("/marketers/delete")
async def delete_marketer(request: Request):
    form = await request.form()
    name_to_delete = form.get("marketer_name", "")

    if os.path.exists(MARKETERS_PATH):
        try:
            df = pd.read_excel(MARKETERS_PATH)
            col = next((c for c in df.columns if "marketer" in c.lower()
                       or "visitor" in c.lower() or "بازاریاب" in c), None)
            if col:
                df = df[df[col] != name_to_delete]
                df.to_excel(MARKETERS_PATH, index=False)
        except:
            pass

    return RedirectResponse(url="/marketers", status_code=303)


@app.post("/marketers/upload")
async def upload_marketers(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        col = next((c for c in df.columns if "marketer" in c.lower()
                   or "visitor" in c.lower() or "بازاریاب" in c), None)

        if col:
            clean_list = df[col].dropna().unique().tolist()
            save_marketers_list(clean_list)
    except Exception as e:
        print(f"Error uploading marketers: {e}")

    return RedirectResponse(url="/marketers", status_code=303)

# ==========================================
# 4. Product Blacklist Functions (لیست سیاه کالا)
# ==========================================


@app.get("/product-blacklist", response_class=HTMLResponse)
async def view_product_blacklist(request: Request):
    # 1. لود مپ کالا
    try:
        df_map = load_product_group_map()
        if not df_map.empty:
            df_map["ProductCode"] = df_map["ProductCode"].apply(
                canonicalize_code)
    except Exception:
        df_map = pd.DataFrame(columns=["ProductCode", "ProductName"])

    # 2. آماده‌سازی پیشنهادات
    product_suggestions = []
    if not df_map.empty:
        df_sorted = df_map.sort_values(by="ProductName", na_position='last')
        product_suggestions = df_sorted[[
            "ProductCode", "ProductName"]].to_dict(orient="records")

    # 3. لود لیست سیاه
    blacklist_data = pd.DataFrame()
    if os.path.exists(PRODUCT_BLACKLIST_PATH):
        try:
            df_bl = pd.read_excel(PRODUCT_BLACKLIST_PATH)
            if not df_bl.empty:
                df_bl["ProductCode"] = df_bl["ProductCode"].apply(
                    canonicalize_code)
                blacklist_data = df_bl
        except Exception as e:
            print(f"Error loading blacklist: {e}")

    # 4. ترکیب داده‌ها
    final_list = []
    if not blacklist_data.empty:
        records = blacklist_data.to_dict(orient="records")

        for item in records:
            p_code = item.get("ProductCode", "")
            p_name_manual = item.get("ProductName", "")

            p_name_final = ""
            # اگر نام در لیست سیاه وجود دارد، از آن استفاده کن
            if pd.notna(p_name_manual) and str(p_name_manual).strip():
                p_name_final = str(p_name_manual).strip()
            # در غیر این صورت از مپ کالا بخوان
            else:
                if not df_map.empty:
                    match = df_map[df_map["ProductCode"] == p_code]
                    if not match.empty:
                        p_name_final = match.iloc[0]["ProductName"]

            item["DisplayName"] = p_name_final
            final_list.append(item)

    return templates.TemplateResponse(
        "product_blacklist.html",
        {
            "request": request,
            "title": "لیست سیاه کالا",
            "active_tab": "product-blacklist",
            "blacklist_data": final_list,
            "product_suggestions": product_suggestions
        }
    )


@app.post("/product-blacklist/add")
async def add_to_product_blacklist(request: Request):
    form = await request.form()
    code = form.get("code")

    if code:
        norm_code = canonicalize_code(code)
        if norm_code:
            current_set = load_product_blacklist_set()
            current_set.add(norm_code)
            save_product_blacklist(list(current_set))

    return RedirectResponse(url="/product-blacklist", status_code=303)


@app.post("/product-blacklist/delete")
async def delete_from_product_blacklist(request: Request):
    form = await request.form()
    code_to_del = form.get("code")

    if code_to_del:
        norm_del = canonicalize_code(code_to_del)
        current_set = load_product_blacklist_set()
        if norm_del in current_set:
            current_set.remove(norm_del)
            save_product_blacklist(list(current_set))
    return RedirectResponse(url="/product-blacklist", status_code=303)


@app.post("/product-blacklist/upload")
async def upload_product_blacklist(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        df_new = pd.read_excel(io.BytesIO(contents))
        target_col = None
        for c in df_new.columns:
            if "code" in str(c).lower() or "کد" in str(c):
                target_col = c
                break

        if target_col:
            new_codes = set()
            for val in df_new[target_col]:
                c = canonicalize_code(val)
                if c:
                    new_codes.add(c)
            save_product_blacklist(list(new_codes))

    except Exception as e:
        print(f"Upload Error: {e}")

    return RedirectResponse(url="/product-blacklist", status_code=303)


# ==========================================
# 5. Blacklist JSON APIs (عملیات لیست سیاه مشتری)
# ==========================================

@app.post("/blacklist-item")
async def blacklist_item(request: Request):
    try:
        body = await request.json()
        customer_name = body.get("customer_name")

        if not customer_name:
            return JSONResponse(content={"status": "error", "message": "نام مشتری ارسال نشده است"}, status_code=400)

        bind_file_path = "customer_codes_bind.xlsx"
        blacklist_file_path = "blacklist.xlsx"

        # حذف از فایل اصلی
        if os.path.exists(bind_file_path):
            df_bind = pd.read_excel(bind_file_path)
            initial_len = len(df_bind)
            df_bind = df_bind[df_bind["CustomerName"] != customer_name]

            if len(df_bind) < initial_len:
                df_bind.to_excel(bind_file_path, index=False)
            else:
                return JSONResponse(content={"status": "error", "message": "مشتری در لیست اصلی یافت نشد"}, status_code=404)
        else:
            return JSONResponse(content={"status": "error", "message": "فایل لیست اصلی یافت نشد"}, status_code=404)

        # افزودن به لیست سیاه
        if os.path.exists(blacklist_file_path):
            df_black = pd.read_excel(blacklist_file_path)
        else:
            df_black = pd.DataFrame(columns=["CustomerName", "DateAdded"])

        if not df_black.empty and "CustomerName" in df_black.columns:
            if customer_name in df_black["CustomerName"].values:
                return JSONResponse(content={"status": "ok", "message": "قبلاً در لیست سیاه وجود داشت."})

        new_row = pd.DataFrame([{
            "CustomerName": customer_name,
            "DateAdded": pd.Timestamp.now()
        }])
        df_black = pd.concat([df_black, new_row], ignore_index=True)
        df_black.to_excel(blacklist_file_path, index=False)

        return JSONResponse(content={"status": "ok", "message": "با موفقیت به لیست سیاه منتقل شد."})

    except Exception as e:
        print(f"Error blacklisting item: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.post("/unblacklist-item")
async def unblacklist_item(request: Request):
    try:
        body = await request.json()
        customer_name = body.get("customer_name")
        if not customer_name:
            return JSONResponse(content={"status": "error", "message": "نام مشتری ارسال نشده است"}, status_code=400)

        blacklist_file_path = "blacklist.xlsx"

        if os.path.exists(blacklist_file_path):
            df_black = pd.read_excel(blacklist_file_path)
            initial_len = len(df_black)
            norm_target = normalize_persian_name(customer_name)

            if "CustomerName" in df_black.columns:
                df_black["Normalized"] = df_black["CustomerName"].apply(
                    normalize_persian_name)
                df_black = df_black[df_black["Normalized"] != norm_target]
                df_black = df_black.drop(columns=["Normalized"])

            if len(df_black) < initial_len:
                df_black.to_excel(blacklist_file_path, index=False)
                return JSONResponse(content={"status": "ok", "message": "با موفقیت از لیست سیاه حذف شد."})
            else:
                return JSONResponse(content={"status": "error", "message": "مشتری در لیست سیاه یافت نشد"}, status_code=404)
        else:
            return JSONResponse(content={"status": "error", "message": "فایل لیست سیاه یافت نشد"}, status_code=404)

    except Exception as e:
        print(f"Error unblacklisting item: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)
