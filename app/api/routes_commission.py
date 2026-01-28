# app/api/routes_commission.py
from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import pandas as pd
from fastapi.templating import Jinja2Templates
import json

# ایمپورت سرویس‌ها و هلپرها
from app.services.sales_excel_loader import load_sales_excel
from app.services.payments_excel_loader import load_payments_excel
from app.services.checks_excel_loader import load_checks_excel
from app.services.commission_service import (
    load_default_group_config,
    load_product_group_map,
    save_product_group_map,
    prepare_sales,
    compute_commissions,
    prepare_payments,
    build_name_code_mapping,
    build_name_code_map_from_balances,
    load_name_code_map_from_excel,
    extract_customer_for_payment
)
from app.services.helpers import (
    canonicalize_code,
    to_jalali_str,
    format_number
)
from app.services.customer_balances import load_balances_from_db
from app.state import LAST_UPLOAD, SESSION_SETTINGS

# تعریف روتر
router = APIRouter()

# تنظیمات تمپلیت (می‌توانید این را از main.py پاس دهید، اما فعلاً اینجا تعریف می‌کنیم)
templates = Jinja2Templates(directory="templates")

# متغیر سراسری برای نگهداری وضعیت آپلود
LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    "group_col": None,
    "group_config": None,
    "sales_result": None,
    "payments_result": None,
    "history": None,
}
# بعد از تعریف LAST_UPLOAD این را اضافه کنید:
SESSION_SETTINGS = {
    "reactivation_days": 95  # مقدار پیش‌فرض
}
# ------------------ صفحه اصلی ------------------ #


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "سیستم جامع فروش",
            "active_tab": "main"
        },
    )

# ------------------ آپلود فایل‌ها ------------------ #


@router.post("/upload-all", response_class=HTMLResponse)
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

# ------------------ محاسبه پورسانت ------------------ #


@router.post("/calculate-commission", response_class=HTMLResponse)
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

# ------------------ آمار مشتری (نمودار) ------------------ #


@router.get("/customer-stats")
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
