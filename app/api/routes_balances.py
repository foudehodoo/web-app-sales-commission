# app/api/routes_balances.py
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pandas as pd

# ایمپورت سرویس‌ها
from app.services.customer_balances import (
    load_balances_from_excel,
    save_balances_to_db,
    load_balances_from_db,
    update_balances,
    normalize_name as normalize_balance_name,
)
from app.services.helpers import canonicalize_code

# تعریف روتر
router = APIRouter()

# تنظیمات تمپلیت
templates = Jinja2Templates(directory="templates")

# ------------------ صفحه مدیریت مانده مشتریان ------------------ #


@router.get("/customer-balances", response_class=HTMLResponse)
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


@router.post("/upload-balances", response_class=HTMLResponse)
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


@router.post("/edit-balance")
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


@router.post("/add-balance")
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


@router.post("/delete-balance")
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


@router.post("/clear-balances")
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
