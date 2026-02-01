# app/api/routes_balances.py
from app.services.checks_excel_loader import load_checks_excel  # ایمپورت تابع جدید
from app.services.customer_balances import CHECKS_DB_PATH, normalize_name
import os
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
    save_raw_checks_file  # تابع جدید ایمپورت شد
)
from app.services.helpers import canonicalize_code

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ------------------ صفحه مدیریت مانده مشتریان ------------------ #


@router.get("/customer-balances", response_class=HTMLResponse)
async def customer_balances_page(request: Request):
    current_data = load_balances_from_db()

    processed_data = []
    if current_data:
        for item in current_data:
            code = item.get("CustomerCode", "")
            display_code = int(float(code)) if code and str(
                code) != 'nan' else ""

            name = item.get("OriginalName", item.get("CustomerName", ""))
            balance = item.get("Balance", 0)

            processed_data.append({
                "raw_code": code,
                "display_code": display_code,
                "name": name,
                "balance": balance,
                "balance_fmt": f"{balance:,.0f}",
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

    # دریافت فایل مانده حساب
    balances_file = form.get("balances_file")
    # دریافت فایل چک‌ها
    checks_file = form.get("checks_file")

    # بررسی فایل مانده‌ها
    if not balances_file or not balances_file.filename:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "فایل مانده حساب انتخاب نشده است.",
            "back_link": "/customer-balances"
        })

    # بررسی فایل چک‌ها
    if not checks_file or not checks_file.filename:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "فایل چک‌ها انتخاب نشده است.",
            "back_link": "/customer-balances"
        })

    # 1. پردازش فایل مانده‌ها (طبق روال قبل)
    new_items = load_balances_from_excel(balances_file.file)

    if not new_items:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error_message": "نتوانستیم داده‌ای از فایل مانده‌ها استخراج کنیم. ساختار فایل را بررسی کنید.",
            "back_link": "/customer-balances"
        })

    # به‌روزرسانی دیتابیس مانده‌ها
    update_balances(new_items)

    # 2. ذخیره فایل چک‌ها (فعلا فقط ذخیره می‌کنیم)
    save_success = save_raw_checks_file(checks_file.file)
    if not save_success:
        print("Warning: Failed to save checks file.")

    # ریدایرکت به صفحه نمایش
    return RedirectResponse(url="/customer-balances", status_code=303)


@router.post("/edit-balance")
async def edit_balance(request: Request):
    form = await request.form()
    old_name = form.get("old_name")
    new_code = form.get("code")
    new_name = form.get("name")
    new_balance_str = form.get("balance")

    current_data = load_balances_from_db()

    updated_data = []
    found = False
    for item in current_data:
        if item.get("CustomerName") == old_name:
            found = True
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

        should_delete = False
        if code:
            if item_code == str(code):
                should_delete = True
        elif name:
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
    """
    empty_df = pd.DataFrame(
        columns=["CustomerCode", "CustomerName", "OriginalName", "Balance"])
    save_balances_to_db(empty_df)
    return JSONResponse(content={"status": "ok"})

# در ابتدای فایل این‌ها را اگر ندارید اضافه کنید:


@router.get("/debug-checks", response_class=HTMLResponse)
async def debug_checks_page():
    if not os.path.exists(CHECKS_DB_PATH):
        return "<h1>هیچ فایل چکی در سیستم ذخیره نشده است. ابتدا فایل آپلود کنید.</h1>"

    try:
        with open(CHECKS_DB_PATH, "rb") as f:
            df = load_checks_excel(f)

        if df.empty:
            return "<h1>فایل خوانده شد اما خالی است.</h1>"

        html_content = """
        <html>
        <head>
            <style>
                body { font-family: Tahoma, sans-serif; direction: rtl; padding: 20px; }
                table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 14px; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: center; }
                th { background-color: #f2f2f2; }
                .pending { background-color: #ffe6e6; font-weight: bold; } /* قرمز برای در جریان */
                .passed { background-color: #e6ffe6; color: #aaa; } /* سبز کمرنگ برای پاس شده */
                .important-col { background-color: #fff3cd; } /* زرد برای ستون مهم */
            </style>
        </head>
        <body>
            <h2>دیباگ فایل چک‌ها (اصلاح شده)</h2>
            <p>ستون زرد رنگ <b>(صاحب حساب)</b> اکنون مبنای تطبیق با لیست بدهکاران است.</p>
            
            <table>
                <thead>
                    <tr>
                        <th class="important-col">صاحب حساب (مبنای جدید)</th>
                        <th>نام نرمال شده</th>
                        <th>نام طرف حساب (قدیم)</th>
                        <th>مبلغ</th>
                        <th>وضعیت</th>
                        <th>تشخیص سیستم</th>
                    </tr>
                </thead>
                <tbody>
        """

        # اولویت با CustomerName است
        name_col = "CustomerName" if "CustomerName" in df.columns else "AccountName"

        for _, row in df.iterrows():
            status = str(row.get("Status", ""))

            # نامی که الان سیستم استفاده می‌کند
            main_name = str(row.get(name_col, "---"))
            # نامی که قبلاً استفاده می‌شد (جهت مقایسه)
            other_name = str(row.get("AccountName", "---"))

            amount = row.get("Amount", 0)

            is_pending = "در جریان" in status or "در جريان" in status
            norm_name = normalize_name(main_name)

            bg_class = "pending" if is_pending else "passed"
            status_detect = "✅ کسر می‌شود" if is_pending else "نادیده"

            html_content += f"""
                <tr class="{bg_class}">
                    <td class="important-col"><b>{main_name}</b></td>
                    <td>{norm_name}</td>
                    <td>{other_name}</td>
                    <td>{amount:,.0f}</td>
                    <td>{status}</td>
                    <td>{status_detect}</td>
                </tr>
            """

        html_content += """
                </tbody>
            </table>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)

    except Exception as e:
        return f"<h1>Error: {str(e)}</h1>"
