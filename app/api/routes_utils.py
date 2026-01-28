# app/api/routes_utils.py
from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import io
import os

# ایمپورت سرویس‌ها
from app.services.commission_service import (
    load_default_group_config,
    load_product_group_map,
    save_product_group_map,
    load_blacklist_sets,
    load_product_blacklist_set,
    save_product_blacklist,
    load_allowed_marketers,
    save_marketers_list,
    prepare_payments,
    build_name_code_map_from_balances,
    load_name_code_map_from_excel,
    extract_customer_for_payment
)
from app.services.helpers import (
    canonicalize_code,
    normalize_persian_name
)
from app.services.payments_excel_loader import load_payments_excel
from app.services.checks_excel_loader import load_checks_excel
from app.state import LAST_UPLOAD

# تعریف روتر
router = APIRouter()

# تنظیمات تمپلیت
templates = Jinja2Templates(directory="templates")

# مسیر فایل‌ها
BLACKLIST_FILE = "blacklist.xlsx"
MARKETERS_PATH = "marketers.xlsx"
PRODUCT_BLACKLIST_PATH = "product_blacklist.xlsx"
OUTPUT_CODES_FILENAME = "customer_codes_generated.xlsx"
DEFAULT_GROUP_CONFIG_PATH = "group_config.xlsx"

# ============================================================
# 1. مدیریت بلک‌لیست مشتریان
# ============================================================


@router.get("/blacklist", response_class=HTMLResponse)
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


@router.post("/upload-blacklist")
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

# ============================================================
# 2. مدیریت بازاریاب‌ها
# ============================================================


@router.get("/marketers", response_class=HTMLResponse)
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


@router.post("/marketers/add")
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


@router.post("/marketers/delete")
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


@router.post("/marketers/upload")
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

# ============================================================
# 3. مدیریت لیست سیاه کالا (Product Blacklist)
# ============================================================


@router.get("/product-blacklist", response_class=HTMLResponse)
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


@router.post("/product-blacklist/add")
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


@router.post("/product-blacklist/delete")
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


@router.post("/product-blacklist/upload")
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

# ============================================================
# 4. تنظیمات گروه‌های کالا (پیش‌فرض)
# ============================================================


@router.get("/group-config")
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


@router.post("/group-config")
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

# ============================================================
# 5. تخصیص کالا به گروه
# ============================================================


@router.get("/group-items")
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


@router.post("/group-items-save", response_class=HTMLResponse)
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

# ============================================================
# 6. عطف کد به مشتری (Bind Codes)
# ============================================================


@router.get("/bind-codes", response_class=HTMLResponse)
async def bind_codes_page(request: Request):
    return templates.TemplateResponse(
        "bind_codes.html",
        {
            "request": request,
            "title": "عطف کد به مشتری",
            "active_tab": "bind"
        }
    )


@router.post("/process-bind-codes", response_class=HTMLResponse)
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


@router.get("/download-bind-file")
async def download_bind_file():
    output_filename = "customer_codes_bind.xlsx"
    if not os.path.exists(output_filename):
        return HTMLResponse(content="<h1>فایل یافت نشد. لطفاً ابتدا فایل را بسازید.</h1>")
    return FileResponse(
        output_filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=output_filename
    )

# ============================================================
# 7. دانلود مستقیم اکسل
# ============================================================


@router.post("/process-direct-download", response_class=HTMLResponse)
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


@router.get("/download-generated-file")
async def download_generated_file():
    if not os.path.exists(OUTPUT_CODES_FILENAME):
        return HTMLResponse(content="<h1>فایل یافت نشد. لطفاً ابتدا فایل را بسازید.</h1>")

    return FileResponse(
        OUTPUT_CODES_FILENAME,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=OUTPUT_CODES_FILENAME
    )

# ============================================================
# 8. رفع اشکال کدهای مشتری (Fix Unresolved)
# ============================================================


@router.get("/fix-unresolved", response_class=HTMLResponse)
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


@router.post("/manual-map-save")
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


@router.post("/edit-resolved-item")
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


@router.post("/delete-resolved-item")
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


# ============================================================
# 9. APIهای لیست سیاه (JSON)
# ============================================================


@router.post("/blacklist-item")
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


@router.post("/unblacklist-item")
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

# ============================================================
# 10. آپلود پرداخت و چک برای استخراج کد (Extract Codes)
# ============================================================


@router.get("/upload-payments-checks")
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


@router.post("/process-payments-checks")
async def process_payments_checks(
    request: Request,
    payments_file: UploadFile = File(...),
    checks_file: UploadFile | None = File(None)
):
    try:

        df_pay = load_payments_excel(payments_file.file)
        df_chk = pd.DataFrame()
        if checks_file and checks_file.filename:
            df_chk = load_checks_excel(checks_file.file)

        # 2. ساخت مپ نام به کد از دیتابیس مانده‌ها
        name_code_map_from_balances = build_name_code_map_from_balances()

        # 3. آماده‌سازی پرداخت‌ها
        payments_df, unresolved_items = prepare_payments(
            # sales_df اینجا خالی ارسال می‌شود چون فقط برای استخراج کد است
            df_pay, df_chk, pd.DataFrame()
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
                    "CustomerName": row["CustomerName"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": row["ResolvedCustomer"],
                    "Status": "کد یافت شد ✅"
                })

        # پردازش مواردی که کد پیدا نشد (Unresolved)
        if unresolved_items:
            unresolved_df = pd.DataFrame(unresolved_items)
            grouped_unresolved = unresolved_df.groupby(
                "Name").agg({"Amount": "sum"}).reset_index()
            for _, row in grouped_unresolved.iterrows():
                result_data.append({
                    "CustomerName": row["Name"],
                    "TotalAmount": row["Amount"],
                    "CustomerCode": "",
                    "Status": "کد یافت نشد ❌"
                })

        df_result = pd.DataFrame(result_data)

        # ذخیره موقت برای دانلود (می‌توانیم از یک متغیر سراسری دیگر استفاده کنیم یا در همینجا هندل کنیم)
        # برای سادگی، ما در اینجا فقط رندر می‌کنیم و دانلود را جداگانه هندل می‌کنیم
        # اما چون state نداریم، باید دیتافریم را در فایل موقت ذخیره کنیم یا در session
        # فعلاً فرض می‌کنیم کاربر فقط می‌خواهد ببیند. برای دانلود، در مرحله بعد فایل می‌سازیم.

        # ساخت HTML جدول
        table_html = "<p>داده‌ای برای نمایش وجود ندارد.</p>"
        if not df_result.empty:
            table_html = df_result.to_html(
                index=False, border=0, classes="data-table")

        # ذخیره در فایل موقت برای دانلود بعدی
        temp_filename = "temp_payments_codes.xlsx"
        df_result.to_excel(temp_filename, index=False)

        return templates.TemplateResponse(
            "process_payments_checks_result.html",
            {
                "request": request,
                "active_tab": "main",
                "table_html": table_html,
                "has_results": not df_result.empty,
                "temp_filename": temp_filename  # ارسال نام فایل به تمپلیت
            }
        )
    except Exception as e:
        print(f"Error processing payments/checks: {e}")
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": f"خطا در پردازش فایل‌ها: {str(e)}",
                "back_url": "/upload-payments-checks"
            }
        )


@router.get("/download-codes-excel")
async def download_codes_excel(request: Request):
    """
    دانلود فایل اکسل حاوی کدهای استخراج شده.
    """
    temp_filename = "temp_payments_codes.xlsx"

    if not os.path.exists(temp_filename):
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "message": "خطا: داده‌ای برای دانلود وجود ندارد. لطفاً ابتدا فایل‌ها را پردازش کنید.",
                "back_url": "/upload-payments-checks"
            }
        )

    return FileResponse(
        temp_filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="customer_codes_extracted.xlsx"
    )
