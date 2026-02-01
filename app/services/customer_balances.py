# app/services/customer_balances.py
import pandas as pd
import os
import re
import shutil
from typing import List, Dict

# لودر اختصاصی چک‌ها را ایمپورت می‌کنیم
from app.services.checks_excel_loader import load_checks_excel

# مسیر فایل‌های ذخیره شده
BALANCES_DB_PATH = "customer_balances_db.xlsx"
CHECKS_DB_PATH = "customer_checks_db.xlsx"

# ---------------------------------------------------------
# بخش ۱: توابع کمکی و پارسر فایل مانده (کد خودتان)
# ---------------------------------------------------------

# app/services/customer_balances.py


def normalize_name(name: str) -> str:
    """
    نام‌ها را استاندارد می‌کند تا فاصله‌ها، پرانتزها و حروف عربی/فارسی
    باعث عدم تطابق نشوند.
    """
    if name is None:
        return ""

    # تبدیل به رشته و حذف فاصله‌های اول و آخر
    n = str(name).strip()

    if not n or n.lower() == 'nan':
        return ""

    # 1. یکسان‌سازی حروف ی و ک
    n = n.replace("ي", "ی").replace("ك", "ک")

    # 2. تبدیل نیم‌فاصله (\u200c) و فاصله نشکن (\xa0) به فاصله معمولی
    n = n.replace("\u200c", " ")
    n = n.replace("\xa0", " ")

    # 3. جدا کردن پرانتزها از کلمات چسبیده
    # این کار باعث می‌شود "علی(اراک)" و "علی (اراک)" هر دو یک‌شکل شوند
    n = n.replace("(", " ( ").replace(")", " ) ")

    # 4. حذف تمام فاصله‌های تکراری (تبدیل چند فاصله به یک فاصله)
    while "  " in n:
        n = n.replace("  ", " ")

    return n.strip()


def load_balances_from_excel(file_path_or_buffer) -> list[dict]:
    """
    خواندن فایل اکسل مانده حساب (فرمت پیچیده دو ردیفه).
    این تابع فقط زمان آپلود استفاده می‌شود.
    """
    try:
        df_raw = pd.read_excel(file_path_or_buffer, header=None)
    except Exception as e:
        print(f"Error reading balances file: {e}")
        return []

    # ایندکس ردیف‌ها
    row_main_header = 4
    row_sub_header = 5

    def fix_yek(text):
        if text is None:
            return ""
        return str(text).replace("ي", "ی").replace("ك", "ک")

    # پیدا کردن ستون‌ها
    col_name = None
    col_code = None
    col_debit = None
    col_credit = None

    if len(df_raw) > row_sub_header:
        for c_idx in range(len(df_raw.columns)):
            val_sub = fix_yek(df_raw.iloc[row_sub_header, c_idx])
            if "شرح" in val_sub:
                col_name = c_idx
            clean_val = val_sub.strip().lower()
            if clean_val == "كد" or clean_val == "code":
                col_code = c_idx
            if "بدهكار" in val_sub or "بدهکار" in val_sub:
                col_debit = c_idx
            elif "بستانكار" in val_sub or "بستانکار" in val_sub:
                col_credit = c_idx

    if col_code is None and col_name is not None:
        col_code = col_name - 1

    if col_name is None:
        return []

    balances_list = []
    data_start_row = row_sub_header + 1

    for r_idx in range(data_start_row, len(df_raw)):
        raw_name = df_raw.iloc[r_idx, col_name]
        norm_name = normalize_name(raw_name)

        # === اصلاح ۱: نادیده گرفتن ردیف جمع از فایل ورودی ===
        if not norm_name or "جمع" in norm_name:
            continue
        # =================================================

        raw_code = ""
        if col_code is not None:
            val_code = df_raw.iloc[r_idx, col_code]
            if pd.notna(val_code):
                temp_str = str(val_code).strip()
                if temp_str.endswith(".0"):
                    raw_code = temp_str[:-2]
                else:
                    raw_code = temp_str

        debit_val = 0.0
        credit_val = 0.0

        if col_debit is not None:
            d_val = df_raw.iloc[r_idx, col_debit]
            if pd.notna(d_val):
                try:
                    debit_val = float(str(d_val).replace(
                        ",", "").replace("،", ""))
                except:
                    pass

        if col_credit is not None:
            c_val = df_raw.iloc[r_idx, col_credit]
            if pd.notna(c_val):
                try:
                    credit_val = float(str(c_val).replace(
                        ",", "").replace("،", ""))
                except:
                    pass

        net_balance = credit_val - debit_val

        balances_list.append({
            "CustomerCode": raw_code,
            "CustomerName": norm_name,
            "OriginalName": str(raw_name).strip(),
            "Balance": net_balance
        })

    return balances_list


def save_balances_to_db(data: list[dict]):
    """ذخیره لیست استاندارد شده در فایل دیتابیس اکسلی."""
    if not data:
        return
    df = pd.DataFrame(data)

    # ستون‌های مورد نیاز (RawBalance اضافه شد)
    cols = ["CustomerCode", "CustomerName", "OriginalName",
            "Balance", "RawBalance", "PendingChecks"]

    # ایجاد ستون‌های خالی اگر وجود ندارند
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # مرتب‌سازی ستون‌ها برای زیبایی فایل اکسل
    df = df[cols]

    try:
        df.to_excel(BALANCES_DB_PATH, index=False)
    except Exception as e:
        print(f"Error saving DB excel: {e}")


def update_balances(new_items: list[dict]):
    """آپدیت دیتابیس مانده‌ها با داده‌های جدید."""

    # خواندن دیتابیس فعلی
    current_data = []
    if os.path.exists(BALANCES_DB_PATH):
        try:
            current_data = pd.read_excel(
                BALANCES_DB_PATH).to_dict(orient="records")
        except:
            pass

    current_map = {str(item.get("CustomerName"))
                       : item for item in current_data}

    # جایگزینی یا افزودن
    for item in new_items:
        key = item["CustomerName"]

        # نکته مهم: وقتی از فایل مانده جدید می‌خوانیم، مقدار Balance در واقع همان RawBalance است
        # پس آن را به عنوان RawBalance ست می‌کنیم
        item["RawBalance"] = item["Balance"]

        current_map[key] = item

    # به جای ذخیره مستقیم، از تابع محاسبه‌گر استفاده می‌کنیم
    recalculate_and_save_db(list(current_map.values()))


# ---------------------------------------------------------
# بخش ۲: توابع جدید مربوط به چک‌ها
# ---------------------------------------------------------


def save_raw_checks_file(file_content_stream):
    """ذخیره فایل چک‌ها جهت پردازش‌های بعدی."""
    try:
        file_content_stream.seek(0)
        with open(CHECKS_DB_PATH, "wb") as buffer:
            shutil.copyfileobj(file_content_stream, buffer)
        return True
    except Exception as e:
        print(f"Error saving checks file: {e}")
        return False

# این تابع را جایگزین تابع قبلی get_pending_checks_deductions کنید


def get_pending_checks_deductions() -> dict[str, float]:
    """
    فایل چک‌های ذخیره شده را می‌خواند و مجموع چک‌های 'در جریان'
    را برای هر مشتری (بر اساس صاحب حساب) برمی‌گرداند.
    """
    deductions = {}

    # 1. بررسی و خواندن فایل چک
    if not os.path.exists(CHECKS_DB_PATH):
        return deductions

    try:
        with open(CHECKS_DB_PATH, "rb") as f:
            checks_df = load_checks_excel(f)
    except Exception as e:
        print(f"Error loading checks file: {e}")
        return deductions

    if checks_df.empty:
        return deductions

    # 2. منطق اصلی: اولویت با 'CustomerName' (صاحب حساب) است
    # اگر ستون صاحب حساب نبود، سراغ 'AccountName' (طرف حساب) می‌رویم
    target_col = "CustomerName" if "CustomerName" in checks_df.columns else "AccountName"

    # اطمینان از وجود ستون‌های ضروری
    if target_col not in checks_df.columns or "Status" not in checks_df.columns or "Amount" not in checks_df.columns:
        return deductions

    # 3. محاسبه کسورات
    for _, row in checks_df.iterrows():
        status = str(row["Status"])

        # بررسی وضعیت در جریان (شامل ی عربی و فارسی)
        if "در جریان" in status or "در جريان" in status:
            # خواندن نام از ستون تشخیص داده شده
            raw_name = str(row[target_col])
            amount = row["Amount"]

            norm_name = normalize_name(raw_name)

            if norm_name:
                if norm_name in deductions:
                    deductions[norm_name] += amount
                else:
                    deductions[norm_name] = amount

    return deductions

# ---------------------------------------------------------
# بخش ۳: تابع اصلی فراخوانی (ترکیب مانده و چک)
# ---------------------------------------------------------


def load_balances_from_db() -> list[dict]:
    """
    خواندن مانده‌ها جهت نمایش در UI.
    محاسبه دقیق جمع کل بر اساس داده‌های فیلتر شده.
    """
    if not os.path.exists(BALANCES_DB_PATH):
        return []

    try:
        df_bal = pd.read_excel(BALANCES_DB_PATH)
        df_bal = df_bal.where(pd.notnull(df_bal), None)
        raw_balances = df_bal.to_dict(orient="records")
    except Exception as e:
        print(f"Error loading balances DB: {e}")
        return []

    checks_map = get_pending_checks_deductions()

    final_list = []

    # متغیرهای جمع برای وب‌اپ
    sum_raw = 0.0
    sum_pending = 0.0
    sum_final_balance = 0.0

    for item in raw_balances:
        cust_name = str(item.get("CustomerName", ""))

        # نادیده گرفتن ردیف جمع موجود در فایل (خودمان دوباره می‌سازیم)
        if cust_name == "جمع":
            continue

        if item.get("RawBalance") is not None:
            raw_balance = float(item.get("RawBalance"))
        else:
            raw_balance = float(item.get("Balance", 0))

        pending_amount = checks_map.get(cust_name, 0.0)
        effective_balance = raw_balance - pending_amount

        new_item = item.copy()
        new_item["Balance"] = effective_balance
        new_item["RawBalance"] = raw_balance
        new_item["PendingChecks"] = pending_amount

        new_item["display_code"] = item.get("CustomerCode", "")
        new_item["name"] = item.get("OriginalName", cust_name)
        new_item["balance_fmt"] = "{:,.0f}".format(effective_balance)
        new_item["color"] = "#10b981" if effective_balance >= 0 else "#ef4444"

        # افزودن به جمع‌ها
        sum_raw += raw_balance
        sum_pending += pending_amount
        sum_final_balance += effective_balance

        final_list.append(new_item)

    # === اصلاح ۳: افزودن ردیف جمع محاسبه شده به انتهای لیست نمایشی ===
    summary_item = {
        "CustomerCode": "",
        "CustomerName": "جمع",
        "OriginalName": "جمع کل",
        "display_code": "",
        "name": "جمع کل",
        "RawBalance": sum_raw,
        "PendingChecks": sum_pending,
        "Balance": sum_final_balance,  # این عدد دقیقا جمع ستون مانده نهایی است
        "balance_fmt": "{:,.0f}".format(sum_final_balance),
        "color": "#000000"  # رنگ مشکی یا متمایز برای جمع
    }
    final_list.append(summary_item)
    # ===============================================================

    return final_list

# عملیات دستی (افزودن/حذف)


def add_customer_mapping(name: str, code: str, balance: float = 0):
    norm_name = normalize_name(name)
    clean_code = str(code).strip()

    # این تابع محاسبه شده برمیگرداند، اما برای ذخیره باید خام را آپدیت کنیم
    current = load_balances_from_db()
    # بهتر است فایل خام را مستقیم بخوانیم:
    if os.path.exists(BALANCES_DB_PATH):
        raw_data = pd.read_excel(BALANCES_DB_PATH).to_dict(orient="records")
    else:
        raw_data = []

    # آپدیت یا افزودن در لیست خام
    found = False
    for item in raw_data:
        if item.get("CustomerName") == norm_name:
            item["CustomerCode"] = clean_code
            item["OriginalName"] = name
            item["Balance"] = balance
            found = True
            break

    if not found:
        raw_data.append({
            "CustomerCode": clean_code,
            "CustomerName": norm_name,
            "OriginalName": name,
            "Balance": balance
        })

    save_balances_to_db(raw_data)
    return True


def save_raw_checks_file(file_content_stream):
    """ذخیره فایل چک‌ها و بروزرسانی بلافاصله فایل مانده‌ها."""
    try:
        file_content_stream.seek(0)
        with open(CHECKS_DB_PATH, "wb") as buffer:
            shutil.copyfileobj(file_content_stream, buffer)

        # ==> تغییر مهم: بلافاصله بعد از ذخیره چک، مانده‌ها را در دیتابیس بازنویسی می‌کنیم
        recalculate_and_save_db()

        return True
    except Exception as e:
        print(f"Error saving checks file: {e}")
        return False


def recalculate_and_save_db(data_list: list[dict] = None):
    """
    این تابع داده‌ها را می‌گیرد، چک‌های در جریان را از آن کم می‌کند
    و در فایل اکسل ذخیره می‌کند. همچنین ردیف جمع را محاسبه می‌کند.
    """
    if data_list is None:
        if os.path.exists(BALANCES_DB_PATH):
            try:
                df = pd.read_excel(BALANCES_DB_PATH)
                data_list = df.to_dict(orient="records")
            except:
                data_list = []
        else:
            data_list = []

    if not data_list:
        return

    checks_map = get_pending_checks_deductions()

    processed_data = []

    # متغیرهای محاسبه جمع کل
    total_raw = 0.0
    total_pending = 0.0
    total_balance = 0.0

    for item in data_list:
        norm_name = str(item.get("CustomerName", ""))

        # حذف ردیف‌های جمع قدیمی برای جلوگیری از دوبله شدن
        if norm_name == "جمع":
            continue

        # تعیین مانده خام
        raw_balance = item.get("RawBalance")
        if raw_balance is None or str(raw_balance) == "" or str(raw_balance) == "nan":
            raw_balance = float(item.get("Balance", 0))
        else:
            raw_balance = float(raw_balance)

        # محاسبه مبلغ چک
        pending_amount = checks_map.get(norm_name, 0.0)

        # مانده نهایی (پس از کسر چک)
        final_balance_for_excel = raw_balance - pending_amount

        # آپدیت آیتم
        item["RawBalance"] = raw_balance
        item["Balance"] = final_balance_for_excel
        item["PendingChecks"] = pending_amount

        # افزودن به جمع کل
        total_raw += raw_balance
        total_pending += pending_amount
        total_balance += final_balance_for_excel

        processed_data.append(item)

    # === اصلاح ۲: افزودن ردیف جمع نهایی به انتهای لیست ===
    # این باعث می‌شود فایل اکسل دارای یک ردیف جمع صحیح باشد
    processed_data.append({
        "CustomerCode": "",
        "CustomerName": "جمع",
        "OriginalName": "جمع کل",
        "RawBalance": total_raw,      # جمع ستون خام
        "PendingChecks": total_pending,  # جمع ستون چک‌ها
        # جمع ستون خالص (که مشکل شما را حل می‌کند)
        "Balance": total_balance
    })
    # ====================================================

    save_balances_to_db(processed_data)
