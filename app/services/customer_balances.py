# app/services/customer_balances.py
import pandas as pd
import os
import re

# مسیر فایل ذخیره مانده‌ها
BALANCES_DB_PATH = "customer_balances_db.xlsx"


def normalize_name(name: str) -> str:
    """
    نرمال‌سازی نام مشتری برای تطبیق دقیق.
    """
    if name is None or pd.isna(name):
        return ""
    name = str(name).strip()
    if not name:
        return ""
    replacements = {
        "ي": "ی", "ك": "ک", "ۀ": "ه", "ة": "ه", "ؤ": "و", "إ": "ا", "أ": "ا", "ٱ": "ا", "ئ": "ی", "‌": " ",
    }
    for src, dst in replacements.items():
        name = name.replace(src, dst)
    name = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", name)
    for ch in ["،", ",", "-", "_", "ـ"]:
        name = name.replace(ch, " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name.lower()


def load_balances_from_excel(file_path_or_buffer) -> list[dict]:
    """
    خواندن فایل اکسل با منطق دو ردیفی:
    ردیف ۵: سرستون‌ها (مانده پایان دوره، اشخاص)
    ردیف ۶: زیرستون‌ها (شرح، کد، بدهکار، بستانکار)
    """
    try:
        # خواندن کل فایل بدون هدر
        df_raw = pd.read_excel(file_path_or_buffer, header=None)
    except Exception as e:
        print(f"Error reading balances file: {e}")
        return []

    # تعریف ایندکس ردیف‌ها
    row_main_header = 4   # ردیف ۵
    row_sub_header = 5    # ردیف ۶

    # تابع کمکی اصلاح ی و ک
    def fix_yek(text):
        if text is None:
            return ""
        return str(text).replace("ي", "ی").replace("ك", "ک")

    # --- مرحله ۱: پیدا کردن ستون‌های اصلی در ردیف ۵ ---
    col_balance_main = None
    col_people_main = None

    for c_idx in range(len(df_raw.columns)):
        val = fix_yek(df_raw.iloc[row_main_header, c_idx])
        if "مانده پایان دوره" in val:
            col_balance_main = c_idx
        elif "اشخاص و شركتها" in val or "اشخاص و شرکتها" in val:
            col_people_main = c_idx

    if col_balance_main is None:
        print("ERROR: ستون 'مانده پایان دوره' در ردیف ۵ پیدا نشد.")
        return []

    if col_people_main is None:
        print("ERROR: ستون 'اشخاص و شرکت‌ها' در ردیف ۵ پیدا نشد.")
        return []

    print(
        f"DEBUG: ستون اصلی مانده: {col_balance_main}, ستون اصلی اشخاص: {col_people_main}")

    # --- مرحله ۲: پیدا کردن زیرستون‌ها در ردیف ۶ ---
    col_name = None
    col_code = None
    col_debit = None
    col_credit = None

    for c_idx in range(len(df_raw.columns)):
        val_sub = fix_yek(df_raw.iloc[row_sub_header, c_idx])

        # جستجوی شرح
        if "شرح" in val_sub:
            col_name = c_idx

        # جستجوی کد (با نرمال‌سازی کامل و چک کردن دقیق)
        # هم "کد" را چک می‌کنیم هم "code"
        clean_val = val_sub.strip().lower()
        if clean_val == "كد" or clean_val == "code":
            col_code = c_idx

        # جستجوی بدهکار و بستانکار
        if "بدهكار" in val_sub or "بدهکار" in val_sub:
            col_debit = c_idx
        elif "بستانكار" in val_sub or "بستانکار" in val_sub:
            col_credit = c_idx

    # --- منطق فال‌بک برای کد ---
    if col_code is None and col_name is not None:
        # اگر ستون کد پیدا نشد، فرض کن ستون قبل از شرح، کد مشتری است
        col_code = col_name - 1
        print(
            f"DEBUG: ستون کد پیدا نشد، ستون قبل از شرح ({col_code}) به عنوان کد انتخاب شد.")

    if col_name is None:
        print("ERROR: ستون 'شرح' در ردیف ۶ پیدا نشد.")
        return []

    print(
        f"DEBUG: Name: {col_name}, Code: {col_code}, Debit: {col_debit}, Credit: {col_credit}")
    # --- مرحله ۳: استخراج داده‌ها ---
    balances_list = []
    data_start_row = row_sub_header + 1  # ردیف ۷ به بعد

    for r_idx in range(data_start_row, len(df_raw)):
        # دریافت نام
        raw_name = df_raw.iloc[r_idx, col_name]
        norm_name = normalize_name(raw_name)

        if not norm_name:
            continue

        # دریافت کد
        raw_code = ""
        if col_code is not None:
            val_code = df_raw.iloc[r_idx, col_code]
            if pd.notna(val_code):
                # تبدیل به رشته و حذف فاصله‌های اضافی
                temp_str = str(val_code).strip()

                # حذف .0 از انتهای رشته (تحت هر شرایطی)
                if temp_str.endswith(".0"):
                    raw_code = temp_str[:-2]
                else:
                    raw_code = temp_str
        # دریافت مبالغ بدهکار و بستانکار
        debit_val = 0.0
        credit_val = 0.0

        if col_debit is not None:
            d_val = df_raw.iloc[r_idx, col_debit]
            if pd.notna(d_val):
                try:
                    debit_val = float(str(d_val).replace(
                        ",", "").replace("،", ""))
                except ValueError:
                    pass

        if col_credit is not None:
            c_val = df_raw.iloc[r_idx, col_credit]
            if pd.notna(c_val):
                try:
                    credit_val = float(str(c_val).replace(
                        ",", "").replace("،", ""))
                except ValueError:
                    pass

        # محاسبه مانده خالص
        net_balance = credit_val - debit_val

        balances_list.append({
            "CustomerCode": raw_code,
            "CustomerName": norm_name,
            "OriginalName": str(raw_name).strip(),
            "Balance": net_balance
        })

    return balances_list


def save_balances_to_db(data: list[dict]):
    """
    ذخیره لیست مانده‌ها در فایل اکسل دیتابیس.
    """
    df = pd.DataFrame(data)
    df = df.sort_values(by="CustomerName")
    df.to_excel(BALANCES_DB_PATH, index=False)


def load_balances_from_db() -> list[dict]:
    """
    خواندن مانده‌های ذخیره شده از فایل دیتابیس.
    """
    if not os.path.exists(BALANCES_DB_PATH):
        return []
    try:
        df = pd.read_excel(BALANCES_DB_PATH)
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"Error loading balances DB: {e}")
        return []


def update_balances(new_items: list[dict]):
    """
    به‌روزرسانی مانده‌ها: آیتم‌های جدید جایگزین یا اضافه می‌شوند.
    """
    current_data = load_balances_from_db()
    current_map = {item["CustomerName"]: item for item in current_data}
    for item in new_items:
        key = item["CustomerName"]
        current_map[key] = item
    save_balances_to_db(list(current_map.values()))


def add_customer_mapping(name: str, code: str):
    """
    افزودن یا ویرایش یک مپینگ نام -> کد به دیتابیس مانده‌ها.
    این تابع برای بخش 'رفع اشکال دستی' استفاده می‌شود.
    اگر نام وجود داشت، کد آن را آپدیت می‌کند. اگر نبود، با مانده ۰ اضافه می‌کند.
    """
    norm_name = normalize_name(name)
    clean_code = str(code).strip()

    if not norm_name or not clean_code:
        return False

    current_data = load_balances_from_db()
    updated_data = []
    found = False

    for item in current_data:
        if item.get("CustomerName") == norm_name:
            # آپدیت کد اگر نام وجود داشت
            item["CustomerCode"] = clean_code
            item["OriginalName"] = name  # آپدیت نام اصلی برای نمایش بهتر
            found = True
        updated_data.append(item)

    if not found:
        # افزودن جدید
        updated_data.append({
            "CustomerCode": clean_code,
            "CustomerName": norm_name,
            "OriginalName": name,
            "Balance": 0  # مانده پیش‌فرض صفر است
        })

    save_balances_to_db(updated_data)
    return True
