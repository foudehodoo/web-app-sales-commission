import os
import pandas as pd
from datetime import datetime

from app.services.customer_balances import load_balances_from_db
# ایمپورت توابع کمکی از فایل helpers
from app.services.helpers import (
    canonicalize_code,
    normalize_persian_name,
    name_key_for_matching,
    parse_jalali_or_gregorian,
    to_jalali_str
)

# ------------------ تنظیمات فایل‌های پیکربندی ------------------
DEFAULT_GROUP_CONFIG_PATH = "group_config.xlsx"
PRODUCT_GROUP_MAP_PATH = "product_group_map.xlsx"
MARKETERS_PATH = "marketers.xlsx"
PRODUCT_BLACKLIST_PATH = "product_blacklist.xlsx"
BLACKLIST_FILE = "blacklist.xlsx"

# ------------------ مدیریت تنظیمات گروه‌ها ------------------


def load_default_group_config(path: str = DEFAULT_GROUP_CONFIG_PATH) -> dict:
    """
    خواندن تنظیمات پیش‌فرض گروه‌ها از یک اکسل:
    ستون‌ها: Group, Percent, DueDays, IsCash
    - Group : اسم گروه کالا (مثلاً "نقدی ۲٪ هفت روزه")
    - Percent : درصد پورسانت (به صورت انسانی: 2 یعنی 2٪)
    - DueDays : مهلت تسویه (روز)
    - IsCash : 0/1 یا True/False
    خروجی: دیکشنری
        group_name -> {percent, due_days, is_cash}
    که percent به صورت ضریب (0.02) برمی‌گردد.
    """
    if not os.path.exists(path):
        return {}

    df = pd.read_excel(path)

    cfg: dict[str, dict] = {}

    for _, row in df.iterrows():
        key = str(row.get("Group", "")).strip()
        if not key:
            continue

        # درصد (در اکسل به صورت درصد انسانی ذخیره شده است)
        percent_val = 0.0
        p = row.get("Percent")
        if pd.notna(p):
            try:
                percent_val = float(p) / 100.0
            except ValueError:
                percent_val = 0.0

        # مهلت تسویه
        due_days_val = None
        d = row.get("DueDays")
        if pd.notna(d):
            try:
                due_days_val = int(float(d))
            except ValueError:
                due_days_val = None

        # نقدی بودن
        is_cash_val = bool(row.get("IsCash"))

        cfg[key] = {
            "percent": percent_val,
            "due_days": due_days_val,
            "is_cash": is_cash_val,
        }

    return cfg


def load_product_group_map(path: str = PRODUCT_GROUP_MAP_PATH) -> pd.DataFrame:
    """
    خواندن مپ کد کالا → نام گروه کالا از اکسل.
    ستون‌ها: ProductCode, ProductName, Group
    """
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ProductCode", "ProductName", "Group"])

    df = pd.read_excel(path)

    for c in ["ProductCode", "ProductName", "Group"]:
        if c not in df.columns:
            df[c] = None

    # نرمال‌سازی کد کالا
    df["ProductCode"] = df["ProductCode"].map(
        lambda v: canonicalize_code(v) if pd.notna(v) else None
    )

    return df[["ProductCode", "ProductName", "Group"]]


def save_product_group_map(df: pd.DataFrame, path: str = PRODUCT_GROUP_MAP_PATH) -> None:
    """
    ذخیره‌ی مپ کد کالا → گروه در اکسل.
    """
    cols = ["ProductCode", "ProductName", "Group"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df_out = df[cols].copy()
    df_out.to_excel(path, index=False)


def get_priority(product_group: str) -> str:
    """
    fallback: اگر تنظیمی نداشتیم، از روی نام گروه نقدی/عادی را حدس می‌زنیم.
    """
    text = str(product_group)
    if "نقدی" in text:
        return "cash"
    return "normal"

# ------------------ مدیریت لیست‌ها (بلک‌لیست و بازاریاب) ------------------


def load_blacklist_sets():
    """
    خواندن فایل اکسل بلک‌لیست و بازگرداندن دو مجموعه:
    1. set of banned_codes (canonicalized)
    2. set of banned_names (normalized)
    """
    banned_codes = set()
    banned_names = set()

    if not os.path.exists(BLACKLIST_FILE):
        return banned_codes, banned_names

    try:
        df = pd.read_excel(BLACKLIST_FILE)

        # 1. جمع‌آوری کدها
        if "CustomerCode" in df.columns:
            for val in df["CustomerCode"]:
                c = canonicalize_code(val)
                if c:
                    banned_codes.add(c)

        # 2. جمع‌آوری نام‌ها
        if "CustomerName" in df.columns:
            for val in df["CustomerName"]:
                n = normalize_persian_name(val)
                if n:
                    banned_names.add(n)

    except Exception as e:
        print(f"Error loading blacklist file: {e}")

    return banned_codes, banned_names


# ------------------ مدیریت لیست سیاه کالا ------------------ #

def load_product_blacklist_set():
    """
    خواندن کدهای کالای ممنوعه و بازگرداندن یک مجموعه (Set) از کدهای نرمال‌شده.
    """
    banned_products = set()
    if not os.path.exists(PRODUCT_BLACKLIST_PATH):
        return banned_products

    try:
        df = pd.read_excel(PRODUCT_BLACKLIST_PATH)
        # فرض می‌کنیم ستون ProductCode یا 'کد کالا' داریم
        col_name = None
        for c in df.columns:
            if "code" in c.lower() or "کد" in c:
                col_name = c
                break

        if col_name:
            # استفاده از canonicalize_code برای اینکه 101 با 101.0 یکی شود
            for val in df[col_name]:
                c = canonicalize_code(val)
                if c:
                    banned_products.add(c)
    except Exception as e:
        print(f"Error loading product blacklist: {e}")

    return banned_products


def save_product_blacklist(codes: list):
    """
    ذخیره لیست کدهای ممنوعه در اکسل
    """
    df = pd.DataFrame({"ProductCode": codes, "DateAdded": [
                      datetime.now()] * len(codes)})
    df.to_excel(PRODUCT_BLACKLIST_PATH, index=False)


def load_allowed_marketers() -> set:
    """
    لیست بازاریاب‌های مجاز را برمی‌گرداند (مجموعه‌ای از نام‌های نرمال شده).
    """
    if not os.path.exists(MARKETERS_PATH):
        return set()  # اگر فایل نباشد، یعنی هیچ بازاریابی مجاز نیست (یا همه غیرمجازند؟ بسته به منطق)
        # نکته: اگر فایل وجود نداشته باشد، منطقاً باید فرض کنیم فیلتر بازاریاب غیرفعال است
        # اما طبق درخواست شما "فقط و فقط... در لیست باشند"، پس اگر لیست خالی باشد، خروجی صفر خواهد بود.

    try:
        df = pd.read_excel(MARKETERS_PATH)
        # فرض می‌کنیم ستونی به نام 'MarketerName' یا 'VisitorName' داریم
        col = next((c for c in df.columns if "marketer" in c.lower()
                   or "visitor" in c.lower() or "بازاریاب" in c), None)

        if not col:
            return set()

        # نرمال‌سازی نام‌ها برای مقایسه دقیق
        return set(df[col].dropna().apply(lambda x: normalize_persian_name(str(x))).unique())
    except Exception as e:
        print(f"Error loading marketers: {e}")
        return set()


def save_marketers_list(names: list):
    df = pd.DataFrame({"MarketerName": names})
    df.to_excel(MARKETERS_PATH, index=False)

# ------------------ منطق اصلی پردازش فروش ------------------


def prepare_sales(sales_df: pd.DataFrame, group_config: dict, group_col: str) -> pd.DataFrame:
    """
    نسخه جدید شامل:
    1. فیلتر بازاریاب‌ها (Whitelist)
    2. فیلتر کالاها (Product Blacklist) -> جدید
    3. فیلتر مشتریان (Customer Blacklist)
    """
    sales_df = sales_df.copy()

    # --- 1. فیلتر بازاریاب‌ها (Marketers Whitelist) ---
    allowed_marketers = load_allowed_marketers()
    if os.path.exists(MARKETERS_PATH):
        if "Salesperson" in sales_df.columns:
            sales_df["_TempMarketerNorm"] = sales_df["Salesperson"].apply(
                lambda x: normalize_persian_name(str(x))
            )
            sales_df = sales_df[sales_df["_TempMarketerNorm"].isin(
                allowed_marketers)]
            sales_df.drop(columns=["_TempMarketerNorm"], inplace=True)
        else:
            # اگر فایل هست ولی ستون نیست، کل دیتا حذف شود (امنیت)
            sales_df = sales_df.iloc[0:0]

    # --- 2. فیلتر کالاهای ممنوعه (Product Blacklist - NEW) ---
    # فرض: ستون کالا در فایل فروش 'ProductCode' نام دارد.
    # اگر نام ستون چیز دیگری است (مثلاً 'Product Code' یا 'کد کالا') اینجا را تغییر دهید.
    product_col_name = "ProductCode"

    # چک می‌کنیم آیا ستون کالا اصلاً وجود دارد؟
    if product_col_name in sales_df.columns:
        banned_products = load_product_blacklist_set()
        if banned_products:
            sales_df["_TempProdKey"] = sales_df[product_col_name].map(
                canonicalize_code)

            before_prod_filter = len(sales_df)
            # نگه‌داشتن ردیف‌هایی که کد کالایشان در لیست سیاه نیست
            sales_df = sales_df[~sales_df["_TempProdKey"].isin(
                banned_products)]

            removed = before_prod_filter - len(sales_df)
            if removed > 0:
                print(f"PRODUCT BLACKLIST: Removed {removed} rows.")

            sales_df.drop(columns=["_TempProdKey"], inplace=True)

    # --- 3. آماده‌سازی ستون‌های ضروری (جلوگیری از خطا در صورت خالی شدن) ---
    if "InvoiceDate" not in sales_df.columns:
        if sales_df.empty:
            pass
        else:
            raise ValueError(
                "در فایل فروش ستونی به نام 'InvoiceDate' پیدا نشد.")

    if sales_df.empty:
        expected_cols = ["InvoiceDate", "CustomerCode", "CustomerName", "Amount",
                         "Remaining", "CommissionAmount", "DueDate", "Priority", "PriorityRank"]
        for c in expected_cols:
            if c not in sales_df.columns:
                sales_df[c] = pd.NA
        return sales_df

    sales_df["InvoiceDate"] = sales_df["InvoiceDate"].apply(
        parse_jalali_or_gregorian)

    if "CustomerCode" not in sales_df.columns:
        sales_df["CustomerCode"] = pd.NA

    # --- 4. فیلتر مشتریان (Customer Blacklist) ---
    banned_codes, banned_names = load_blacklist_sets()
    sales_df["_TempKey"] = sales_df["CustomerCode"].map(canonicalize_code)
    sales_df["_TempName"] = sales_df["CustomerName"].apply(
        normalize_persian_name)

    mask_banned_code = sales_df["_TempKey"].isin(banned_codes)
    mask_banned_name = sales_df["_TempName"].isin(banned_names)
    sales_df = sales_df[~(mask_banned_code | mask_banned_name)]

    sales_df.drop(columns=["_TempKey", "_TempName"], inplace=True)
    # ------------------------------------------------------------------

    # ادامه منطق استاندارد محاسبات...
    sales_df["CustomerKey"] = sales_df["CustomerCode"].map(canonicalize_code)
    sales_df = sales_df[sales_df["CustomerKey"].notna()]

    if "DueDate" in sales_df.columns:
        sales_df["DueDate"] = sales_df["DueDate"].apply(
            parse_jalali_or_gregorian)
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
            # تابع get_priority باید موجود باشد
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
        # تابع get_priority باید موجود باشد
        return get_priority(row.get(group_col, ""))

    sales_df["Priority"] = sales_df.apply(compute_priority, axis=1)
    sales_df["PriorityRank"] = (
        sales_df["Priority"].map(
            {"cash": 0, "normal": 1}).fillna(1).astype(int)
    )

    def row_percent(row):
        key = str(row.get(group_col))
        cfg = group_config.get(key) if group_config else None
        if cfg is None:
            return 0.0
        return float(cfg.get("percent", 0.0))

    if "Amount" not in sales_df.columns:
        if not sales_df.empty:
            raise ValueError("در فایل فروش ستونی به نام 'Amount' پیدا نشد.")

    sales_df["CommissionPercent"] = sales_df.apply(row_percent, axis=1)
    sales_df["Amount"] = sales_df["Amount"].astype(float)
    sales_df["PaidAmount"] = 0.0
    sales_df["Remaining"] = sales_df["Amount"]
    sales_df["CommissionAmount"] = 0.0

    return sales_df


def compute_commissions(
    sales_raw: pd.DataFrame,
    payments_raw: pd.DataFrame,
    checks_raw: pd.DataFrame,
    group_config: dict,
    group_col: str,
    reactivation_days: int = 90
):
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

    # ---------------------------------------------------------
    # اصلاحیه: دریافت خروجی صحیح از prepare_payments
    # این تابع یک تاپل برمی‌گرداند: (payments_df, unresolved_items)
    # ---------------------------------------------------------
    payments_df, _ = prepare_payments(payments_raw, checks_df, sales_df)

    # اگر پرداختی نداریم
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

    # تسویه بر اساس CustomerKey استاندارد
    for cust_key, pay_group in payments_df.groupby("ResolvedCustomerKey"):
        if cust_key is None or (isinstance(cust_key, float) and pd.isna(cust_key)):
            continue
        if str(cust_key).strip() == "":
            continue

        cust_invoice_idx = sales_df.index[sales_df["CustomerKey"] == cust_key]
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

# ------------------ منطق پرداخت‌ها (برای عطف کد) ------------------


def extract_customer_for_payment(
    row: pd.Series,
    checks_df: pd.DataFrame,
    db_map: dict,
    bind_map: dict  # <--- ورودی جدید: مپ اکسل دستی
) -> str | None:
    """
    استخراج کد مشتری برای یک ردیف پرداخت.
    اولویت‌ها:
    ۱. فایل اکسل دستی (customer_codes_bind.xlsx)
    ۲. دیتابیس مانده‌ها (DB Map)
    """
    stype = row.get("SourceType", "Payment")
    name = row.get("CustomerName")
    desc_str = str(row.get("Description") or "")

    # --- گام ۱: پیدا کردن "نام واقعی" ---
    # اگر چک است، سعی می‌کنیم نام صاحب چک را پیدا کنیم
    effective_name = name

    if stype == "Check":
        # تلاش برای استخراج شماره چک
        candidates = []
        if pd.notna(row.get("CheckNumber")):
            candidates.append(str(row.get("CheckNumber")))

        import re
        m = re.search(r"(\d{3,10})", desc_str)
        if m:
            candidates.append(m.group(1))

        # جستجو در فایل چک‌ها
        if checks_df is not None and not checks_df.empty:
            # اینجا فرض بر این است که checks_df قبلاً نرمالایز شده یا در تابع اصلی هندل می‌شود
            # اما برای محکم کاری یک جستجوی ساده انجام میدهیم
            for cand in candidates:
                clean_num = re.sub(r"\D", "", str(cand)).lstrip("0")
                if not clean_num:
                    continue

                # جستجو در ستون CheckNumber دیتافریم چک‌ها
                # نکته: این بخش می‌تواند کند باشد، بهتر است در prepare_payments مپ ساخته شود
                # اما برای حفظ ساختار فعلی اینجا می‌نویسیم:
                found_rows = checks_df[checks_df["CheckNumber"].astype(
                    str).str.contains(clean_num, na=False)]
                if not found_rows.empty:
                    # اگر در خود فایل چک، کد مشتری بود، همان عالی است
                    chk_code = found_rows.iloc[0].get("CustomerCode")
                    if pd.notna(chk_code):
                        return canonicalize_code(chk_code)

                    # اگر کد نبود، نام را برمی‌داریم
                    chk_name = found_rows.iloc[0].get("CustomerName")
                    if pd.notna(chk_name):
                        effective_name = chk_name
                    break

    # --- گام ۲: جستجو در فایل اکسل BIND (اولویت بالا) ---
    if pd.notna(effective_name):
        key = name_key_for_matching(effective_name)
        if key and key in bind_map:
            # اگر در اکسل دستی پیدا شد، فوراً برگردان
            return canonicalize_code(bind_map[key])

    # --- گام ۳: جستجو در دیتابیس مانده‌ها (اولویت دوم) ---
    if db_map is not None and pd.notna(effective_name):
        key = name_key_for_matching(effective_name)
        if key and key in db_map:
            return canonicalize_code(db_map[key])

    return None


def prepare_payments(
    payments_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    # این آرگومان هست اما فعلاً برای مچ کردن استفاده نمی‌شود (مچ در مرحله بعد است)
    sales_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    آماده‌سازی پرداخت‌ها با اولویت فایل اکسل Bind.
    """
    payments_df = payments_df.copy()

    # تبدیل تاریخ و فرمت‌دهی اولیه
    if "PaymentDate" in payments_df.columns:
        payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
            parse_jalali_or_gregorian)

    if "Amount" not in payments_df.columns:
        raise ValueError("ستون Amount در فایل پرداخت‌ها یافت نشد.")
    payments_df["Amount"] = payments_df["Amount"].astype(float)

    if "CustomerName" not in payments_df.columns:
        payments_df["CustomerName"] = None

    # ---------------------------------------------------------
    # ۱. لود کردن مپ‌ها (حافظه موقت)
    # ---------------------------------------------------------

    # الف) مپ اکسل دستی (اولویت اول)
    # دیکشنری: {normalized_name: code}
    bind_map = load_name_code_map_from_excel()

    # ب) مپ دیتابیس (اولویت دوم)
    db_map = build_name_code_map_from_balances()

    unresolved_items = []

    def resolve_logic(row):
        # این تابع از extract_customer_for_payment جدید استفاده می‌کند
        code = extract_customer_for_payment(
            row,
            checks_df,
            db_map=db_map,
            bind_map=bind_map  # <--- پاس دادن مپ جدید
        )

        if pd.isna(code):
            # برای گزارش‌دهی موارد پیدا نشده
            unresolved_items.append({
                "Name": row.get("CustomerName"),
                "Amount": row.get("Amount"),
                "Date": row.get("PaymentDate"),
                "Source": row.get("SourceType", "Payment")
            })
            return "یافت نشد"

        return code

    # اعمال تابع روی همه ردیف‌ها
    payments_df["ResolvedCustomer"] = payments_df.apply(resolve_logic, axis=1)

    # ساخت کلید استاندارد (ResolvedCustomerKey) برای مقایسه راحت با فایل فروش
    def clean_key(val):
        if val == "یافت نشد":
            return None  # یا "یافت نشد" بسته به منطق بعدی شما
        return canonicalize_code(val)

    payments_df["ResolvedCustomerKey"] = payments_df["ResolvedCustomer"].map(
        clean_key)

    # فیلتر کردن ردیف‌هایی که کد پیدا نشده (اختیاری - اگر می‌خواهید در محاسبات شرکت نکنند)
    # فعلا همه را نگه می‌داریم تا کاربر ببیند چه چیزی مچ نشده

    return payments_df, unresolved_items


def build_name_code_map_from_balances() -> dict[str, str]:
    """
    ساخت دیکشنری نام -> کد.
    اگر کد مشتری یا نام مشتری در بلک لیست باشد، در نظر گرفته نمی‌شود.
    """
    balances = load_balances_from_db()
    name_to_code = {}

    # بارگذاری لیست سیاه (کدها و نام‌ها)
    banned_codes, banned_names = load_blacklist_sets()

    for item in balances:
        name = item.get("CustomerName")
        code = item.get("CustomerCode")

        if name and code:
            # 1. چک کردن کد
            clean_code = canonicalize_code(code)
            if clean_code in banned_codes:
                continue

            # 2. چک کردن نام
            norm_name = normalize_persian_name(name)
            if norm_name in banned_names:
                continue

            key = name_key_for_matching(name)
            if key:
                name_to_code[key] = str(code).strip()

    return name_to_code


def load_name_code_map_from_excel() -> dict[str, str]:
    """
    خواندن نگاشت نام -> کد از فایل اکسل 'customer_codes_bind.xlsx'.
    این فایل باید شامل ستون‌های CustomerName و CustomerCode باشد.
    """
    file_path = "customer_codes_bind.xlsx"
    name_to_code = {}

    if not os.path.exists(file_path):
        return name_to_code

    try:
        df = pd.read_excel(file_path)
        # بررسی وجود ستون‌های لازم
        if "CustomerName" in df.columns and "CustomerCode" in df.columns:
            for _, row in df.iterrows():
                name = str(row.get("CustomerName", "")).strip()
                code = str(row.get("CustomerCode", "")).strip()

                # فقط اگر کد معتبر است و "یافت نشد" نیست
                if code and code != "یافت نشد" and name:
                    # نرمال‌سازی نام برای تطبیق بهتر
                    key = name_key_for_matching(name)
                    if key:
                        name_to_code[key] = code
    except Exception as e:
        print(f"Error loading bind excel: {e}")

    return name_to_code


def build_name_code_mapping(sales_df: pd.DataFrame) -> dict[str, str]:
    """
    از روی جدول فروش، map می‌سازد:
        نام نرمال‌شده (بدون فاصله) → کد مشتری (استاندارد شده)
    فقط وقتی که آن نام دقیقاً به *یک* کد منجر شود.
    """
    if "CustomerName" not in sales_df.columns or "CustomerCode" not in sales_df.columns:
        return {}

    tmp = sales_df[["CustomerName", "CustomerCode"]].dropna()
    name_to_codes: dict[str, set[str]] = {}

    for _, row in tmp.iterrows():
        key = name_key_for_matching(row["CustomerName"])
        code = canonicalize_code(row["CustomerCode"])
        if not key or not code:
            continue
        name_to_codes.setdefault(key, set()).add(code)

    result: dict[str, str] = {}
    for key, codes in name_to_codes.items():
        if len(codes) == 1:
            result[key] = next(iter(codes))
    return result
