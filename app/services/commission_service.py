import os
import pandas as pd
import numpy as np
from datetime import datetime

# --- ایمپورت‌های اصلی پروژه شما ---
from app.services.customer_balances import load_balances_from_db
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
    if not os.path.exists(path):
        return {}
    df = pd.read_excel(path)
    cfg: dict[str, dict] = {}
    for _, row in df.iterrows():
        key = str(row.get("Group", "")).strip()
        if not key:
            continue
        percent_val = 0.0
        p = row.get("Percent")
        if pd.notna(p):
            try:
                percent_val = float(p) / 100.0
            except ValueError:
                percent_val = 0.0
        due_days_val = None
        d = row.get("DueDays")
        if pd.notna(d):
            try:
                due_days_val = int(float(d))
            except ValueError:
                due_days_val = None
        is_cash_val = bool(row.get("IsCash"))
        cfg[key] = {
            "percent": percent_val,
            "due_days": due_days_val,
            "is_cash": is_cash_val,
        }
    return cfg


def load_product_group_map(path: str = PRODUCT_GROUP_MAP_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ProductCode", "ProductName", "Group"])
    df = pd.read_excel(path)
    for c in ["ProductCode", "ProductName", "Group"]:
        if c not in df.columns:
            df[c] = None
    df["ProductCode"] = df["ProductCode"].map(
        lambda v: canonicalize_code(v) if pd.notna(v) else None
    )
    return df[["ProductCode", "ProductName", "Group"]]


def save_product_group_map(df: pd.DataFrame, path: str = PRODUCT_GROUP_MAP_PATH) -> None:
    cols = ["ProductCode", "ProductName", "Group"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df_out = df[cols].copy()
    df_out.to_excel(path, index=False)


def get_priority(product_group: str) -> str:
    text = str(product_group)
    if "نقدی" in text:
        return "cash"
    return "normal"

# ------------------ مدیریت لیست‌ها (بلک‌لیست و بازاریاب) ------------------


def load_blacklist_sets():
    banned_codes = set()
    banned_names = set()
    if not os.path.exists(BLACKLIST_FILE):
        return banned_codes, banned_names
    try:
        df = pd.read_excel(BLACKLIST_FILE)
        if "CustomerCode" in df.columns:
            for val in df["CustomerCode"]:
                c = canonicalize_code(val)
                if c:
                    banned_codes.add(c)
        if "CustomerName" in df.columns:
            for val in df["CustomerName"]:
                n = normalize_persian_name(val)
                if n:
                    banned_names.add(n)
    except Exception as e:
        print(f"Error loading blacklist file: {e}")
    return banned_codes, banned_names


def load_product_blacklist_set():
    banned_products = set()
    if not os.path.exists(PRODUCT_BLACKLIST_PATH):
        return banned_products
    try:
        df = pd.read_excel(PRODUCT_BLACKLIST_PATH)
        col_name = None
        for c in df.columns:
            if "code" in c.lower() or "کد" in c:
                col_name = c
                break
        if col_name:
            for val in df[col_name]:
                c = canonicalize_code(val)
                if c:
                    banned_products.add(c)
    except Exception as e:
        print(f"Error loading product blacklist: {e}")
    return banned_products


def save_product_blacklist(codes: list):
    df = pd.DataFrame({"ProductCode": codes, "DateAdded": [
                      datetime.now()] * len(codes)})
    df.to_excel(PRODUCT_BLACKLIST_PATH, index=False)


def load_allowed_marketers() -> set:
    if not os.path.exists(MARKETERS_PATH):
        return set()
    try:
        df = pd.read_excel(MARKETERS_PATH)
        col = next((c for c in df.columns if "marketer" in c.lower()
                   or "visitor" in c.lower() or "بازاریاب" in c), None)
        if not col:
            return set()
        return set(df[col].dropna().apply(lambda x: normalize_persian_name(str(x))).unique())
    except Exception as e:
        print(f"Error loading marketers: {e}")
        return set()


def save_marketers_list(names: list):
    df = pd.DataFrame({"MarketerName": names})
    df.to_excel(MARKETERS_PATH, index=False)

# ------------------ منطق اصلی پردازش فروش ------------------


def prepare_sales(sales_df: pd.DataFrame, group_config: dict, group_col: str) -> pd.DataFrame:
    sales_df = sales_df.copy()

    # 1. فیلتر بازاریاب‌ها
    allowed_marketers = load_allowed_marketers()
    if os.path.exists(MARKETERS_PATH):
        if "Salesperson" in sales_df.columns:
            sales_df["_TempMarketerNorm"] = sales_df["Salesperson"].apply(
                lambda x: normalize_persian_name(str(x)))
            sales_df = sales_df[sales_df["_TempMarketerNorm"].isin(
                allowed_marketers)]
            sales_df.drop(columns=["_TempMarketerNorm"], inplace=True)
        else:
            sales_df = sales_df.iloc[0:0]

    # 2. فیلتر کالاهای ممنوعه
    product_col_name = "ProductCode"
    if product_col_name in sales_df.columns:
        banned_products = load_product_blacklist_set()
        if banned_products:
            sales_df["_TempProdKey"] = sales_df[product_col_name].map(
                canonicalize_code)
            sales_df = sales_df[~sales_df["_TempProdKey"].isin(
                banned_products)]
            sales_df.drop(columns=["_TempProdKey"], inplace=True)

    # 3. آماده‌سازی ستون‌ها
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

    # 4. فیلتر مشتریان
    banned_codes, banned_names = load_blacklist_sets()
    sales_df["_TempKey"] = sales_df["CustomerCode"].map(canonicalize_code)
    sales_df["_TempName"] = sales_df["CustomerName"].apply(
        normalize_persian_name)
    mask_banned_code = sales_df["_TempKey"].isin(banned_codes)
    mask_banned_name = sales_df["_TempName"].isin(banned_names)
    sales_df = sales_df[~(mask_banned_code | mask_banned_name)]
    sales_df.drop(columns=["_TempKey", "_TempName"], inplace=True)

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
        except:
            pass
        return get_priority(row.get(group_col, ""))

    sales_df["Priority"] = sales_df.apply(compute_priority, axis=1)
    sales_df["PriorityRank"] = (sales_df["Priority"].map(
        {"cash": 0, "normal": 1}).fillna(1).astype(int))

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


# =============================================================================
# تابع اصلی محاسبات (اصلاح شده برای اعمال مانده حساب)
# =============================================================================
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
    - اعمال مانده حساب (مثبت یا منفی) روی پرداخت‌ها
    - تسویه فاکتورها طبق اولویت (نقدی → عادی، قدیمی → جدید)
    - محاسبه پورسانت
    """
    # 1. آماده‌سازی فروش
    sales_df = prepare_sales(sales_raw, group_config, group_col)

    checks_df = (checks_raw.copy(
    ) if checks_raw is not None and not checks_raw.empty else pd.DataFrame())

    # 2. آماده‌سازی پرداخت‌ها
    payments_df, _ = prepare_payments(payments_raw, checks_df, sales_df)

    # اگر پرداختی نداریم، خروجی خالی برمی‌گردانیم (مگر اینکه مانده مثبت داشته باشیم، اما فعلا منطق اصلی روال پرداخت است)
    if payments_df.empty:
        salesperson_df = (
            sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
            .sum()
            .reset_index()
        )
        salesperson_df.rename(
            columns={"CommissionAmount": "TotalCommission"}, inplace=True)
        return sales_df, salesperson_df, payments_df

    # --- [NEW] لود کردن مانده حساب‌ها ---
    balances_list = load_balances_from_db()
    # دیکشنری: {کد_استاندارد: مبلغ_مانده}
    balances_map = {}
    for item in balances_list:
        c_code = canonicalize_code(item.get("CustomerCode"))
        bal_val = item.get("Balance", 0)
        try:
            bal_val = float(bal_val)
        except:
            bal_val = 0
        if c_code:
            balances_map[c_code] = bal_val

    # تسویه بر اساس CustomerKey استاندارد
    for cust_key, pay_group in payments_df.groupby("ResolvedCustomerKey"):
        if cust_key is None or (isinstance(cust_key, float) and pd.isna(cust_key)):
            continue
        if str(cust_key).strip() == "":
            continue

        # پیدا کردن فاکتورهای این مشتری
        cust_invoice_idx = sales_df.index[sales_df["CustomerKey"] == cust_key]
        if len(cust_invoice_idx) == 0:
            continue

        # مرتب‌سازی فاکتورها (اول نقدی، بعد قدیمی‌ترها)
        cust_invoice_idx = (
            sales_df.loc[cust_invoice_idx]
            .sort_values(["PriorityRank", "InvoiceDate"])
            .index
        )

        # مرتب‌سازی پرداخت‌ها بر اساس تاریخ
        if "PaymentDate" in pay_group.columns:
            pay_group = pay_group.sort_values("PaymentDate")

        # --- [NEW LOGIC START] اعمال مانده حساب مشتری ---
        customer_balance = balances_map.get(cust_key, 0.0)

        # لیست نهایی پرداخت‌ها برای این مشتری (که اصلاح شده است)
        final_payments_list = []

        if customer_balance > 0:
            # === حالت اول: بستانکار (مانده مثبت) ===
            # اضافه کردن یک پرداخت مجازی با تاریخ بسیار قدیمی
            virtual_payment = {
                "Amount": customer_balance,
                # تاریخ ازلی (همیشه سررسید را پاس می‌کند)
                "PaymentDate": pd.Timestamp.min,
                "SourceType": "InitialCredit",
                "Description": "مانده بستانکار ابتدای دوره"
            }
            # اول پرداخت مجازی اضافه می‌شود
            final_payments_list.append(virtual_payment)

            # سپس تمام پرداخت‌های جدید عیناً اضافه می‌شوند
            for _, row in pay_group.iterrows():
                final_payments_list.append(row.to_dict())

        elif customer_balance < 0:
            # === حالت دوم: بدهکار (مانده منفی) ===
            debt_remaining = abs(customer_balance)

            for _, row in pay_group.iterrows():
                # کپی دیکشنری برای تغییر ندادن دیتافریم اصلی
                p_data = row.to_dict()
                amount = float(p_data.get("Amount", 0))

                if debt_remaining > 0:
                    if amount <= debt_remaining:
                        # کل این پرداخت صرف بدهی قبلی شد
                        debt_remaining -= amount
                        amount = 0
                        # پرداختی که مبلغش صفر شد عملاً در حلقه پایین تاثیری ندارد
                    else:
                        # بخشی صرف بدهی، بخشی باقی می‌ماند
                        amount -= debt_remaining
                        debt_remaining = 0

                if amount > 0:
                    p_data["Amount"] = amount
                    final_payments_list.append(p_data)
        else:
            # === حالت سوم: مانده صفر ===
            for _, row in pay_group.iterrows():
                final_payments_list.append(row.to_dict())

        # --- [NEW LOGIC END] ---

        # حلقه تخصیص (Allocation Loop) روی لیست نهایی پرداخت‌ها
        for p in final_payments_list:
            remaining_payment = float(p.get("Amount", 0))
            pay_date = p.get("PaymentDate", None)

            # پیمایش روی فاکتورها برای تخصیص
            for idx in cust_invoice_idx:
                if remaining_payment <= 0.001:  # اگر پول تمام شد (با تلورانس)
                    break

                remaining_invoice = sales_df.at[idx, "Remaining"]
                if remaining_invoice <= 0:
                    continue

                # چقدر از این فاکتور را می‌توانیم با این پرداخت پاس کنیم؟
                allocate = min(remaining_payment, remaining_invoice)

                # شرط تعلق پورسانت: تاریخ پرداخت <= تاریخ سررسید
                # (اگر پرداخت مجازی باشد، pay_date مینیمم است و شرط True می‌شود)
                in_due = True
                if isinstance(pay_date, (pd.Timestamp, datetime)):
                    due_date = sales_df.at[idx, "DueDate"]
                    if pd.notna(due_date):
                        in_due = bool(pay_date <= due_date)
                    else:
                        # اگر فاکتور تاریخ سررسید ندارد، سخت‌گیرانه عمل کنیم یا نه؟
                        # پیش‌فرض فعلی شما: اگر due_date نباشد، چه کنیم؟
                        # معمولا اگر due_date نباشد یعنی محدودیت زمانی نیست، پس True
                        in_due = True

                if in_due:
                    percent = sales_df.at[idx, "CommissionPercent"]
                    sales_df.at[idx, "CommissionAmount"] += allocate * percent

                # آپدیت مقادیر فاکتور و مانده پرداخت
                sales_df.at[idx, "PaidAmount"] += allocate
                sales_df.at[idx, "Remaining"] -= allocate
                remaining_payment -= allocate

    # جمع‌بندی پورسانت‌ها
    salesperson_df = (
        sales_df.groupby("Salesperson", dropna=False)["CommissionAmount"]
        .sum()
        .reset_index()
    )
    salesperson_df.rename(
        columns={"CommissionAmount": "TotalCommission"}, inplace=True
    )

    return sales_df, salesperson_df, payments_df


# ------------------ توابع کمکی پرداخت ------------------

def extract_customer_for_payment(row: pd.Series, checks_df: pd.DataFrame, db_map: dict, bind_map: dict) -> str | None:
    stype = row.get("SourceType", "Payment")
    name = row.get("CustomerName")
    desc_str = str(row.get("Description") or "")
    effective_name = name

    if stype == "Check":
        candidates = []
        if pd.notna(row.get("CheckNumber")):
            candidates.append(str(row.get("CheckNumber")))
        import re
        m = re.search(r"(\d{3,10})", desc_str)
        if m:
            candidates.append(m.group(1))

        if checks_df is not None and not checks_df.empty:
            for cand in candidates:
                clean_num = re.sub(r"\D", "", str(cand)).lstrip("0")
                if not clean_num:
                    continue
                found_rows = checks_df[checks_df["CheckNumber"].astype(
                    str).str.contains(clean_num, na=False)]
                if not found_rows.empty:
                    chk_code = found_rows.iloc[0].get("CustomerCode")
                    if pd.notna(chk_code):
                        return canonicalize_code(chk_code)
                    chk_name = found_rows.iloc[0].get("CustomerName")
                    if pd.notna(chk_name):
                        effective_name = chk_name
                    break

    if pd.notna(effective_name):
        key = name_key_for_matching(effective_name)
        if key and key in bind_map:
            return canonicalize_code(bind_map[key])

    if db_map is not None and pd.notna(effective_name):
        key = name_key_for_matching(effective_name)
        if key and key in db_map:
            return canonicalize_code(db_map[key])

    return None


def prepare_payments(payments_df: pd.DataFrame, checks_df: pd.DataFrame, sales_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    payments_df = payments_df.copy()
    if "PaymentDate" in payments_df.columns:
        payments_df["PaymentDate"] = payments_df["PaymentDate"].apply(
            parse_jalali_or_gregorian)
    if "Amount" not in payments_df.columns:
        raise ValueError("ستون Amount در فایل پرداخت‌ها یافت نشد.")
    payments_df["Amount"] = payments_df["Amount"].astype(float)
    if "CustomerName" not in payments_df.columns:
        payments_df["CustomerName"] = None

    bind_map = load_name_code_map_from_excel()
    db_map = build_name_code_map_from_balances()
    unresolved_items = []

    def resolve_logic(row):
        code = extract_customer_for_payment(
            row, checks_df, db_map=db_map, bind_map=bind_map)
        if pd.isna(code):
            unresolved_items.append({
                "Name": row.get("CustomerName"),
                "Amount": row.get("Amount"),
                "Date": row.get("PaymentDate"),
                "Source": row.get("SourceType", "Payment")
            })
            return "یافت نشد"
        return code

    payments_df["ResolvedCustomer"] = payments_df.apply(resolve_logic, axis=1)

    def clean_key(val):
        if val == "یافت نشد":
            return None
        return canonicalize_code(val)
    payments_df["ResolvedCustomerKey"] = payments_df["ResolvedCustomer"].map(
        clean_key)
    return payments_df, unresolved_items


def build_name_code_map_from_balances() -> dict[str, str]:
    balances = load_balances_from_db()
    name_to_code = {}
    banned_codes, banned_names = load_blacklist_sets()
    for item in balances:
        name = item.get("CustomerName")
        code = item.get("CustomerCode")
        if name and code:
            clean_code = canonicalize_code(code)
            if clean_code in banned_codes:
                continue
            norm_name = normalize_persian_name(name)
            if norm_name in banned_names:
                continue
            key = name_key_for_matching(name)
            if key:
                name_to_code[key] = str(code).strip()
    return name_to_code


def load_name_code_map_from_excel() -> dict[str, str]:
    file_path = "customer_codes_bind.xlsx"
    name_to_code = {}
    if not os.path.exists(file_path):
        return name_to_code
    try:
        df = pd.read_excel(file_path)
        if "CustomerName" in df.columns and "CustomerCode" in df.columns:
            for _, row in df.iterrows():
                name = str(row.get("CustomerName", "")).strip()
                code = str(row.get("CustomerCode", "")).strip()
                if code and code != "یافت نشد" and name:
                    key = name_key_for_matching(name)
                    if key:
                        name_to_code[key] = code
    except Exception as e:
        print(f"Error loading bind excel: {e}")
    return name_to_code


def build_name_code_mapping(sales_df: pd.DataFrame) -> dict[str, str]:
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
