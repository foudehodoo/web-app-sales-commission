import math
from typing import Optional

import pandas as pd


def _normalize_str(s) -> str:
    """
    نرمال‌سازی متن فارسی برای مقایسه:
    - یکسان‌سازی ی/ي و ک/ك
    - حذف فاصله‌ی مجازی
    - جمع کردن فاصله‌های تکراری
    """
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    s = str(s)
    repl = {
        "ي": "ی",
        "ك": "ک",
        "‌": "",
        "\u200c": "",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    return " ".join(s.split())


def _find_header_top(df: pd.DataFrame) -> Optional[int]:
    """
    سطر بالایی هدر را پیدا می‌کند (سطر شامل «واریز یا برداشت کننده»).
    """
    max_rows = min(20, len(df))
    for i in range(max_rows):
        row_norm = [_normalize_str(v) for v in df.iloc[i]]
        if any("واریز یا برداشت کننده" in cell for cell in row_norm):
            return i
    return None


def _find_date_col_idx(df: pd.DataFrame) -> Optional[int]:
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        if "تاریخ" in nc and "مدرک" in nc:
            return idx
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        if "تاریخ" in nc:
            return idx
    return None


def _find_account_code_idx(df: Pd.DataFrame) -> Optional[int]:
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        if "کد طرف حساب" in nc or ("کد" in nc and "حساب" in nc):
            return idx
    return None


def _find_customer_name_idx(df: pd.DataFrame) -> Optional[int]:
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        if "واریز یا برداشت کننده" in nc:
            return idx
    return None


def _find_desc_idx(df: pd.DataFrame) -> Optional[int]:
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        if "توضیحات" in nc or "شرح" in nc:
            return idx
    return None


def _find_deposit_withdraw_idx(df: pd.DataFrame) -> tuple[Optional[int], Optional[int]]:
    dep = wd = None
    for idx, c in enumerate(df.columns):
        nc = _normalize_str(c)
        # در فایل تو ستون‌ها دقیقا «واریزی» و «برداشتی» هستند
        if dep is None and "واریزی" in nc:
            dep = idx
        if wd is None and "برداشتی" in nc:
            wd = idx
    return dep, wd


def _find_doc_no_idx(df: pd.DataFrame) -> Optional[int]:
    # اولین ستون با هدر «شماره»
    for idx, c in enumerate(df.columns):
        if _normalize_str(c) == "شماره":
            return idx
    for idx, c in enumerate(df.columns):
        if "شماره" in _normalize_str(c):
            return idx
    return None


def load_payments_excel(file) -> pd.DataFrame:
    """
    فایل اکسل پرداخت (گردش بانک) را به قالب استاندارد تبدیل می‌کند.

    خروجی حتماً ستون‌های زیر را (در صورت امکان) خواهد داشت:
    - PaymentDate
    - Amount
    - PaymentID
    - CustomerCode
    - CustomerName
    - Description
    - SourceType  (ثابت: CustomerAccount)
    """
    # اول کل شیت را بدون هدر می‌خوانیم
    raw = pd.read_excel(file, header=None)

    header_top_idx = _find_header_top(raw)
    if header_top_idx is None or header_top_idx + 1 >= len(raw):
        raise ValueError(
            "الگوی هدر فایل پرداخت‌ها شناخته نشد. لطفاً نمونه فایل را بررسی کن.")

    header_bottom_idx = header_top_idx + 1

    # دو سطر هدر را با هم ادغام می‌کنیم
    top = raw.iloc[header_top_idx].fillna("").astype(str).str.strip()
    bot = raw.iloc[header_bottom_idx].fillna("").astype(str).str.strip()
    combined_cols: list[str] = []
    for t, b in zip(top, bot):
        tn = _normalize_str(t)
        bn = _normalize_str(b)
        if tn and bn and tn != bn:
            combined_cols.append(f"{tn} {bn}")
        elif tn:
            combined_cols.append(tn)
        else:
            combined_cols.append(bn)

    df = raw.iloc[header_bottom_idx + 1:].copy()
    df.columns = combined_cols
    df = df.reset_index(drop=True)
    df = df.dropna(how="all")  # حذف ردیف‌های کاملاً خالی

    # پیدا کردن اندیس ستون‌های مهم
    date_idx = _find_date_col_idx(df)
    acc_idx = _find_account_code_idx(df)
    cust_idx = _find_customer_name_idx(df)
    desc_idx = _find_desc_idx(df)
    dep_idx, _ = _find_deposit_withdraw_idx(df)
    doc_idx = _find_doc_no_idx(df)

    if dep_idx is None:
        raise ValueError(
            "در هدر فایل پرداخت‌ها ستونی با عنوان «واریزی» پیدا نشد.")

    # حذف ردیف‌هایی که در ستون تاریخ‌شان «جمع» نوشته شده (مثل «جمع نقل از قبل»)
    if date_idx is not None:
        date_series = df.iloc[:, date_idx].astype(str).apply(_normalize_str)
        df = df[~date_series.str.contains("جمع")]

    # ساخت دیتافریم استاندارد
    out = pd.DataFrame()

    if date_idx is not None:
        out["PaymentDate"] = df.iloc[:, date_idx]

    # مبلغ = ستون «واریزی»
    dep_series = df.iloc[:, dep_idx]
    dep_numeric = pd.to_numeric(
        dep_series.astype(str).str.replace(",", ""),
        errors="coerce",
    )
    out["Amount"] = dep_numeric

    if doc_idx is not None:
        out["PaymentID"] = df.iloc[:, doc_idx]

    if acc_idx is not None:
        out["CustomerCode"] = df.iloc[:, acc_idx]

    if cust_idx is not None:
        out["CustomerName"] = df.iloc[:, cust_idx]

    if desc_idx is not None:
        out["Description"] = df.iloc[:, desc_idx]

    # همه‌ی این ردیف‌ها ماهیتاً «واریز از حساب مشتری» هستند
    out["SourceType"] = "CustomerAccount"

    # حذف ردیف‌های بدون مبلغ
    out = out[out["Amount"].notna() & (out["Amount"] != 0)]

    return out.reset_index(drop=True)
