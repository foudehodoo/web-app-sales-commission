# app/services/checks_excel_loader.py
from __future__ import annotations

import pandas as pd
from typing import IO, Any


def _load_special_checks_layout(file_obj: IO[Any]) -> pd.DataFrame:
    """
    خواندن فرمت «لیست کلیه اسناد دریافتنی» که فرستادی.
    از روی سطر هدر فارسی (رديف چك / صاحب حساب / شماره/سريال چك / ...) 
    یک دیتافریم تمیز می‌سازیم با ستون‌های استاندارد:
      CheckNumber, CustomerName, Amount, DueDate, ...
    """
    file_obj.seek(0)
    raw = pd.read_excel(file_obj, header=None)

    # پیدا کردن سطر هدر (جایی که "رديف چك" نوشته شده)
    header_idx: int | None = None
    for i in range(min(40, len(raw))):
        row = raw.iloc[i]
        texts = [str(v).strip() for v in row.values]
        if "رديف چك" in texts or "ردیف چک" in texts:
            header_idx = i
            break

    if header_idx is None:
        return pd.DataFrame()

    header_row = raw.iloc[header_idx]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = header_row
    df = df.dropna(how="all")

    # مپ کردن اسم ستون‌های فارسی به اسم‌های استاندارد
    col_map: dict[Any, str] = {}
    for col in df.columns:
        name = str(col).strip()
        if name in ("رديف چك", "ردیف چک"):
            col_map[col] = "CheckIndex"
        elif name in ("شماره/سريال چك", "شماره/سریال چک"):
            col_map[col] = "CheckSerial"
        elif name in ("صاحب حساب",):
            col_map[col] = "CustomerName"
        elif name in ("مبلغ", "مبلغ چك", "مبلغ چک"):
            col_map[col] = "Amount"
        elif name in ("سررسيد", "تاريخ سررسيد", "تاريخ پاس", "تاریخ پاس"):
            col_map[col] = "DueDate"

    df = df.rename(columns=col_map)

    # ساختن ستون CheckNumber (کلید اتصال) از روی ردیف چک یا شماره/سریال
    base = None
    if "CheckIndex" in df.columns:
        base = df["CheckIndex"]
    elif "CheckSerial" in df.columns:
        base = df["CheckSerial"]

    if base is not None:
        check_number = base.astype(str).str.strip()
        # فقط رقم‌ها (مثلاً 1750 از "001750")
        check_number = check_number.str.replace(r"\D", "", regex=True)
        df["CheckNumber"] = check_number

    if "CheckNumber" in df.columns:
        df = df[df["CheckNumber"].astype(str).str.strip() != ""]

    return df


def _load_simple_checks_layout(file_obj: IO[Any]) -> pd.DataFrame:
    """
    اگر فایل چک‌ها ساده بود (هدر انگلیسی یا فارسی تمیز داشت)
    همین‌طوری می‌خوانیم و فقط اسم ستون‌ها را نرمال می‌کنیم.
    """
    file_obj.seek(0)
    df = pd.read_excel(file_obj)

    col_map: dict[Any, str] = {}
    for col in df.columns:
        name = str(col).strip()
        if name in ("شماره/سريال چك", "شماره/سریال چک", "CheckNumber"):
            col_map[col] = "CheckNumber"
        elif name in ("صاحب حساب", "نام طرف حساب", "CustomerName"):
            col_map[col] = "CustomerName"
        elif name in ("کد مشتری", "كد مشتری", "CustomerCode"):
            col_map[col] = "CustomerCode"

    if col_map:
        df = df.rename(columns=col_map)

    return df


def load_checks_excel(file_obj: IO[Any]) -> pd.DataFrame:
    """
    لودر اصلی چک‌ها؛ اول تلاش می‌کند فرمت «لیست اسناد دریافتنی» را تشخیص دهد،
    اگر نشد، می‌رود سراغ حالت ساده.
    """
    df_special = _load_special_checks_layout(file_obj)
    if not df_special.empty:
        return df_special

    return _load_simple_checks_layout(file_obj)
