# app/services/payments_excel_loader.py
from __future__ import annotations

import pandas as pd
import re
from typing import IO, Any


def _find_first_col(row: pd.Series, keywords: list[str]) -> int | None:
    """
    در یک سطر، اولین ستونی که یکی از کلمات داده‌شده را دارد پیدا می‌کند.
    اگر پیدا نشد، None برمی‌گرداند.
    """
    for idx, val in enumerate(row):
        text = str(val)
        for kw in keywords:
            if kw in text:
                return idx
    return None


def _load_special_bank_layout(file_obj: IO[Any]) -> pd.DataFrame:
    """
    تمیز کردن فرمتی که در نمونه‌ی پرداخت.xlsx فرستادی (دفتر حساب بانکی با هدرهای فارسی چندسطره).
    خروجی: دیتافریمی استاندارد با ستون‌های:
      PaymentID, PaymentDate, Amount, SourceType, CustomerCode, CustomerName, Description
    """
    # فایل را بدون هدر می‌خوانیم که همه‌ی سطرها را داشته باشیم
    file_obj.seek(0)
    raw = pd.read_excel(file_obj, header=None)

    # پیدا کردن سطر متادیتا (جایی که "كد طرف حساب" نوشته شده)
    meta_idx = None
    for i in range(min(40, len(raw))):
        row = raw.iloc[i].astype(str)
        if row.str.contains("كد طرف حساب", na=False).any() or row.str.contains("کد طرف حساب", na=False).any():
            meta_idx = i
            break

    if meta_idx is None:
        # این فرمت نبود
        return pd.DataFrame()

    header2_idx = meta_idx + 1
    if header2_idx >= len(raw):
        return pd.DataFrame()

    meta_row = raw.iloc[meta_idx].astype(str)
    header2_row = raw.iloc[header2_idx].astype(str)

    # پیدا کردن ایندکس ستون‌ها بر اساس متن فارسی
    date_col = _find_first_col(header2_row, ["تاريخ", "تاریخ"])
    type_col = _find_first_col(header2_row, ["نوع"])
    id_col = _find_first_col(header2_row, ["شماره"])
    cust_code_col = _find_first_col(meta_row, ["كد طرف حساب", "کد طرف حساب"])
    cust_name_col = _find_first_col(meta_row, ["واريز يا برداشت كننده", "واریز یا برداشت کننده"])
    deposit_col = _find_first_col(header2_row, ["واريزي", "واریزی"])
    withdraw_col = _find_first_col(header2_row, ["برداشتي", "برداشتی"])
    desc_col = _find_first_col(meta_row, ["توضيحات", "توضیحات"])

    # اگر ستون‌های حیاتی را پیدا نکردیم، ولش کن
    if date_col is None or deposit_col is None:
        return pd.DataFrame()

    # داده‌ها از دو سطر بعد از هدر شروع می‌شوند
    data = raw.iloc[header2_idx + 1 :].copy()

    # تبدیل ستون‌های مبلغ به عدد
    for col_idx in [deposit_col, withdraw_col]:
        if col_idx is not None:
            data[col_idx] = pd.to_numeric(data[col_idx], errors="coerce")

    records: list[dict[str, Any]] = []

    for _, row in data.iterrows():
        # مبلغ واریزی
        amt = float(row[deposit_col]) if pd.notna(row[deposit_col]) else 0.0
        if amt <= 0:
            # فقط ردیف‌هایی که واقعاً واریزی دارند را می‌خواهیم
            continue

        # نوع (برای حذف "جمع ...")
        kind = str(row[type_col]) if type_col is not None else ""
        if "جمع" in kind:
            # سطرهای جمع کل و جمع نقل از قبل و ... را حذف می‌کنیم
            continue

        payment_date = row[date_col] if date_col is not None else None
        cust_code = row[cust_code_col] if cust_code_col is not None else None
        cust_name = row[cust_name_col] if cust_name_col is not None else None
        payment_id = row[id_col] if id_col is not None else None
        desc_text = row[desc_col] if desc_col is not None else None

        has_code = pd.notna(cust_code) and str(cust_code).strip() != ""
        desc_str = str(desc_text or "")

        # فعلاً اگر کد طرف حساب داریم، ساده فرض می‌کنیم واریز مستقیم از حساب مشتری است
        # (در آینده اگر نیاز شد، می‌توانیم "Check" و ارتباط با فایل چک‌ها را هم فعال کنیم)
        if has_code:
            source_type = "CustomerAccount"
        else:
            # اگر کد مشتری نداریم ولی کلمه "چک" در توضیحات بود
            if any(w in desc_str for w in ["چک", "چك"]):
                source_type = "Check"
            else:
                source_type = "Other"

        rec = {
            "PaymentID": str(payment_id).strip() if pd.notna(payment_id) else None,
            "PaymentDate": payment_date,
            "Amount": amt,
            "SourceType": source_type,
            "CustomerCode": str(cust_code).strip() if has_code else None,
            "CustomerName": str(cust_name).strip() if pd.notna(cust_name) else None,
            "Description": desc_str,
        }
        records.append(rec)

    if not records:
        return pd.DataFrame(
            columns=[
                "PaymentID",
                "PaymentDate",
                "Amount",
                "SourceType",
                "CustomerCode",
                "CustomerName",
                "Description",
            ]
        )

    df = pd.DataFrame(records)
    return df.reset_index(drop=True)


def _load_simple_layout(file_obj: IO[Any]) -> pd.DataFrame:
    """
    حالت پشتیبان:
    اگر فایل اصلاً شبیه نمونه‌ی بانکی نبود، فرض می‌کنیم یک اکسل ساده با هدرهای مستقیم است.
    """
    file_obj.seek(0)
    df = pd.read_excel(file_obj)

    # نرمال‌سازی اسامی ستون‌ها
    rename_map = {}
    for col in df.columns:
        name = str(col).strip()
        if name in ["PaymentDate", "تاریخ", "تاريخ", "تاریخ سند", "تاريخ سند"]:
            rename_map[col] = "PaymentDate"
        elif name in ["Amount", "مبلغ", "واريزي", "واریزی", "بستانكار", "بستانکار"]:
            rename_map[col] = "Amount"
        elif name in ["CustomerCode", "کد طرف حساب", "كد طرف حساب", "کد مشتری"]:
            rename_map[col] = "CustomerCode"
        elif name in ["Description", "شرح", "توضيحات", "توضیحات"]:
            rename_map[col] = "Description"
        elif name in ["PaymentID", "شماره سند", "شماره", "شماره تراکنش"]:
            rename_map[col] = "PaymentID"

    df = df.rename(columns=rename_map)

    for c in ["PaymentID", "PaymentDate", "Amount", "CustomerCode", "Description"]:
        if c not in df.columns:
            df[c] = None

    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df[df["Amount"] > 0]

    # پیش‌فرض: پرداخت از حساب مشتری
    df["SourceType"] = "CustomerAccount"

    cols = ["PaymentID", "PaymentDate", "Amount", "SourceType", "CustomerCode", "Description"]
    return df[cols].reset_index(drop=True)


def load_payments_excel(file_obj: IO[Any]) -> pd.DataFrame:
    """
    لودر اصلی پرداخت‌ها:
    - اول تلاش می‌کند فرمت ویژه‌ی دفتر حساب بانکی (مثل پرداخت.xlsx) را تشخیص دهد.
    - اگر نشد، می‌رود روی حالت ساده با هدر معمولی.
    """
    # اول سعی می‌کنیم فرمت بانکی را بخوانیم
    df_special = _load_special_bank_layout(file_obj)
    if not df_special.empty:
        return df_special

    # اگر جواب نداد، می‌رویم سراغ حالت ساده
    return _load_simple_layout(file_obj)
