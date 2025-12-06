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
      PaymentID, PaymentDate, Amount, SourceType, CustomerCode, CustomerName, Description, CheckNumber
    """
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
        # اصلاً این فرمتی نیست که انتظار داریم
        return pd.DataFrame()

    header2_idx = meta_idx + 1
    if header2_idx >= len(raw):
        return pd.DataFrame()

    meta_row = raw.iloc[meta_idx].astype(str)
    header2_row = raw.iloc[header2_idx].astype(str)

    # ستون‌های تاریخ (دو تا: تاریخ اصلی و تاریخ مدرک واریز/برداشت)
    date_positions = [
        i for i, v in enumerate(header2_row)
        if any(k in str(v) for k in ["تاريخ", "تاریخ"])
    ]
    date_col = date_positions[0] if date_positions else _find_first_col(
        header2_row, ["تاريخ", "تاریخ"])
    # فعلاً استفاده نمی‌کنیم، ولی نگهش می‌داریم
    sub_date_col = date_positions[1] if len(date_positions) > 1 else None

    # ستون‌های نوع (نوع عملیات اصلی و نوع مدرک واریز/برداشت)
    type_positions = [i for i, v in enumerate(header2_row) if "نوع" in str(v)]
    type_col = type_positions[0] if type_positions else _find_first_col(header2_row, [
                                                                        "نوع"])
    sub_type_col = type_positions[1] if len(type_positions) > 1 else None

    # ستون‌های شماره (شماره مدرک اصلی و شماره چک)
    id_positions = [i for i, v in enumerate(header2_row) if "شماره" in str(v)]
    id_col = id_positions[0] if id_positions else _find_first_col(header2_row, [
                                                                  "شماره"])
    check_no_col = id_positions[1] if len(id_positions) > 1 else None

    cust_code_col = _find_first_col(meta_row, ["كد طرف حساب", "کد طرف حساب"])
    cust_name_col = _find_first_col(
        meta_row, ["واريز يا برداشت كننده", "واریز یا برداشت کننده"])
    deposit_col = _find_first_col(header2_row, ["واريزي", "واریزی"])
    withdraw_col = _find_first_col(header2_row, ["برداشتي", "برداشتی"])
    desc_col = _find_first_col(meta_row, ["توضيحات", "توضیحات"])

    # بدون تاریخ و ستون واریزی، عملاً به درد ما نمی‌خورد
    if date_col is None or deposit_col is None:
        return pd.DataFrame()

    # دیتای واقعی از سطر بعد از هدر دوم شروع می‌شود
    data = raw.iloc[header2_idx + 1:].copy()

    # تبدیل مبالغ به عدد
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

        # نوع عملیات اصلی (برای حذف "جمع ..." و تشخیص اسناد دریافتنی)
        kind = str(row[type_col]) if type_col is not None else ""
        if "جمع" in kind:
            # سطرهای "جمع نقل از قبل" و "جمع ..." را حذف می‌کنیم
            continue

        payment_date = row[date_col] if date_col is not None else None
        payment_id = row[id_col] if id_col is not None else None
        cust_code = row[cust_code_col] if cust_code_col is not None else None
        cust_name = row[cust_name_col] if cust_name_col is not None else None
        desc_text = row[desc_col] if desc_col is not None else None

        has_code = pd.notna(cust_code) and str(cust_code).strip() != ""
        desc_str = str(desc_text or "")

        # استخراج شماره چک از ستون "شماره" دوم (ستون 8 در فایل تو)
        check_number = None
        if check_no_col is not None:
            raw_chk = row[check_no_col]
            if pd.notna(raw_chk):
                check_number = re.sub(r"\D", "", str(raw_chk))
                if check_number == "":
                    check_number = None

        # تشخیص این‌که این ردیف «وصول چک / عملیات روی اسناد دریافتنی» است یا نه
        is_check_row = False

        # حالت ۱: متن نوع عملیات اصلی شامل "اسناد دريافتني" باشد
        if "اسناد دريافتني" in kind:
            is_check_row = True

        # حالت ۲: نوع مدرک واریز/برداشت (ستون نوع دوم) "چک" باشد
        if sub_type_col is not None:
            sub_kind = str(row[sub_type_col])
            if any(w in sub_kind for w in ["چک", "چك"]):
                is_check_row = True

        # حالت ۳: در توضیحات بنویسد "وصول چک"
        if any(w in desc_str for w in ["وصول چک", "وصول چك"]):
            is_check_row = True

        if is_check_row and check_number:
            source_type = "Check"
        else:
            # منطق قبلی: اگر کد طرف حساب داریم، فرض می‌کنیم واریز مستقیم از حساب مشتری است
            if has_code:
                source_type = "CustomerAccount"
            else:
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
            "CheckNumber": check_number,
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
                "CheckNumber",
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
        if name in ["PaymentDate", "تاریخ", "تاريخ"]:
            rename_map[col] = "PaymentDate"
        elif name in ["Amount", "مبلغ", "مبلغ واریز"]:
            rename_map[col] = "Amount"
        elif name in ["CustomerCode", "کد مشتری", "كد مشتری"]:
            rename_map[col] = "CustomerCode"
        elif name in ["CustomerName", "نام مشتری", "طرف حساب"]:
            rename_map[col] = "CustomerName"
        elif name in ["Description", "شرح", "توضیحات", "توضيحات"]:
            rename_map[col] = "Description"

    if rename_map:
        df = df.rename(columns=rename_map)

    # اگر ستون نوع منبع را نداریم، یک ستون پیش‌فرض می‌گذاریم
    if "SourceType" not in df.columns:
        df["SourceType"] = "CustomerAccount"

    # اگر ستون شماره چک را نداریم، ولی ستونی شبیه آن هست، rename کن
    if "CheckNumber" not in df.columns:
        for col in df.columns:
            if str(col).strip().lower() in ["checknumber", "check_no", "cheque_no"]:
                df = df.rename(columns={col: "CheckNumber"})
                break

    # در نهایت مطمئن می‌شویم همه‌ی ستون‌های لازم وجود دارند
    for col in [
        "PaymentID",
        "PaymentDate",
        "Amount",
        "SourceType",
        "CustomerCode",
        "CustomerName",
        "Description",
        "CheckNumber",
    ]:
        if col not in df.columns:
            df[col] = None

    return df[
        [
            "PaymentID",
            "PaymentDate",
            "Amount",
            "SourceType",
            "CustomerCode",
            "CustomerName",
            "Description",
            "CheckNumber",
        ]
    ]


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
