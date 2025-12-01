# app/services/payments_excel_loader.py

import pandas as pd
import re


def _normalize_header(s: str) -> str:
    """
    نرمال‌سازی نام ستون‌ها:
    - حذف فاصله اضافی
    - یکسان‌سازی ي/ی و ك/ک
    - حروف کوچک
    """
    s = str(s).strip()
    s = s.replace("ي", "ی").replace("ك", "ک")
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def load_payments_excel(file) -> pd.DataFrame:
    """
    لودر استاندارد برای اکسل پرداخت‌ها.
    سعی می‌کنیم ستون‌های مهم را به نام‌های استاندارد زیر برگردانیم:

    PaymentDate  : تاریخ پرداخت
    Amount       : مبلغ
    CustomerCode : کد مشتری / حساب
    CustomerName : نام مشتری (ستون «واريز يا برداشت كننده» این‌جا می‌آید)
    Description  : شرح / توضیح
    """
    df = pd.read_excel(file)

    if df.empty:
        return df

    # نگاشت نام ستون‌های خام → نام استاندارد
    col_map = {}

    for col in df.columns:
        raw = str(col)
        norm = _normalize_header(raw)

        # تاریخ پرداخت
        if any(x in norm for x in ["تاریخ", "تاريخ"]):
            # فقط اولین ستونی که تاریخ است را PaymentDate می‌کنیم
            if "PaymentDate" not in col_map.values():
                col_map[raw] = "PaymentDate"
                continue

        # مبلغ
        if "مبلغ" in norm and "Amount" not in col_map.values():
            col_map[raw] = "Amount"
            continue

        # کد مشتری / حساب
        if any(x in norm for x in ["کد", "كد"]) and any(
            x in norm for x in ["مشتری", "مشتري", "طرف حساب", "طرف‌حساب", "حساب"]
        ):
            if "CustomerCode" not in col_map.values():
                col_map[raw] = "CustomerCode"
                continue

        # نام مشتری – این‌جا مهم‌ترین بخش برای توست
        # «واريز يا برداشت كننده» / «واریز یا برداشت کننده»
        if any(x in norm for x in ["واریز یا برداشت کننده", "واريز يا برداشت كننده", "واريز يا برداشت کننده"]):
            col_map[raw] = "CustomerName"
            continue

        # توضیحات
        if any(x in norm for x in ["شرح", "توضیح", "توضيحات"]):
            if "Description" not in col_map.values():
                col_map[raw] = "Description"
                continue

    # rename بر اساس نگاشت
    df = df.rename(columns=col_map)

    # اگر ستون CustomerName هنوز ساخته نشده بود، یک تلاش دیگر
    if "CustomerName" not in df.columns:
        for col in df.columns:
            norm = _normalize_header(col)
            if "واریز" in norm or "برداشت" in norm:
                df["CustomerName"] = df[col]
                break

    return df
