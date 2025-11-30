# app/services/sales_excel_loader.py
from __future__ import annotations

import io
from typing import BinaryIO

import pandas as pd
import numpy as np


def load_sales_excel(file_obj: BinaryIO) -> pd.DataFrame:
    """
    خواندن اکسل فروش (نمونه‌ای که فرستادی) و تبدیلش به یک DataFrame تمیز
    با ستون‌های استاندارد مثل:
    InvoiceDate, InvoiceID, CustomerCode, CustomerName, ProductCode, ProductName, Amount, Salesperson, ...

    ورودی: file_obj همون sales_file.file از FastAPI است.
    """

    # کل فایل رو می‌خوانیم تا هر چند بار لازم شد از روی BytesIO بخونیم
    data = file_obj.read()
    # اگر جای دیگه هم خواستی از همین UploadFile استفاده کنی، برگردونش اول فایل
    file_obj.seek(0)

    # یک بار با header=None می‌توانستیم ساختار را کشف کنیم،
    # ولی چون ساختار فعلی مشخصه، مستقیم از ردیف ۵ به بعد هدر را می‌گیریم:
    # (ردیفی که ستون‌های "تاريخ ", "نوع", "شماره", ... توش هستند)
    buf = io.BytesIO(data)
    df = pd.read_excel(buf, header=5)

    # فقط ردیف‌هایی که نوعشان "فاكتور" است (خود فاکتورهای واقعی)
    if "نوع" in df.columns:
        df = df[df["نوع"].astype(str).str.strip() == "فاكتور"].copy()

    # نگاشت ستون‌های فارسی به ستون‌های استاندارد
    rename_map = {
        "تاريخ ": "InvoiceDate",
        "نوع": "InvoiceType",
        "شماره": "InvoiceID",
        "بازارياب": "Salesperson",
        "نماينده فروش": "SalesAgent",
        "كد": "CustomerCode",
        "شرح": "CustomerName",
        "نام انبار": "Warehouse",
        "كد.1": "ProductCode",
        "شرح.1": "ProductName",
        "واحد كالا": "UnitName",
        "مقدار": "Quantity",
        "بهاي واحد": "UnitPrice",
        "مبلغ": "Amount",
    }
    # فقط ستون‌هایی که واقعا وجود دارند را rename کنیم
    df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns},
        inplace=True,
    )

    # تمیز کردن مبلغ‌ها (حذف کاما و تبدیل به float)
    if "Amount" in df.columns:
        df["Amount"] = (
            df["Amount"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace("", np.nan)
        )
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0)

    # مقدار (Quantity) را هم اگر بود عددی کنیم
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(
            df["Quantity"], errors="coerce").fillna(0.0)

    # شناسه‌ها را اگر لازم بود عددی کنیم (اختیاری – می‌توانی به صورت رشته هم نگه‌شان داری)
    for col in ["InvoiceID", "CustomerCode", "ProductCode"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

    # ⚠️ نکته درباره تاریخ‌ها:
    # این تاریخ‌ها الان شمسی هستند (مثل 1404/08/01) و اگر مستقیم به datetime تبدیلشان کنیم
    # pandas ارور OutOfBoundsDatetime می‌دهد. پس فعلاً همان رشته بمانند
    # و بعداً در یک مرحله جدا با کتابخانه‌ای مثل jdatetime یا khayyam به میلادی تبدیلشان می‌کنیم.
    # اگر به هر حال خواستی تست کنی، می‌توانی این را باز کنی (با errors="coerce"):
    #
    # if "InvoiceDate" in df.columns:
    #     df["InvoiceDate_parsed"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    return df
