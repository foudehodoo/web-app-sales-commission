# app/services/checks_excel_loader.py
from __future__ import annotations
from typing import IO, Any
import pandas as pd


def load_checks_excel(file_obj: IO[Any]) -> pd.DataFrame:
    print("--- DEBUG: Starting load_checks_excel ---")

    file_obj.seek(0)
    try:
        raw = pd.read_excel(file_obj, header=None)
        print(f"DEBUG: Excel file loaded. Shape: {raw.shape}")
    except Exception as e:
        print(f"ERROR: Failed to read excel file: {e}")
        return pd.DataFrame()

    # پیدا کردن ردیف هدر
    header_idx = None
    for i in range(min(40, len(raw))):
        row = raw.iloc[i].astype(str)
        # چاپ ردیف‌ها برای بررسی چشمی در کنسول
        # print(f"Checking row {i}: {row.tolist()}")
        if row.str.contains("رديف چك", na=False).any() or row.str.contains("ردیف چک", na=False).any():
            header_idx = i
            print(f"DEBUG: Header found at row index: {header_idx}")
            break

    if header_idx is None:
        print("ERROR: Header 'ردیف چک' not found in first 40 rows.")
        return pd.DataFrame()

    header = raw.iloc[header_idx]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = header
    df = df.dropna(how="all")

    print(f"DEBUG: Original Columns found: {df.columns.tolist()}")

    # مپ کردن اسم ستون‌ها
    rename_map: dict[Any, str] = {}
    for col in df.columns:
        name = str(col).strip()
        if name in ("رديف چك", "ردیف چک"):
            rename_map[col] = "CheckIndex"
        elif name in ("شماره/سريال چك", "شماره/سریال چک"):
            rename_map[col] = "CheckSerial"
        elif name in ("صاحب حساب",):
            rename_map[col] = "CustomerName"
        elif name in ("نام طرف حساب",):
            rename_map[col] = "AccountName"
        elif name in ("سررسيد", "تاريخ سررسيد", "تاریخ سررسید"):
            rename_map[col] = "DueDate"
        elif name in ("مبلغ", "مبلغ چك", "مبلغ چک"):
            rename_map[col] = "Amount"
        elif name in ("وضعيت", "وضعیت"):
            rename_map[col] = "Status"

    print(f"DEBUG: Rename Map created: {rename_map}")

    if rename_map:
        df = df.rename(columns=rename_map)
    else:
        print("WARNING: No columns were mapped! Check column names in Excel.")

    # چک کردن وجود ستون‌های حیاتی
    required_cols = ["Status", "Amount", "AccountName"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(
            f"CRITICAL WARNING: Missing columns: {missing}. Logic will fail.")

    # تمیزکاری داده‌ها
    if "CheckSerial" in df.columns or "CheckIndex" in df.columns:
        # منطق ساخت شماره چک (اختیاری برای محاسبه مانده، ولی برای نمایش خوب است)
        pass

    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0)

    # چاپ نمونه وضعیت‌ها
    if "Status" in df.columns:
        unique_statuses = df["Status"].unique()
        print(f"DEBUG: Unique Statuses found: {unique_statuses}")

    print(f"DEBUG: Final DataFrame rows: {len(df)}")
    print("--- DEBUG: End load_checks_excel ---")

    return df.reset_index(drop=True)
