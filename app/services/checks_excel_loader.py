from __future__ import annotations

from typing import IO, Any
import pandas as pd


def load_checks_excel(file_obj: IO[Any]) -> pd.DataFrame:
    """
    خواندن فایل «لیست کليه اسناد دريافتني» (چک ها.xlsx) و تبدیل آن به دیتافریم استاندارد.

    خروجی حداقل این ستون‌ها را دارد:
      - CheckNumber : شماره چک (فقط رقم، بدون صفرهای اضافه‌ی اول)
      - CustomerName: صاحب حساب چک
    """
    file_obj.seek(0)
    raw = pd.read_excel(file_obj, header=None)

    # پیدا کردن ردیف هدر (جایی که "رديف چك" نوشته شده)
    header_idx = None
    for i in range(min(40, len(raw))):
        row = raw.iloc[i].astype(str)
        if row.str.contains("رديف چك", na=False).any() or row.str.contains("ردیف چک", na=False).any():
            header_idx = i
            break

    if header_idx is None:
        return pd.DataFrame()

    header = raw.iloc[header_idx]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = header
    df = df.dropna(how="all")

    # مپ کردن اسم ستون‌ها به اسامی استاندارد
    rename_map = {}
    for col in df.columns:
        name = str(col).strip()
        if name in ["رديف چك", "ردیف چک"]:
            rename_map[col] = "CheckIndex"
        elif name in ["شماره/سريال چك", "شماره/سریال چک"]:
            rename_map[col] = "CheckSerial"
        elif name in ["صاحب حساب"]:
            rename_map[col] = "CustomerName"

    if rename_map:
        df = df.rename(columns=rename_map)

    # ساختن CheckNumber از روی شماره سریال (یا در صورت نبود، از روی ردیف چک)
    check_source = None
    if "CheckSerial" in df.columns:
        check_source = df["CheckSerial"]
    elif "CheckIndex" in df.columns:
        check_source = df["CheckIndex"]

    if check_source is not None:
        check_numbers = (
            check_source.astype(str)
            .str.replace(r"\\D", "", regex=True)  # فقط رقم
            .str.lstrip("0")                      # حذف صفرهای ابتدایی
        )
        df["CheckNumber"] = check_numbers
        df = df[df["CheckNumber"] != ""]

    return df.reset_index(drop=True)
