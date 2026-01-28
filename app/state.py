# app/state.py

# متغیر سراسری برای نگهداری وضعیت آپلود فایل‌های اصلی (فروش، پرداخت، چک)
LAST_UPLOAD = {
    "sales": None,
    "payments": None,
    "checks": None,
    "group_col": None,
    "group_config": None,
    "sales_result": None,
    "payments_result": None,
    "history": None,
}

# تنظیمات نشست (Session)
SESSION_SETTINGS = {
    "reactivation_days": 95
}
