from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

# ایمپورت کردن روترها
from app.api import routes_commission, routes_balances, routes_utils
from app.services.helpers import format_number

# ------------------ تنظیمات اولیه برنامه ------------------
app = FastAPI()

# تعیین مسیر پایه پروژه
# چون main.py در پوشه app است، parent خودش یعنی همان پوشه app
BASE_DIR = Path(__file__).resolve().parent

# تنظیمات فایل‌های استاتیک (CSS, JS)
# مسیر: app/static
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# تنظیمات قالب‌ها (HTML)
# مسیر: app/templates
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ثبت فیلتر فرمت عدد در جینجا
templates.env.filters["format_number"] = format_number

# ------------------ تزریق Templates به روترها ------------------
# مقداردهی متغیر templates در فایل‌های routes
routes_commission.templates = templates
routes_balances.templates = templates
routes_utils.templates = templates

# ------------------ ثبت روترها ------------------
app.include_router(routes_commission.router)
app.include_router(routes_balances.router)
app.include_router(routes_utils.router)

# ------------------ Health Check ------------------


@app.get("/health", response_class=HTMLResponse)
async def health_check(request: Request):
    return "<h1>System is Online ✅</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
