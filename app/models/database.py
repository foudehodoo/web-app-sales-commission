from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# تنظیمات دیتابیس (مثلاً استفاده از SQLite برای توسعه)
DATABASE_URL = "sqlite:///./sales_commission.db"  # مسیر دیتابیس شما می‌تواند تغییر کند

# ایجاد engine
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

# ایجاد session local برای ارتباط با دیتابیس
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ایجاد session در صورت نیاز
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
