from fastapi import APIRouter, File, UploadFile
import pandas as pd
from app.services.excel_processing import process_sales_data

router = APIRouter()

@router.post("/upload/")
async def upload_sales(file: UploadFile = File(...)):
    content = await file.read()
    df = pd.read_excel(content)
    process_sales_data(df)  # تابع پردازش داده‌های اکسل
    return {"message": "Sales data uploaded successfully"}
