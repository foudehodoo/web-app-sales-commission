from fastapi import APIRouter, File, UploadFile
import pandas as pd
from app.services.excel_processing import process_payments_data

router = APIRouter()

@router.post("/upload/")
async def upload_payments(file: UploadFile = File(...)):
    content = await file.read()
    df = pd.read_excel(content)
    process_payments_data(df)  # تابع پردازش داده‌های اکسل
    return {"message": "Payment data uploaded successfully"}
