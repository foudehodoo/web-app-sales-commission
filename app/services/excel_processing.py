from app.models import Customer, Invoice, Payment
from app.models.database import SessionLocal

def process_sales_data(df):
    db = SessionLocal()
    for index, row in df.iterrows():
        customer_name = row["Customer"]
        customer = db.query(Customer).filter(Customer.name == customer_name).first()
        if not customer:
            customer = Customer(name=customer_name)
            db.add(customer)
            db.commit()

        invoice = Invoice(date=row["Date"], amount=row["Amount"], priority=row["Priority"], commission_percent=row["Commission Percent"], status="open", customer=customer)
        db.add(invoice)
        db.commit()

def process_payments_data(df):
    db = SessionLocal()
    for index, row in df.iterrows():
        customer_name = row["Customer"]
        customer = db.query(Customer).filter(Customer.name == customer_name).first()
        if not customer:
            customer = Customer(name=customer_name)
            db.add(customer)
            db.commit()

        payment = Payment(date=row["Date"], amount=row["Amount"], payment_type=row["Payment Type"], status=row["Status"], customer=customer)
        db.add(payment)
        db.commit()
