from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=True)
    address = Column(String, nullable=True)

    invoices = relationship('Invoice', back_populates='customer')
    payments = relationship('Payment', back_populates='customer')

class Invoice(Base):
    __tablename__ = 'invoices'

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    amount = Column(Float, nullable=False)
    priority = Column(String, nullable=False)
    commission_percent = Column(Float, nullable=False)
    status = Column(String, nullable=False)

    customer_id = Column(Integer, ForeignKey('customers.id'))
    customer = relationship('Customer', back_populates='invoices')

    payments = relationship('Payment', back_populates='invoice')

class Payment(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    amount = Column(Float, nullable=False)
    payment_type = Column(String, nullable=False)
    status = Column(String, nullable=False)

    customer_id = Column(Integer, ForeignKey('customers.id'))
    customer = relationship('Customer', back_populates='payments')

    invoice_id = Column(Integer, ForeignKey('invoices.id'))
    invoice = relationship('Invoice', back_populates='payments')

class Check(Base):
    __tablename__ = 'checks'

    id = Column(Integer, primary_key=True)
    check_number = Column(String, nullable=False)
    due_date = Column(Date, nullable=False)
    status = Column(String, nullable=False)

    customer_id = Column(Integer, ForeignKey('customers.id'))
    customer = relationship('Customer')
