import pandas as pd
import re
import pyodbc 
from sqlalchemy import create_engine

customers = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\us_customer_data.csv")
transaction = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\transaction_data.csv")
orders = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\order_data.csv")
country_code = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\Country_2-3_Digits_Codes.csv")

customers['address'] = customers['address'].str.replace(',', '-', regex=False)

customers[['first_name', 'last_name']] = customers['name'].str.split(' ', n=1, expand= True)
customers.rename(columns={'name': 'full_name'}, inplace=True)

customers['loyalty_status_number'] = customers['loyalty_status'].map({
    'Gold' : 2,
    'Silver' : 1,
    'Bronze' : 0
})

customers['country_code'] = customers['address'].str.extract(r'(\s[A-Z]{2}\s)')

customers['dialing_code'] = None
code_to_dialing = country_code.set_index('A2')['DIALINGCODE'].to_dict()
customers['dialing_code'] = customers['country_code'].map(code_to_dialing)

customers['phone'] = customers['phone'].str.replace(r'[^0-9-]', '', regex = True)

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=TEJU;'
    'DATABASE=python_practice;'
    'UID=sa;'
    'PWD=Kasmo@123'
)

Unified_Customer_View = customers.merge(transaction)
Unified_Customer_View = Unified_Customer_View.merge(orders)

engine = create_engine(
    "mssql+pyodbc://TEJU/python_practice?driver=ODBC+Driver+17+for+SQL+Server"
)

Unified_Customer_View.to_sql(
    'customers_unified_orders',
    con=engine,
    if_exists='replace',
    index=False
)
