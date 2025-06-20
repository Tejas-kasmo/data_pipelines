import pandas as pd
import re

customers = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\us_customer_data 1.csv")
transaction = pd.read_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\transaction_data.csv")

merged_data = customers.merge(transaction, how="outer")

merged_data['phone'] = merged_data['phone'].str.replace(r'[^0-9-]', '', regex = True)

merged_data.loc[merged_data['phone'].str.match(r'^[0-9]{3}-[0-9]{3}-[0-9]{4}$') == False, 'phone'] = None

merged_data.loc[merged_data['email'].isnull(), 'email'] = merged_data['name'] + '@example.com'

merged_data.loc[merged_data['transaction_date'].isnull(), 'transaction_date'] = '2050-12-31'

merged_data.loc[merged_data['product_category'].isnull(), 'product_category'] = 'not_avilable'

merged_data.loc[merged_data['payment_method'].isnull(), 'payment_method'] = 'not_avilabe'

merged_data.loc[merged_data['store_location'].isnull(), 'store_location'] = 'not_avilable'

merged_data['status_number'] = merged_data['']

merged_data['name'] = merged_data['name'].str.title()

high_transaction_data = merged_data[merged_data['amount'] > merged_data['amount'].mean()]

high_transaction_data.to_csv(r"C:\Users\mysur\OneDrive\Desktop\python_tutorial\data\cleaned_data_18_june.csv", index=False)
