import yaml
import pandas as pd
import numpy as np
import sqlite3
# here we can add all the data cleaning methods to have one file cleaning and merging data
customer_demo = 'data_files/customer_demographics.yaml'
# Load YAML data from a file
with open(customer_demo, 'r') as yaml_file:
    yaml_data = yaml.safe_load(yaml_file)

# Create a list of dictionaries with the desired column names
data_list = []
for customer_id, details in yaml_data.items():
    data_list.append({
        'address': details.get('address'),
        'city': details.get('city'),
        'credit_card_expires': details.get('credit_card_expires'),
        'credit_card_number': details.get('credit_card_number'),
        'credit_card_provider': details.get('credit_card_provider'),
        'credit_card_security_code': details.get('credit_card_security_code'),
        'customer_id': details.get('customer_id'),
        'email': details.get('email'),
        'name': details.get('name'),
        'phone_number': details.get('phone_number'),
        'state': details.get('state'),
        'zip_code': details.get('zip_code'),
    })

# Convert the list of dictionaries to Pandas DataFrame
cust_demo = pd.DataFrame(data_list)
customer_data = cust_demo.drop_duplicates()

cust_stat = pd.read_csv('data_files/customer_statistics.csv')
customer_stats = cust_stat.drop_duplicates()

orders = pd.read_csv('data_files/orders (1).csv')
new_orders = orders.drop_duplicates()

merged_data = cust_demo.merge(cust_stat, left_index=True, right_index=True)
merged_data.drop_duplicates(inplace=True)

merged_data['credit_card_number'] = pd.to_numeric(merged_data['credit_card_number'], errors='coerce')
merged_data_cleaned = merged_data.dropna(subset=['credit_card_number'])

merged_data_cleaned['credit_card_number'] = merged_data_cleaned['credit_card_number'].astype(str).str.extract('(\d{15,19})')

merged_data_cleaned = merged_data_cleaned.dropna(subset=['credit_card_number'])
