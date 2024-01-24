import yaml
import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine

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
cust_demo = cust_demo.drop_duplicates()

cust_demo.loc[cust_demo['credit_card_provider'] == 'VISA 13 digit', 'credit_card_provider'] = 'VISA 16 digit'

cust_demo.reset_index(inplace = True, drop=True)
cust_demo = cust_demo.drop('customer_id', axis=1)
cust_demo.index.name = 'customer_id'
cust_demo.loc[(cust_demo['credit_card_number'].astype(str).str.len() > 16) | (cust_demo['credit_card_number'].astype(str).str.len() < 15), 'credit_card_number'] = None

cust_stat = pd.read_csv('data_files/customer_statistics.csv')
cust_stat = cust_stat.drop_duplicates()


orders = pd.read_csv('data_files/orders (1).csv')
orders = orders.drop_duplicates()

def phone_cleanup(df):
    extensions = df['phone_number'].str.extract(r'x(.+)', expand=False)
    extensions = extensions.fillna('')
    df['phone_number'] = df['phone_number'].str.replace(r'x(.+)', '')
    df['phone_number'] = df['phone_number'].str.replace(r'\D', '')
    for i in range(len(df.phone_number)):
        if isinstance(df.phone_number[i], str) and df.phone_number[i] is not None:
            number = ''.join(filter(str.isdigit, df.phone_number[i]))
            if len(number) == 10:
                df.phone_number[i] = f'({number[0:3]}) {number[3:6]}-{number[6:10]}'
            elif len(number) == 11:
                df.phone_number[i] = f'+{number[0]} ({number[1:4]}) {number[4:7]}-{number[7:11]}'
            elif len(number) == 12:
                df.phone_number[i] = f'+{number[0:1]} ({number[2:5]}) {number[5:8]}-{number[8:12]}'
            elif len(number) == 13:
                df.phone_number[i] = f'+{number[0:2]} ({number[3:6]}) {number[6:9]}-{number[9:13]}'
            if str(extensions[i]).isdigit and extensions[i] != '':
                df.phone_number[i] = df.phone_number[i] + f' x {extensions[i]}'
            else:
                pass
phone_cleanup(cust_demo)




merged_data = pd.merge(cust_demo, cust_stat, how='left', on='customer_id')
merged_data = pd.merge(merged_data, orders, how='left', on="customer_id")
merged_data.drop_duplicates(inplace=True)

#cleaning credit card numbers update
df = merged_data.dropna(subset=['credit_card_number'])
df['credit_card_number'] = df['credit_card_number'].astype(str).str.extract('(\d{15,19})')
df = df.dropna(subset=['credit_card_number'])

#Checking for NULL values in the customer stats dataframe
#RESULT: No NULLS were found
pd.isnull(cust_stat).sum()
#Checking for NULL values in the orders dataframe
#RESULT: NONE were found.
pd.isnull(orders).sum()
#Checking for NULL values in the customer demographics dataframe
#RESULT: Found multiple NULLS for address, credit card, email, and phone
null_count_demo = pd.isnull(cust_demo).sum()
null_count_demo





#checking for any outlier in the items column. Item count of ZERO should be considered in error.
#RESULT: No items found less than ONE.
rslt_df = orders.sort_values(by = 'items')
rslt_df.head()

#checking for outlier of amounts considered too great for item count.
#RESULT: No items found greater than TEN.
rslt_df.iloc[-5:]

#checking for total values that don't fit the expectation of the data
#RESULT: No totals found less than $100, which is in tolerance
rslt_df = orders.sort_values(by = 'total')
rslt_df.head()

#checking for outlier totals that would exceed expected amount
#RESULT: No totals found greater than $1000, which is in tolerance
rslt_df.iloc[-5:]

#cheking the number of invalid email entries against NULL values
#RESULT: All emails that are entered are in proper format
cust_email = cust_demo_df["email"].str.match(r"^.+@.+\..{2,}$")
count1=0
count=0
for i in cust_email:
    if i == True:
        count +=1
    else:
        count1 +=1
email_null_count = pd.isnull(cust_demo_df['email']).sum()
print(f'There are {count} emails that are valid')
print(f"There are {email_null_count} emails with NULL values")
print(f"There are {count1 - email_null_count} *NON NULL* emails that aren't valid")






orders_f = df[['order_id', 'customer_id', 'items', 'aperitifs', 'appetizers', 'entrees', 'desserts', 'total']]
CC_df_f = df[["credit_card_expires", "credit_card_number", "credit_card_provider", "credit_card_security_code"]]
cust_info_f = df[["name", "phone_number", "address", "city", "state", "zip_code"]]
cust_stats_f = df[["customer_id", "total_orders", "total_items", "total_spent"]]





engine = create_engine('sqlite:///PEP1_db.db')
orders_f.to_sql('orders', con=engine, index=False, if_exists='replace')
CC_df_f.to_sql('credit_cards', con=engine, index=False, if_exists='replace')
cust_info_f.to_sql('customer_info', con=engine, index=False, if_exists='replace')
cust_stats_f.to_sql('customer_stats', con=engine, index=False, if_exists='replace')

# SQLite database connection string
engine = create_engine('sqlite:///PEP1_db.db')
# Query the data from the database into a DataFrame
table_name = 'orders'
query = f'SELECT * FROM {table_name}'
orders_df = pd.read_sql(query, con=engine)
table_name = 'credit_cards'
query = f'SELECT * FROM {table_name}'
CC_df = pd.read_sql(query, con=engine)
table_name = 'customer_info'
query = f'SELECT * FROM {table_name}'
cust_info = pd.read_sql(query, con=engine)
table_name = 'customer_stats'
query = f'SELECT * FROM {table_name}'
cust_stats = pd.read_sql(query, con=engine)
