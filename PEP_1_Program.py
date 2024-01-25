"""
The purpose of this program will be to load data from .csv files into panda dataframes. After analysis and transforming of the data it will then load the data into a 
database file, along with creation of custom .csv files identifying records with NULL values. This program also holds user interaction to SELECT, INSERT, and DELETE 
records in the newly created database. 

Authors: Team 3
Date: 1/25/2024
File: PEP_1_Program.py

"""

import yaml
import pandas as pd
import numpy as np
import sqlite3
import sqlalchemy
from sqlalchemy import create_engine

#open yaml file 
with open('c:/Users/fearn/JUMP/PEP1/customer_demographics.yaml', 'r') as yaml_file:
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

#create new index for customer demo table, drop unnecessary column, and rename new index
cust_demo.reset_index(inplace = True, drop=True)
cust_demo = cust_demo.drop('customer_id', axis=1)
cust_demo.index.name = 'customer_id'

#open .csv files and drop duplicates
cust_stat = pd.read_csv('c:/Users/fearn/JUMP/PEP1/customer_statistics.csv')
cust_stat = cust_stat.drop_duplicates()

orders = pd.read_csv('c:/Users/fearn/JUMP/PEP1/orders.csv')
orders = orders.drop_duplicates()


"""---------------------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN CLEANING and TRANSFORMING data and identifying OUTLIERS                                                                                                         
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------"""


#TRANSFORM Credit Card numbers and Providers to standard format
cust_demo.loc[cust_demo['credit_card_provider'] == 'VISA 13 digit', 'credit_card_provider'] = 'VISA 16 digit'
cust_demo.loc[(cust_demo['credit_card_number'].astype(str).str.len() > 16) | (cust_demo['credit_card_number'].astype(str).str.len() < 15), 'credit_card_number'] = None

#TRANSFORM and Standardizes Phone Numbers
def phone_cleanup(df):
    extensions = df['phone_number'].str.extract(r'x(.+)', expand=False)
    extensions = extensions.fillna('')
    df['phone_number'] = df['phone_number'].str.replace(r'x(.+)', '', regex = True)
    df['phone_number'] = df['phone_number'].str.replace(r'\D', '', regex = True)
    for i in range(len(df.phone_number)):
        if isinstance(df.phone_number[i], str) and df.phone_number[i] is not None:
            number = ''.join(filter(str.isdigit, df.phone_number[i]))
            if len(number) == 10:
                df.phone_number[i] = f'({number[0:3]}) {number[3:6]}-{number[6:10]}'
            elif len(number) == 11:
                df.phone_number[i] = f'+{number[0]} ({number[1:4]}) {number[4:7]}-{number[7:11]}'
            elif len(number) == 12:
                df.phone_number[i] = f'+{number[0:2]} ({number[2:5]}) {number[5:8]}-{number[8:12]}'
            elif len(number) == 13:
                df.phone_number[i] = f'+{number[0:3]} ({number[3:6]}) {number[6:9]}-{number[9:13]}'
            if str(extensions[i]).isdigit and extensions[i] != '':
                df.phone_number[i] = df.phone_number[i] + f' x {extensions[i]}'
            else:
                pass
phone_cleanup(cust_demo)

#VERIFYING the number of invalid email entries against NULL values
#RESULT: All emails that are entered are in proper format
cust_email = cust_demo["email"].str.match(r"^.+@.+\..{2,}$")
count1=0
count=0
for i in cust_email:
    if i == True:
        count +=1
    else:
        count1 +=1
email_null_count = pd.isnull(cust_demo['email']).sum()
print(f'There are {count} emails that are valid')
print(f"There are {email_null_count} emails with NULL values")
print(f"There are {count1 - email_null_count} *NON NULL* emails that aren't valid")

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


"""------------------------------------------------------------------------------------------------------------------------------------------------------------------------
FINISHED CLEANING and TRANSORMING and IDENTIFYING OUTLIERS 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------"""

"""------------------------------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN CREATING .csv files for Client review
------------------------------------------------------------------------------------------------------------------------------------------------------------------------"""

#RETRIEVING all records with NULL CREDIT CARD NUMBERS
cc_number_nulls = cust_demo.loc[cust_demo['credit_card_number'].isin([None])]

#CREATING a .csv for the client to review customers with invalid Credit Cards
cc_nulls_df = pd.DataFrame(cc_number_nulls)
cc_nulls_df.to_csv('cust_null_CC.csv', index=True)  

#RETRIEVING all records with NULL values for the ADDRESS columns
address_nulls = cust_demo.loc[cust_demo['address'].isin([None])]

#CREATING a .csv for the client to review customers with NULL addresses
address_nulls_df = pd.DataFrame(address_nulls)
address_nulls_df.to_csv('cust_null_address.csv', index=True)  

#Display records with NULL values in the EMAIL column
email_nulls = cust_demo.loc[cust_demo['email'].isin([None])]

#CREATING a .csv for the client to review customers with NULL emails
email_nulls_df = pd.DataFrame(email_nulls)
email_nulls_df.to_csv('cust_null_email.csv', index=False)  

#Display records with NULL values in the phone number column
phone_nulls = cust_demo.loc[cust_demo['phone_number'].isin([None])]

#CREATING a .csv for the client to review customers with NULL phone numbers
phone_nulls_df = pd.DataFrame(phone_nulls)
phone_nulls_df.to_csv('cust_null_phone.csv', index=False)  


"""----------------------------------------------------------------------------------------------------------------------------------------------------------------
FINISH creating .csv files, BEGIN DATAFRAME MERGE
-----------------------------------------------------------------------------------------------------------------------------------------------------------------"""


#Merge dataframes into one table
merged_data = pd.merge(cust_demo, cust_stat, how='left', on='customer_id')
merged_data = pd.merge(merged_data, orders, how='left', on="customer_id")
merged_data.drop_duplicates(inplace=True)


#COPY all Credit Card columns from customer demographics into new Credit Card DataFrame and DELETE Credit Card columns from cust_demo
CC_df_f = cust_demo[['credit_card_expires', 'credit_card_number', 'credit_card_provider', 'credit_card_security_code']].copy()
cust_demo = cust_demo.drop(['credit_card_expires', 'credit_card_number', 'credit_card_provider', 'credit_card_security_code'], axis=1)

#REARRANGE cutomer table columns
cust_demo = cust_demo[[ 'name', 'address', 'city', 'state', 'phone_number', 'email']]

#ASSIGN dataframe data to variables
orders_f = merged_data[['order_id', 'customer_id', 'items', 'aperitifs', 'appetizers', 'entrees', 'desserts', 'total']]
CC_df_f = merged_data[["customer_id","credit_card_expires", "credit_card_number", "credit_card_provider", "credit_card_security_code"]]
cust_info_f = merged_data[["customer_id","name", "phone_number", "address", "city", "state", "zip_code"]]
cust_stats_f = merged_data[["customer_id", "total_orders", "total_items", "total_spent"]]

#CREATE database and convert data to SQL tables
engine = create_engine('sqlite:///PEP1_db.db')
orders_f.to_sql('orders', con=engine, index=False, if_exists='replace')
CC_df_f.to_sql('credit_cards', con=engine, index=False, if_exists='replace')
cust_info_f.to_sql('customer_info', con=engine, index=False, if_exists='replace')
cust_stats_f.to_sql('customer_stats', con=engine, index=False, if_exists='replace')


"""----------------------------------------------------------------------------------------------------------------------------------------------------------------------
END DATABASE CREATION AND BEGIN FRONTEND USER SQL QUERY INTERACTION
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------"""

#connect to database and create cursor
conn = sqlite3.connect("PEP1_db.db")                                            
cursor = conn.cursor()

user_in = "y"

while user_in.lower() == 'y':

    #display menu
    print("\nPlease choose from the following choices: \n")

    print('1. Search CUSTOMER INFO by Customer ID')
    print('2. Search ORDER INFO by CUSTOMER ID')                                
    print('3. Search CUSTOMER STATS by CUSTOMER ID')
    print('4. INSERT NEW CUSTOMER')
    print('5. DELETE CUSTOMER')

    user_in = input("\nMake a selection: ")

    match user_in:

        #SEARCH CUSTOMER INFO
        case '1':
            user_in = input('Enter Customer Number: ')
            conn.row_factory = sqlite3.Row
            rows = conn.execute(f'SELECT DISTINCT * FROM customer_info WHERE customer_id = {user_in}')   

            for row in rows:
                print(f'\n {dict(row)} \n')
        
        #SEARCH ORDERS
        case '2':
            user_in = input('Enter Customer Number: ')
            conn.row_factory = sqlite3.Row
            rows = conn.execute(f"SELECT * total_orders FROM orders WHERE customer_ID = {user_in}")      #

            for row in rows:
                print(f'\n {dict(row)} \n')

        #SEARCH CUSTOMER STATISTICS
        case '3':
            user_in = input('Enter Customer Number: ')
            conn.row_factory = sqlite3.Row
            rows = conn.execute(f"SELECT total_orders as 'TOTAL_ORDERS' FROM customer_stats WHERE customer_ID = {user_in}")

            for row in rows:
                print(f'\n {dict(row)} \n')

        #INSERT NEW CUSTOMER
        case '4':
            user_id = input("User_ID: ")
            name = input("Enter the name: ")
            phone = input("Enter the phone number: ")
            address = input("Enter the address: ")
            city = input("Enter the city: ")
            state = input("Enter the state: ")
            zip_code = input("Enter the zip code: ")

            statement = f"INSERT into customer_info VALUES ('{user_id}', '{name}', '{phone}', '{address}', '{city}', '{state}', '{zip_code}')"
           
            print(f"\n Would you like to SAVE: \n {user_id} {name} {phone} {address} {city} {state} {zip_code} ?\n")
            user_in = input("Press Y to SAVE, or any other button to return to main screen: ")

            if user_in.lower() == 'y':
                conn.execute(statement)
                conn.commit()
                print('\nSUCCESS! CUSTOMER HAS BEEN ADDED!\n')
            else:
                print('\nUSER NOT SAVED!\n')

        #DELETE CUSTOMER
        case '5':
            user_in = input('Enter Customer Number: ')
            conn.row_factory = sqlalchemy.Row
            rows = conn.execute(f'SELECT DISTINCT * FROM customer_info WHERE customer_id = {user_in}')

            for row in rows:
                print(f'\n {dict(row)} \n')

            print("DELETE this entry?")
            user_in2 = input("\nPress Y to DELETE, or any other button to return: ")

            if user_in2.lower() == 'y':
                conn.execute(f'DELETE FROM customer_info WHERE customer_id = {user_in}')
                conn.commit()
                print("\nSUCCESSFULLY DELETE!\n")
            else:
                print("\nUSER NOT deleted!")
            
    user_in = input('Would you like to make another search? Y or any other button to quit: ')

print("\nGoodbye!\n")
