import yaml
import pandas as pd
#


# Load YAML data from a file
with open('customer_demographics.yaml', 'r') as yaml_file:
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

merged_data = cust_demo.merge(cust_stat, left_index=True, right_index=True)
merged_data = cust_demo.merge(cust_stat, left_index=True, right_index=True)

merged_data = merged_data.rename(columns={'customer_id_y': 'customer_id'})
df = merged_data.merge(orders, on='customer_id')
df = df.rename(columns={'customer_id_x': 'customer_id_demo'})

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


phone_cleanup(df)
