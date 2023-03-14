import pandas as pd
import sqlalchemy
import argparse
import json

# ARGs parser

parser = argparse.ArgumentParser(description='Load script')
parser.add_argument('--input_folder', type=str)
parser.add_argument('--credentials_file', type=str)
parser.add_argument('--target_table', type=str)
args = parser.parse_args()

# Read data from the stagging zone

df = pd.read_parquet(f'{args.input_folder}/output.parquet')

# Create database connection data

with open(args.credentials_file) as json_file:
    credentials_file = json.load(json_file)

database_username = credentials_file['database_username']
database_password = credentials_file['database_password']
database_ip       = credentials_file['database_ip']
database_name     = credentials_file['database_name']

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

# Saves data into SQL

df.to_sql(con=database_connection, name=args.target_table, if_exists='append')