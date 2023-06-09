import pandas as pd
import json
import argparse

def extract_data(record):
    return {
        'full_name': ' '.join(record['name'].values()).upper(),
        'gender': 'M' if record['gender'].lower() == 'male' else 'F',
        'age': record['dob']['age'],
        'country': record['location']['country'].lower(),
        'state': record['location']['state'].lower(),
        'city': record['location']['city'].lower(),
        'email': record.get('email', '').lower(),
        'phone': record.get('cell', '').lower()
    }


# --- ARG parser
parser = argparse.ArgumentParser(description='Transform script')

parser.add_argument('--input_folder', type=str)
parser.add_argument('--output_folder', type=str)

args = parser.parse_args()
# ---

# READ DATA FROM RAW ZONE
with open(f'{args.input_folder}/output.json', 'r') as input_folder:
    data = json.loads(input_folder.read())

# TRANSFORM STAGE: EXTRACT SPECIFIC FIELDS

us_data = []

if data:
    us_data = [extract_data(record) for record in data if record['location']['country'].lower() == "united states"]

# Save data as parquet
pd.DataFrame.from_records(us_data).to_parquet(f'{args.output_folder}/output.parquet', engine='fastparquet')