import argparse
import requests
import json

# ARG parser
parser = argparse.ArgumentParser(description='Extract script')

parser.add_argument('--url', type=str)
parser.add_argument('--n_results', type=int)
parser.add_argument('--output_folder', type=str)

args = parser.parse_args()

# Number of users to be saved
API_ENDPOINT = "{url}?results={n_results}".format(url = args.url, n_results = args.n_results)

# Get data from API
r = requests.get(API_ENDPOINT)

# Parse from text to JSON
data = json.loads(r.text)

# get the results array
results = data.get('results', [])

with open(f'{args.output_folder}/output.json', 'w') as output_file:
    json.dump(results, output_file)