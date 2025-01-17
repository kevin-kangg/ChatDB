#
# Convert CSV file to JSON.
#
import csv
import json

import pandas as pd

# Convert the salaries data to json.
table_df = pd.read_csv('salaries.csv', header=0)
table_df.to_json('salaries.json', orient="records", indent=4)

# Convert the purchases data to json.
table_df = pd.read_csv('purchases.csv', header=0)
table_df.to_json('purchases.json', orient="records", indent=4)

# Convert the loan data to json.
table_df = pd.read_csv('loan.csv', header=0)
table_df.to_json('loan.json', orient="records", indent=4)

