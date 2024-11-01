import pandas as pd
from pymongo import MongoClient
import sys

if len(sys.argv) != 2:
    print("Error: correct command line argument is 'python3 upload_mongo.py <csv>'")
    sys.exit(1)

# file_path = sys.argv[1]
file_path = '../spotify-2023.csv'

data = pd.read_csv(file_path, encoding='ISO-8859-1')

client = MongoClient('localhost', 27017)

db = client['spotify_2023']
collection = db['spotify_data']

data_dict = data.to_dict('records')

collection.insert_many(data_dict)

print(f"Data from {file_path} has been successfully imported into MongoDB.")
