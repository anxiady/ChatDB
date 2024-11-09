import pandas as pd
from pymongo import MongoClient
import sys

# if len(sys.argv) != 2:
#     print("Error: correct command line argument is 'python3 upload_mongo.py <csv>'")
#     sys.exit(1)

# # file_path = sys.argv[1]
# file_path = '../spotify-2023.csv'

# data = pd.read_csv(file_path, encoding='ISO-8859-1')

# client = MongoClient('localhost', 27017)

# db = client['spotify_2023']
# collection = db['spotify_data']

# data_dict = data.to_dict('records')

# collection.insert_many(data_dict)

# print(f"Data from {file_path} has been successfully imported into MongoDB.")

def connect_and_upload_to_mongodb(uri, db_name, collection_name, file_path):
    # Establish MongoDB connection
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Read data from CSV
    data = pd.read_csv(file_path, encoding='ISO-8859-1')
    data_dict = data.to_dict('records')
    
    # Insert data into collection if it doesn't already exist
    for record in data_dict:
        if not collection.find_one(record):
            collection.insert_one(record)
    
    print(f"Data from {file_path} has been successfully imported into MongoDB.")
    
    return db





# Example usage:
uri = "mongodb://localhost:27017"
db_name = "spotify_2023"
collection_name = "spotify_data"
file_path = '../spotify-2023.csv'
db = connect_and_upload_to_mongodb(uri, db_name, collection_name, file_path)