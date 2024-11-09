import pandas as pd
from pymongo import MongoClient

def connect_to_mongodb(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def upload_data_to_collection(db, collection_name, file_path):
    collection = db[collection_name]
    data = pd.read_csv(file_path, encoding='ISO-8859-1')
    data_dict = data.to_dict('records')
    
    for record in data_dict:
        if not collection.find_one(record):
            collection.insert_one(record)
    print(f"Data from {file_path} has been successfully imported into MongoDB.")
