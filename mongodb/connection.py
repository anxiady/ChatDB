import pandas as pd
from pymongo import MongoClient

def connect_to_mongodb(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def upload_data_to_collection(db, collection_name, file_path):
    # Check if the collection already exists
    if collection_name in db.list_collection_names():
        print(f"\033[91mWarning:\033[0m The collection '{collection_name}' already exists in the database. \nData upload Aborted.")
        return

    collection = db[collection_name]
    data = pd.read_json(file_path, encoding='ISO-8859-1')
    data_dict = data.to_dict('records')
    
    for record in data_dict:
        if not collection.find_one(record):
            collection.insert_one(record)

    print(f"Data from {file_path} has been successfully imported into MongoDB.")
    print("Data uploaded successfully!")

def check_and_drop_database(uri, db_name):
    client = MongoClient(uri)
    if db_name in client.list_database_names():
        client.drop_database(db_name)
        print(f"Database '{db_name}' found and dropped.")
        return True
    else:
        print(f"Database '{db_name}' does not exist.")
        return True