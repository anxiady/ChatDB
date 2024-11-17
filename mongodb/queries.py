from pprint import pprint
from tabulate import tabulate

def execute_mongo_query(db, collection_name, query):
    collection = db[collection_name]
    return list(collection.find(query))

def execute_aggregation_query(db, collection_name, pipeline):
    collection = db[collection_name]
    return list(collection.aggregate(pipeline))

def print_all_data(db, collection_name):
    collection = db[collection_name]
    results = collection.find()  # No filter, returns all documents
    return list(results)

# def display_first_five_rows(db, collection_name):
#     collection = db[collection_name]
#     first_five = collection.find().limit(5)
#     for idx, record in enumerate(first_five, start=1):
#         print(f"Record {idx}: {record}")

def display_first_five_rows(db, collection_name):
    collection = db[collection_name]
    first_five = collection.find().limit(3)
    print(f"\nSample data from the '{collection_name}' collection:\n")
    for idx, record in enumerate(first_five, start=1):
        print(f"{idx}:")
        pprint(record)
        print("-" * 60)


