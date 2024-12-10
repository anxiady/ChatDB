from pprint import pprint
from tabulate import tabulate
from prettytable import PrettyTable

def execute_mongo_query(db, collection_name, query):
    collection = db[collection_name]
    return list(collection.find(query))

def execute_aggregation_query(db, collection_name, pipeline):
    collection = db[collection_name]
    return list(collection.aggregate(pipeline))

def print_all_data(db, collection_name):
    collection = db[collection_name]
    results = collection.find()  
    return list(results)

# def display_first_five_rows(db, collection_name):
#     collection = db[collection_name]
#     first_five = collection.find().limit(5)
#     for idx, record in enumerate(first_five, start=1):
#         print(f"Record {idx}: {record}")

def display_sample_rows(db, collection_name):
    collection = db[collection_name]
    first_five = collection.find().limit(3)
    print(f"\nSample data from the \033[91m{collection_name}\033[0m collection:\n")
    for idx, record in enumerate(first_five, start=1):
        print(f"{idx}:")
        pprint(record)
        print("-" * 60)

def display_columns(db, collection_name):
    collection = db[collection_name]
    sample_document = collection.find_one()
    if not sample_document:
        print("No documents found in the collection.")
        return

    table = PrettyTable()
    table.field_names = ["Column Name", "Type"]

    for key, value in sample_document.items():
        table.add_row([key, type(value).__name__])

    print(f"Columns in '{collection_name}':")
    print(table)

def display_all_columns(db):
    collections = db.list_collection_names()

    # Iterate through each collection
    for collection_name in collections:
        collection = db[collection_name]
        sample_document = collection.find_one()

        print(f"\nColumns in collection: {collection_name}")

        # Display column names and types
        table = PrettyTable()
        table.field_names = ["Column Name", "Type"]

        for key, value in sample_document.items():
            table.add_row([key, type(value).__name__])

        print(table)
