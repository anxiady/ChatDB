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