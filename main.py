from mongodb.connection import connect_to_mongodb, upload_data_to_collection
from mongodb.queries import execute_mongo_query, execute_aggregation_query, print_all_data
from mongodb.query_parser import generate_mongo_query


if __name__ == "__main__":

    uri = "mongodb://localhost:27017"
    db_name = "spotify_2023"
    collection_name = "spotify_data"
    file_path = './spotify-2023.csv'
    
    db = connect_to_mongodb(uri, db_name)
    upload_data_to_collection(db, collection_name, file_path)
    user_input = "find all artist"
    pipeline = generate_mongo_query(user_input)
    results = execute_aggregation_query(db, collection_name, pipeline)
    
    # for record in results:
    #     print(record)

    all_data = print_all_data(db, collection_name)
    print("All data in the collection:")
    for record in all_data:
        print(record)