# from mysql.Sample_Data import sql_sample_data
# from mysql.Upload_SQL import upload_sql
# from mysql.SQL_Queries import connect_to_SQL
# from mysql.SQL_Queries import gen_sample_queries
# from mongodb.connection import connect_to_mongodb, upload_data_to_collection, check_and_drop_database
# from mongodb.queries import execute_mongo_query, execute_aggregation_query, print_all_data, display_sample_rows, display_columns, display_all_columns
# from mongodb.query_parser import generate_mongo_query, generate_example_queries, execute_query_from_input, display_result, get_execute_query
import pandas as pd
from mongo_main import mongo
from mysql_main import sql


if __name__ == "__main__":

    # ### MongoDB connection
    # db_name = "chatdb_mongo"
    # uri = "mongodb://localhost:27017"
    # dbs = [
    #     {'collection_name' : "disney_movie_stats",
    #     'file_path' : './data/disney_movie_stats.csv'
    #     },
    #     {'collection_name' : "disney_movies+tvs_info",
    #     'file_path' : './data/disney_movies+tvs.csv'
    #     },
    #     {'collection_name' : "best_animated_films",
    #     'file_path' : './data/best_animated_film_winners.csv'
    #     },
    # ]

    # ### Mysql connection
    # user = 'root'
    # password = '111111'
    # host = 'localhost'
    # port = 3306

    # check_and_drop_database(uri, db_name) 
    # ### Input Data
    # db = connect_to_mongodb(uri, db_name)
    # for collection in dbs:
    #     upload_data_to_collection(db, collection['collection_name'], collection['file_path'])

    print("\n\n")
    print("\033[92m+\033[0m"*100)
    print("\n\033[92mWelcome to ChatDB!\033[0m \n")

    while True:
        print("\033[92mPlease select a database: \033[0m\n")
        print("1. SQL\n")
        print("2. NoSQL\n")
        print("3. Quit\n")
        choice = input("Enter your choice 1/2/3: ").strip()
        if choice == '1':
            sql()
        elif choice == '2':
            mongo()
        elif choice == '3':
            print("Exiting the program. Goodbye!")
            break
        else:
            raise ValueError("Invalid choice.")

