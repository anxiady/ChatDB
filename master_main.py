from mysql.Sample_Data import sql_sample_data
from mysql.Upload_SQL import upload_sql
from mysql.SQL_Queries_andy import connect_to_SQL
from mysql.SQL_Queries_andy import gen_sample_queries
from mongodb.connection import connect_to_mongodb, upload_data_to_collection, check_and_drop_database
from mongodb.queries import execute_mongo_query, execute_aggregation_query, print_all_data, display_sample_rows, display_columns, display_all_columns
from mongodb.query_parser import generate_mongo_query, generate_example_queries, execute_query_from_input, display_result, get_execute_query
import pandas as pd
from mongodb import mongo
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
    print("\n\033[92mWelcome to ChatDB!\033[0m ")

    while True:
        print("Please select a database: \n")
        print("1. SQL\n")
        print("2. NoSQL\n")
        print("3. Quit\n")
        choice = input("Enter your choice 1/2/3: ").strip()
        if choice == '1':
            mongo()
        elif choice == '2':
            sql()
        elif choice == '3':
            print("Exiting the program. Goodbye!")
            break
        else:
            raise ValueError("Invalid choice.")

    ###### OLD VERSION
    print("Welcome to ChatDB\n")
    selected_db = None
    connection = None
    while True:

        print("Please select a numbered option: \n")
        print("1. Select or Change Database\n")
        print("2. Upload Data\n")
        print("3. View Table Attributes and Sample Data\n")
        print("4. Generate Sample Queries\n")
        print("5. Answer Natural Language Questions\n")
        print("6. End Program\n")

        choice = input("Enter your choice ('1', '2', '3', '4', '5', '6'): ").strip()

        if choice.strip() == '1':
            connection, selected_db = connect_to_SQL()
        
        elif choice.strip() == '2':
            if selected_db:
                upload_sql(connection)
            else:
                print("Please select a database using option '1'")

        elif choice.strip() == '3':
            if selected_db:
                sql_sample_data(connection)
            else:
                print("Please select a database using option '1'")
            

        elif choice.strip() == '4':
            if selected_db:
                gen_sample_queries(connection, num_queries = 3, random_queries = True)
            else:
                print("Please select a database using option '1'")

        elif choice.strip() == '5':
            if selected_db:
                gen_sample_queries(connection,num_queries = 1, random_queries = False)
            else:
                print("Please select a database using option '1'")


        elif choice.strip() == '6':
            break
        
        else:
            print("Invalid choice. Please Enter one of the options in quotes above")

    if connection:
        connection.close()
