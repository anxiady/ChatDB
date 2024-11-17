from mongodb.connection import connect_to_mongodb, upload_data_to_collection, check_and_drop_database
from mongodb.queries import execute_mongo_query, execute_aggregation_query, print_all_data, display_first_five_rows
from mongodb.query_parser import generate_mongo_query, generate_example_queries, execute_query_from_input, display_result
import pandas as pd


def main():
    uri = "mongodb://localhost:27017"

    ### Data Upload
    db_name = "chatdb"
    dbs = [
        {'collection_name' : "disney_movie_stats",
        'file_path' : './data/disney_movie_stats.csv'
        },
        {'collection_name' : "disney_movies+tvs_info",
        'file_path' : './data/disney_movies+tvs.csv'
        },
        {'collection_name' : "banana_quality",
        'file_path' : './data/banana_quality_dataset.csv'
        },
    ]
    ### Drop previous data
    check_and_drop_database(uri, db_name) 
    ### Input Data
    db = connect_to_mongodb(uri, db_name)
    for collection in dbs:
        upload_data_to_collection(db, collection['collection_name'], collection['file_path'])
    
    print("\n\n")
    print("\033[92m+\033[0m"*100)
    print("\n\033[92mWelcome to ChatDB!\033[0m ")
    while True:
        print("\n\033[92mPlease choose an option:\033[0m")
        print("1. Upload a dataset")
        print("2. Explore available datasets")
        print("3. Exit")

        choice = input("\nEnter your choice (1/2/3): ")

        if choice == "1":
            # Upload a dataset
            file_path = input("Please enter the path to the CSV file you want to upload: ")
            collection_name = input("Please enter the name of the collection where you want to upload the data: ")

            try:
                upload_data_to_collection(db, collection_name, file_path)
                print("Data uploaded successfully!")
            except FileNotFoundError:
                print("\033[91mError:\033[0m The file path you provided does not exist. Please check the path and try again.")
            except pd.errors.EmptyDataError:
                print("\033[91mError:\033[0m The provided file is empty or cannot be read. Please provide a valid CSV file.")
            except Exception as e:
                print(f"An error occurred: {e}")

        elif choice == "2":
            # View available datasets
            collections = db.list_collection_names()
            if collections:
                print("\ncollections in database:")
                for collection in collections:
                    print(f"- {collection}")
                chosen_collection = input("Please input the exact name of the collection you wish to explore:")
                while True:
                    try:
                        if chosen_collection in collections:
                            print(f"You have chosen \033[91m{chosen_collection}\033[0m")

                            display_first_five_rows(db, chosen_collection)

                            ########################################
                            ### Official Query Stage
                            while True:
                                print("\n\033[92mTry ask me for example queries (i.e. example mongo queries, average, group by, sum, total)\033[0m")
                                user_input = input("prompt:")

                                if user_input.lower() in ["exit", "quit"]:
                                    print("Returning to main menu.")
                                    break

                                natural_language_query, mongo_query = generate_example_queries(user_input, db, chosen_collection)

                                # Display the generated example query
                                print(f"\n\033[92mExample Query:\033[0m {natural_language_query}")
                                print(f"\033[92mMongoDB Query:\033[0m {mongo_query}")
                                # print(mongo_query)

                                # Execute the query if user wants me to
                                execution = input("\n\033[92mDo you want me to execute the query for you? (y/n)\033[0m")
                                if execution.lower().strip() == 'y':
                                    results = execute_query_from_input(natural_language_query, db, chosen_collection)
                                    display_result(results)
                                else:
                                    continue
                        else:
                            print(f"\033[91m{chosen_collection}\033[0m not in database") 

                    except Exception as e:
                        print(f"\033[91mError:\033[0m {e}")
            else:
                print("No datasets available in the database.")

        elif choice == "3":
            # Exit the program
            print("Exiting the program. Goodbye!")
            break

        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
        
        print("+"*100)

    

if __name__ == "__main__":
    main()
    # uri = "mongodb://localhost:27017"

    # ### Data Upload
    # db_name = "chatdb"
    # dbs = [
    #     {'collection_name' : "disney_movie_stats",
    #     'file_path' : './data/disney_movie_stats.csv'
    #     },
    #     {'collection_name' : "disney_movies+tvs_info",
    #     'file_path' : './data/disney_movies+tvs.csv'
    #     },
    #     {'collection_name' : "banana_quality",
    #     'file_path' : './data/banana_quality_dataset.csv'
    #     },
    # ]
    # ### Drop previous data
    # check_and_drop_database(uri, db_name) 
    # ### Input Data
    # db = connect_to_mongodb(uri, db_name)
    # for collection in dbs:
    #     upload_data_to_collection(db, collection['collection_name'], collection['file_path'])

    # display_first_five_rows(db, 'banana_quality')

    # user_input = "find all artist"
    # pipeline = generate_mongo_query(user_input)
    # results = execute_aggregation_query(db, collection_name, pipeline)
    
    # # for record in results:
    # #     print(record)

    # all_data = print_all_data(db, collection_name)
    # print("All data in the collection:")
    # for record in all_data:
    #     print(record)