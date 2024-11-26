from mongodb.connection import connect_to_mongodb, upload_data_to_collection, check_and_drop_database
from mongodb.queries import display_sample_rows, display_columns, display_all_columns
from mongodb.query_parser import get_execute_query
import pandas as pd
import os



def mongo():

    ### Input Connection
    uri = "mongodb://localhost:27017"
    ### Data Upload
    db_name = "chatdb"
    dbs = [
        {'collection_name' : "disney_movies",
        'file_path' : './data/disney_movie_stats.json'
        },
        {'collection_name' : "disney_movies+tvs",
        'file_path' : './data/disney_movies+tvs.json'
        },
        {'collection_name' : "best_animated",
        'file_path' : './data/best_animated_film_winners.json'
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
    print("\n\033[92mWelcome to MongoDB!\033[0m ")
    while True:
        print("\n\033[92mPlease choose an option:\033[0m")
        print("1. Upload a dataset")
        print("2. Explore available collections")
        print("3. Exit")

        choice = input("\nEnter your choice (1/2/3): ")

        if choice == "1":
            # Upload a dataset
            file_path = input("Please enter the path to the CSV file you want to upload: ")
            collection_name = input("Please enter the name of the collection where you want to upload the data: ")

            try:
                upload_data_to_collection(db, collection_name, file_path)
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
                while True:
                    try:
                        chosen_collection = input("Please input the exact name of the collection you wish to explore:").strip()
                        if chosen_collection in collections:
                            print(f"\nYou have chosen \033[91m{chosen_collection}\033[0m")

                            display_sample_rows(db, chosen_collection)
                            display_columns(db, chosen_collection)

                            ########################################
                            ### Official Query Stage
                            return_to_main_menu = False
                            
                            while True:
                                try:
                                    print("\n\033[92mPlease choose how you want to explore:\033[0m")
                                    print("1. Sample Queries")
                                    print("2. Your own Queries")
                                    print("3. View attributes for all collections")
                                    print("4. Back<")
                                    print(f"\nCurrent Collection: \033[91m{chosen_collection}\033[0m")
                                    option = input("Enter your choice (1/2/3/4): ")
                                    if option == "1":
                                        get_execute_query(db,chosen_collection, random_query=True)
                                    elif option == "2":
                                        # print('Working on this...')
                                        get_execute_query(db,chosen_collection, random_query=False)
                                    elif option == "3":
                                        display_all_columns(db)
                                    elif option == "4":
                                        return_to_main_menu = True
                                        break
                                except Exception as e:
                                    print(e)
                                    print('\n')

                        else:
                            print(f"\033[91m{chosen_collection}\033[0m not in database") 
                            return_to_main_menu = False
                        
                        if return_to_main_menu:
                            break

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

    