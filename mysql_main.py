from mysql.Upload_SQL import upload, upload_data_to_table, display_all_columns, display_columns, display_sample_rows
from mysql.SQL_Queries import connect_to_SQL
from mysql.SQL_Queries import gen_sample_queries
import pymysql
import pandas as pd

def sql():

    selected_db = "nba_data"
    dbs = [
        {'collection_name': "nba_stats_percent", 'file_path': './data/2024_NBA_Player_Stats_Percentages.csv'},
        {'collection_name': "nba_stats_raw", 'file_path': './data/2024_NBA_Player_Stats_Raw.csv'},
        {'collection_name': "nba_shooting", 'file_path': './data/2024_NBA_Player_Stats_Shooting.csv'},
    ]

    ### Input Connection
    user = 'root'
    password = '111111'
    host = 'localhost'
    port = 3306

    connection = pymysql.connect(user = user, password = password, host = host, port = port)

    upload(connection, selected_db, dbs)

    print("\n\n")
    print("\033[92m+\033[0m"*100)
    print("\n\033[92mWelcome to SQL database!\033[0m ")


    while True:
        print("\n\033[92mPlease choose an option:\033[0m")
        print("1. Upload Data\n")
        print("2. Explore available tables\n")
        print("3. Generate Sample Queries\n")
        print("4. Answer Natural Language Questions\n")
        print("5. Exit")

        choice = input("Enter your choice (1/2/3/4/5): ").strip()

        if choice.strip() == '1':
            file_path = input("Please enter the path to the CSV file you want to upload: ")
            table_name = input("Please enter the name of the collection where you want to upload the data: ")
            try:
                upload_data_to_table(connection,selected_db, table_name, file_path)
            except FileNotFoundError:
                print("\033[91mError:\033[0m The file path you provided does not exist. Please check the path and try again.")
            except pd.errors.EmptyDataError:
                print("\033[91mError:\033[0m The provided file is empty or cannot be read. Please provide a valid CSV file.")
            except Exception as e:
                print(f"An error occurred: {e}")
        elif choice.strip() == '2':
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            if tables:
                print("\nTables in the database:")
                for table in tables:
                    print(f"- {table}")
                
                try:
                    chosen_table = input("Please input the exact name of the table you wish to explore: ").strip()
                    if chosen_table in tables:
                        print(f"\nYou have chosen \033[91m{chosen_table}\033[0m")
                        display_sample_rows(connection, chosen_table)
                        display_columns(connection, chosen_table)
                        # return_to_main_menu = False

                    else:
                        print(f"\033[91m{chosen_table}\033[0m not in database") 
                except Exception as e:
                    print(f"Error: {e}")
            else:
                print("No tables found in the database.")
        elif choice.strip() == '3':
            gen_sample_queries(connection, num_queries = 3, random_queries = True)

        elif choice.strip() == '4':
            gen_sample_queries(connection,num_queries = 1, random_queries = False)

        elif choice.strip() == '5':
            break
        
        else:
            print("Invalid choice. Please Enter one of the options in quotes above")

    if connection:
        connection.close()

# def sql():

#     print("Welcome to SQL database\n")
#     selected_db = None
#     connection = None
#     while True:

#         print("Please select a numbered option: \n")
#         print("1. Select or Change Database\n")
#         print("2. Upload Data\n")
#         print("3. View Table Attributes and Sample Data\n")
#         print("4. Generate Sample Queries\n")
#         print("5. Answer Natural Language Questions\n")
#         print("6. End Program\n")

#         choice = input("Enter your choice ('1', '2', '3', '4', '5', '6'): ").strip()

#         if choice.strip() == '1':
#             connection, selected_db = connect_to_SQL()
        
#         elif choice.strip() == '2':
#             if selected_db:
#                 upload_sql(connection)
#             else:
#                 print("Please select a database using option '1'")

#         elif choice.strip() == '3':
#             if selected_db:
#                 sql_sample_data(connection)
#             else:
#                 print("Please select a database using option '1'")
            

#         elif choice.strip() == '4':
#             if selected_db:
#                 gen_sample_queries(connection, num_queries = 3, random_queries = True)
#             else:
#                 print("Please select a database using option '1'")

#         elif choice.strip() == '5':
#             if selected_db:
#                 gen_sample_queries(connection,num_queries = 1, random_queries = False)
#             else:
#                 print("Please select a database using option '1'")


#         elif choice.strip() == '6':
#             break
        
#         else:
#             print("Invalid choice. Please Enter one of the options in quotes above")

#     if connection:
#         connection.close()
