import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os




def upload_sql(connection):

    file_path = input("Enter the CSV filename to upload to SQL: ")

    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' not found. Check for any typos")
        return

    table = os.path.splitext(os.path.basename(file_path))[0]
    #db = 'SQL_Datasets'
    user = 'root'
    password = 'Dsci-551'
    host = 'localhost'
    port = 3306

    connection = pymysql.connect(user = user, password = password, host = host, port = port)
    cursor = connection.cursor()

    cursor.execute("Show Databases")
    databases = [db[0] for db in cursor.fetchall()]

    print("\nAvailable Databases: ")
    for i, db in enumerate(databases, start = 1):
        print(f"{i}, {db}")


    while True:
        db_choice = input("\n Enter the name of database you want to upload the CSV to (or type 'New' to create a new database): ").strip()

        if db_choice in databases:
            db = db_choice
            cursor.execute(f"Use `{db}`")
            break
        elif db_choice.lower() == 'new':
            new_db = input("Enter the name of the new database: ").strip()
            cursor.execute(f"Create database if not exists `{new_db}`")
            cursor.execute(f"Use `{new_db}`")
            db = new_db
            break
        else:
            print("Did not recognize input. Please enter one of the existing databases of 'New'")
            
   

    cursor.execute(f"Show tables like '{table}'")
    table_exists = cursor.fetchone() is not None

    if table_exists:
        while True:
            replace = input(f"The table '{table}' already exists. Do you want to replace it? (yes/no): ").strip().lower()
            if replace == 'yes':
                break
            elif replace == 'no':
                print(f"Table '{table}' not replaced")
                connection.close()
                return
            else:
                print("Invalid input. Please enter 'yes' or 'no' only")

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    data = pd.read_csv(file_path, encoding = 'ISO-8859-1')
    data.to_sql(name = table, con = engine, if_exists = 'replace', index = False)

    print(f"'{file_path} has been successfully uploaded as '{table}' table in database '{db}'")
    cursor.close()
    connection.close()
    

   
    
