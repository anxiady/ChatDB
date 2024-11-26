import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
from prettytable import PrettyTable




def upload_sql(connection):

    file_path = input("Enter the CSV filename to upload to SQL: (Type 'Exit' to leave.) ")

    if file_path.lower() == 'exit':
        return[]
    

    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' not found. Check for any typos")
        return
    # replace any spaces 
    table = os.path.splitext(os.path.basename(file_path))[0].replace(' ', '_')
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
            new_db = input("Enter the name of the new database: ").strip().replace(' ', '_')
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

    
    data = pd.read_csv(file_path, encoding = 'ISO-8859-1')
    data.columns = [col.replace(' ', '_') for col in data.columns]
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    data.to_sql(name = table, con = engine, if_exists = 'replace', index = False)

    print(f"'{file_path} has been successfully uploaded as '{table}' table in database '{db}'")
    cursor.close()
    connection.close()

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "VARCHAR(25)"  # Default for strings and other types

def upload(connection, db_name, dbs):
    cursor = connection.cursor()

    # Drop the database if it exists
    cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    print(f"Database '{db_name}' dropped.")

    # Create the database
    cursor.execute(f"CREATE DATABASE `{db_name}`")
    print(f"Database '{db_name}' created.")

    # Use the newly created database
    cursor.execute(f"USE `{db_name}`")

    for dataset in dbs:
        table_name = dataset['collection_name']
        file_path = dataset['file_path']

        if not os.path.isfile(file_path):
            print(f"Error: File '{file_path}' not found. Skipping this dataset.")
            continue

        print(f"Uploading data from '{file_path}' into table '{table_name}'...")

        # Read CSV data using pandas
        try:
            data = pd.read_csv(file_path)
        except Exception as e:
            print(f"Error reading CSV file '{file_path}': {e}")
            continue

        # Infer column types and create table schema
        columns = data.columns
        column_definitions = ", ".join([f"`{col}` {infer_sql_type(data[col])}" for col in columns])

        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cursor.execute(f"CREATE TABLE `{table_name}` ({column_definitions})")
        print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

        # Insert data into the table
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in columns])}) VALUES ({placeholders})"

        for _, row in data.iterrows():
            values = tuple(row[col] if not pd.isnull(row[col]) else None for col in columns)
            cursor.execute(insert_query, values)

        connection.commit()
        print(f"Data uploaded successfully to table '{table_name}'.")

    cursor.close()
    print("All datasets have been processed.")


def upload_data_to_table(connection, db_name, table_name, file_path):

    cursor = connection.cursor()

    # Check if the table already exists
    cursor.execute(f"USE `{db_name}`")
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    table_exists = cursor.fetchone()

    if table_exists:
        print(f"\033[91mWarning:\033[0m The table '{table_name}' already exists in the database. \nData upload Aborted.")
        return

    # Load data into a DataFrame
    if file_path.endswith(".csv"):
        data = pd.read_csv(file_path, encoding='ISO-8859-1')
    else:
        print(f"Error: Unsupported file format for {file_path}. Only JSON and CSV are supported.")
        return

    # Create the table dynamically based on the data schema
    columns = data.columns
    column_definitions = ", ".join([f"`{col}` {infer_sql_type(data[col])}" for col in columns])
    cursor.execute(f"CREATE TABLE `{table_name}` ({column_definitions})")
    print(f"Table '{table_name}' created with columns: {', '.join(columns)}")

    # Insert data into the table
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in columns])}) VALUES ({placeholders})"

    for _, row in data.iterrows():
        values = tuple(row[col] for col in columns)
        cursor.execute(insert_query, values)

    connection.commit()
    print(f"Data from {file_path} has been successfully imported into MySQL.")

def display_sample_rows(connection, table_name, row_count=5):

    cursor = connection.cursor()

    # Query to fetch sample rows
    query = f"SELECT * FROM `{table_name}` LIMIT {row_count}"
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print(f"No data found in the table '{table_name}'.")
            return

        # Fetch column names for the table
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = [col[0] for col in cursor.fetchall()]

        print(f"\nSample data from the \033[91m{table_name}\033[0m table:\n")
        
        # Use PrettyTable to display rows
        table = PrettyTable()
        table.field_names = columns

        for row in rows:
            table.add_row(row)

        print(table)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()



def display_columns(connection, table_name):
    cursor = connection.cursor()

    # Query to get column information
    query = f"DESCRIBE `{table_name}`"
    cursor.execute(query)

    # Fetch column details
    columns = cursor.fetchall()
    if not columns:
        print(f"No columns found in the table '{table_name}'.")
        return

    # Display column details using PrettyTable
    table = PrettyTable()
    table.field_names = ["Column Name", "Type"]

    for column in columns:
        column_name = column[0]
        column_type = column[1]
        table.add_row([column_name, column_type])

    print(f"Columns in '{table_name}':")
    print(table)

    cursor.close()

def display_all_columns(connection):
    cursor = connection.cursor()

    # Get the list of all tables in the database
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    if not tables:
        print("No tables found in the database.")
        return

    # Iterate through each table and display its columns
    for (table_name,) in tables:
        print(f"\nColumns in table: {table_name}")

        # Query to get column information for the current table
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()

        # Display column details using PrettyTable
        table = PrettyTable()
        table.field_names = ["Column Name", "Type"]

        for column in columns:
            column_name = column[0]
            column_type = column[1]
            table.add_row([column_name, column_type])

        print(table)

    cursor.close()