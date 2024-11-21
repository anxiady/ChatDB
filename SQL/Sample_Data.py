import pandas as pd
import pymysql
from tabulate import tabulate
from sqlalchemy import create_engine
import os

def sql_sample_data(connection):

    if not connection.open:
        connection.ping(reconnect = True)
        
    cursor = connection.cursor()

    cursor.execute("Select Database()")
    selected_db = cursor.fetchone()[0]

    cursor.execute("Show Tables")
    tables = [table[0] for table in cursor.fetchall()]

    if not tables:
        print(f"No table founds in '{selected_db}'. Please upload some.")
        connection.close()
        return
    
    print(f"Tables and attributes in '{selected_db}':")

    table_columns = {}
    for table in tables:
        cursor.execute(f"Show columns from `{table}`")
        columns = [col[0] for col in cursor.fetchall()]
        table_columns[table] = columns
        print(f"\n {table}")
        print(f"Attributes:", ", ".join(columns))
        print()



    while True:
        selected_table = input("\nEnter name of table you want to view sample data from: ").strip()

        if selected_table in table_columns:
            break

        else:
            print("Table input not recognized. Please enter a table name from above")

    cursor.execute(f"Select * from `{selected_table}` limit 5")
    sample = cursor.fetchall()

    if sample:
        
        print(f"\n Sample data from '{selected_table}': ")
        print(f"{' | '.join(table_columns[selected_table])}")
        print("-"*(5*len(table_columns[selected_table])))

        for row in sample:
            print(f"{' | '.join(map(str, row))}\n")
    else:
        print(f"No data found in '{table}'")

    cursor.close()
    
    
