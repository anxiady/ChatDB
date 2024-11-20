import pandas as pd
import pymysql
from tabulate import tabulate

def sql_sample_data(connection):

    if not connection.open:
        connection.ping(reconnect = True)
        
    cursor = connection.cursor()


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
        df = pd.DataFrame(sample, columns = table_columns[selected_table])
        print(f"\n Sample data from '{selected_table}': ")
        print(tabulate(df, headers = 'keys', tablefmt = 'psql', showindex = False))

    else:
        print(f"No data found in '{table}'")

    cursor.close()
    
    