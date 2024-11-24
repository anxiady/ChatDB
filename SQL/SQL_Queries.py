import pandas as pd
from sqlalchemy import create_engine
import pymysql
import sys
import os
from tabulate import tabulate
import random
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
import re

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

def connect_to_SQL(selected_db = None):

    user = 'root'
    password = 'Dsci-551'
    host = 'localhost'
    port = 3306

    connection = pymysql.connect(user = user, password = password, host = host, port = port)
    cursor = connection.cursor()

    if not selected_db:

        cursor.execute("Show Databases")
        dbs = [db[0] for db in cursor.fetchall()]

        print("\nAvailable Databases: ")
        for i, db in enumerate(dbs, start = 1):
            print(f"{i}, {db}")


        while True:
            db_choice = input("\n Enter the name of database you want to use.: ").strip()

            if db_choice in dbs:
                selected_db = db_choice
                break
            else:
                print("Did not recognize input. Please enter one of the existing databases.\n")
                
    cursor.close()
    connection.close()
    connection = pymysql.connect(user = user, password = password, host = host, port = port, database = selected_db)

    print(f"Connected to database: {selected_db}")
    return connection, selected_db
            

def preprocess(user_input):

    tokens = re.findall(r'<=|>=|=|<|>|\w+[\w_/.\-]*', user_input)
    filtered_tokens = [token.lower() for token in tokens]
    return filtered_tokens

def preprocess_keywords(tokens, mapping):

    processed_tokens = []
    i = 0
    while i < len(tokens):
        match = False
        for phrase, value in mapping.items():
            phrase_tokens = phrase.split()
            if tokens[i:i + len(phrase_tokens)] == phrase_tokens:
                processed_tokens.append(value)
                i += len(phrase_tokens)
                match = True
                break
    
        if not match:
            processed_tokens.append(mapping.get(tokens[i], tokens[i]))
            i += 1
    #print(processed_tokens)
    return processed_tokens
    

def column_types(cursor, table):
    cursor.execute(f"Show Columns From `{table}`")
    
    columns_type = cursor.fetchall()
    columns = [col[0].strip() for col in columns_type]
    numeric_columns = [col[0].strip() for col in columns_type if col[1].strip() in ['int', 'float', 'double', 'decimal', 'smallint', 'tinyint', 'bigint']]
    categorical_columns = [col[0].strip() for col in columns_type if col[1].strip() in ['varchar', 'char', 'text', 'enum', 'nchar', 'nvarchar', 'ntext']]

    return columns, numeric_columns, categorical_columns

   
def get_column_matches(user_input, cursor):

    filtered_tokens = preprocess(user_input)
    cursor.execute("Show Tables")
    tables = [row[0] for row in cursor.fetchall()]
    all_columns = {}

    for table in tables:
        columns, numeric_columns, categorical_columns = column_types(cursor, table)
        all_columns[table] = columns

    matched_columns = {}
    for table, columns in all_columns.items():
        matching_columns = [col for col in columns if col in filtered_tokens]
        if matching_columns:
            matched_columns[table] = matching_columns
    return matched_columns


def get_table(user_input, cursor):

    matched_columns = get_column_matches(user_input, cursor)
    if matched_columns:
        for table, columns in matched_columns.items():
            columns, numeric_columns, categorical_columns = column_types(cursor, table)
            return table, columns, numeric_columns, categorical_columns

    cursor.execute("Show Tables")
    tables = [table[0] for table in cursor.fetchall()]

    for table in tables:
        if table.lower() in user_input.lower():
            columns, numeric_columns, categorical_columns = column_types(cursor, table)
            return table, columns, numeric_columns, categorical_columns
            
    return None, None, None, None


def condition_value(cursor, agg_col, table_choice, conditions): 
    
    condition = random.choice(conditions)
    cursor.execute(f"Select MIN(`{agg_col}`), MAX(`{agg_col}`) from `{table_choice}`")
        
    min_value, max_value = cursor.fetchone()
        
    if min_value is None or max_value is None:
        print(f"Error: Value was None")
        return condition, 0
            
    value = random.uniform(min_value, max_value)

    if min_value == int(min_value) and max_value == int(max_value):
        value = round(value)

    return condition, value


def get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens = None):
    
    where_col, where_condition, where_value = None, None, None
    
    if random_queries:
        where_col = random.choice(categorical_columns + numeric_columns)
        if where_col in numeric_columns:
            where_condition, where_value = condition_value(cursor, where_col, table_choice, conditions)
        elif where_col in categorical_columns:
            cursor.execute(f"Select Distinct `{where_col}` from `{table_choice}` limit 20;")
            unique_values = [row[0] for row in cursor.fetchall()]
            where_value = random.choice(unique_values) if unique_values else 'unknown'
            where_condition = '='
    else:
        where_col = next((col for col in columns if col in where_tokens), None)
        if where_col:
            if where_col in numeric_columns:
                where_condition = next((cond for cond in conditions if cond in where_tokens), None)
                where_value = next((v for v in where_tokens if v.replace('.', '', 1).isdigit()), None)
            elif where_col in categorical_columns:
                where_condition = '='
                if '=' in where_tokens:
                    equal_index = where_tokens.index('=')
                    if equal_index + 1 < len(where_tokens):
                        where_value = where_tokens[equal_index + 1]
                    else:
                        where_value = None
                else:
                    value = None

        
    return where_col, where_condition, where_value


def get_having_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, agg_functions, having_tokens = None):
    
    where_col, where_condition, where_value = None, None, None
    
    if random_queries:
        having_col = random.choice(numeric_columns)
        having_condition, having_value = condition_value(cursor, having_col, table_choice, conditions)
        agg_function = random.choice(agg_functions)
    else:
        agg_function = next((func for func in agg_functions if func in having_tokens), None)
        having_col = next((col for col in numeric_columns if col in having_tokens), None)
        having_condition = next((cond for cond in conditions if cond in having_tokens), None)    
        having_value = next((v for v in having_tokens if v.replace('.', '', 1).isdigit()), None)

    return agg_function, having_col, having_condition, having_value


def get_order_clause(cursor, table_choice, numeric_columns, random_queries, order_tokens = None, filtered_tokens = None):
    
    order_col, order_type, limit_value = None, None, None

    if random_queries: 
        
        order_col = random.choice(numeric_columns)
        limit_value = random.randint(1, 10) if random.choice([True, False]) else None
        order_type = random.choice(['ASC', 'DESC'])
    
    else:
        
        order_col = next((col for col in order_tokens if col in numeric_columns), None)      
        if any(token in filtered_tokens for token in ['bottom', 'ascending', 'asc']):
            order_type = 'ASC'     
        if any(token in filtered_tokens for token in ['top', 'descending', 'desc']):
            order_type = 'DESC'
        limit_value = next((int(v) for v in filtered_tokens if v.isdigit()), None) 


    return order_col, order_type, limit_value

def execute_query(cursor, query, query_number):
    while True:
        execute_query = input(f"Do you want to execute the query? (yes/no) ").strip().lower()
        if execute_query == 'yes':
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    print("\nQuery Result: ")
                    print(f"{' | '.join(columns)}")
                    print('-'*30)
                    for row in result:
                        print(f"{' | '.join(map(str, row))}")
                            
                else:
                    print("No Results Given")
                    

            except Exception as e:
                print(f"Error executing query: {e}")
                
            break
            
        elif execute_query == 'no':
            break
        else:
            print("Could not recognize input. Please type 'yes' or 'no'.")



def gen_sample_queries(connection, num_queries = 1, random_queries = True):

    
    if not connection.open:
        connection.ping(reconnect = True)
        


    cursor = connection.cursor()

    cursor.execute("Show Tables")
    tables = [table[0] for table in cursor.fetchall()]


    if not tables:
        print("No tables in this database")
        connection.close()
        return []

    if random_queries:
        user_input = input("What kind of query would you like to see or would you like ChatDB to generate one?").strip().lower()

    else:
        user_input = input("Ask a question: ").strip()
        
    table_choice = None
    while table_choice is None:
        if random_queries:
            table_choice = random.choice(tables)
            columns, numeric_columns, categorical_columns = column_types(cursor, table_choice)
        else:
            table_choice, columns, numeric_columns, categorical_columns = get_table(user_input, cursor)
            if table_choice is None:
                print("Unable to find table based on user input")
                user_input = input("Rewrite your question: ")
                continue

    
    columns, numeric_columns, categorical_columns = column_types(cursor, table_choice)
    #print(numeric_columns)
    #print(categorical_columns)
    

        
    queries = ['having', 'order', 'group', 'max', 'min', 'sum', 'avg', 'count', 'where', 'select']
    conditions = ['>', '<', '=', '<=', '>=']
    condition_text = {'>': 'greater than', '<': 'less than', '=': 'equal to', '<=': 'less than or equal to', '>=': 'greater than or equal to'}
    agg_functions = ['AVG', 'SUM', 'MAX', 'MIN', 'COUNT']
    agg_text = {'AVG': 'average', 'SUM': 'sum', 'MAX': 'maximum', 'MIN': 'minimum', 'COUNT': 'count'}
    limit_text = {'ASC': 'Bottom', 'DESC': 'Top'}
    keyword_mapping = {'less than or equal to': '<=', 'greater than or equal to': '>=', 'more than or equal to': '>=', 'greater than': '>', 'more than': '>', 'less than': '<', 
                       'equal to': '=', 'is': '=', 'are': '=', 'average': 'AVG', 'maximum': 'MAX', 'minimum': 'MIN', 'total': 'SUM',
                      'find': 'select', 'show': 'select', 'grouped': 'group'}
    
   

    filtered_tokens = preprocess(user_input)
    filtered_tokens = preprocess_keywords(filtered_tokens, keyword_mapping)
    sample_queries = []

    specific_query = [query for query in queries if query in filtered_tokens]
  

    if random_queries:

        if 'any' in filtered_tokens or not any(query in filtered_tokens for query in queries):
            print("Generating any query. You selected 'any' or had no specific query identified.")
            selected_queries = random.sample(queries, k = num_queries)
        else:
            selected_queries = random.choices(specific_query, k = num_queries)
         

    else:
        if specific_query:
            selected_queries = specific_query[:num_queries]
            
        else:
            while not specific_query:
                print("Unable to idenfiy query based on user input\n")
                user_input = input("Rewrite your question: ")
                filtered_tokens = preprocess(user_input)
                specific_query = [query for query in queries if query in filtered_tokens]
            selected_queries = specific_query[:num_queries]

    for query_i in selected_queries:

        nl, query = None, None


        # where and having 
        if 'where' in specific_query and 'having' in specific_query and numeric_columns and categorical_columns:

            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                agg_function, having_col, having_condition, having_value = get_having_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, agg_functions)
                group_col = random.choice(categorical_columns)
                
            else: 
                where_index = filtered_tokens.index('where') if 'where' in filtered_tokens else None
                group_index = filtered_tokens.index('group') if 'group' in filtered_tokens else None  
                having_index = filtered_tokens.index('having') if 'having' in filtered_tokens else None 
    
                if (where_index is not None and group_index is not None and having_index is not None):
                    if where_index < group_index < having_index:
                        where_end = group_index
                        group_end = having_index
                        having_end = len(filtered_tokens)
                            
                    elif where_index < having_index < group_index:
                        where_end = having_index
                        having_end = group_index
                        group_end = len(filtered_tokens)

                    elif group_index < having_index < where_index:
                        group_end = having_index
                        having_end = where_index
                        where_end = len(filtered_tokens)
    
                    where_tokens = filtered_tokens[where_index + 1: where_end]
                    group_tokens = filtered_tokens[group_index + 1: group_end]
                    having_tokens = filtered_tokens[having_index + 1: having_end]

        
                    where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens)
                    agg_function, having_col, having_condition, having_value = get_having_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, agg_functions, having_tokens)
                    group_col = next((col for col in categorical_columns if col in group_tokens))
                
                #print(f"where_col: {where_col} where_condition: {where_condition} where_value: {where_value} group_col: {group_col} having_col: {having_col} agg_function: {agg_function} having_condition: {having_condition} having_values: {having_value}")

                    if (where_col is not None and where_condition is not None and where_value is not None and group_col is not None and having_col is not None and agg_function is not None and having_condition is not None and having_value is not None):

                        if where_col in categorical_columns:
                            where_value = f"'{where_value}'"

                        if having_col in categorical_columns:
                            having_value = f"'{having_value}'"
                    
                        query = (f"Select `{group_col}`, {agg_function}(`{having_col}`) as `{agg_text[agg_function]}_{having_col}` "
                                f"From `{table_choice}` where `{where_col}` {where_condition} {where_value} "
                                f"Group by `{group_col}` having {agg_function}(`{having_col}`) {having_condition} {having_value};")

                        nl = (f"{agg_text[agg_function]} of {having_col} in {table_choice} grouped by {group_col} "
                              f"having {agg_text[agg_function]} of {having_col} {condition_text[having_condition]} {having_value} "
                              f"where {where_col} is {condition_text[where_condition]} {where_value}")
                
              

            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for where and having clause.\n")
                return


        elif 'where' in specific_query and 'order' in specific_query and numeric_columns and categorical_columns:

            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, numeric_columns, random_queries)
                columns_used = ', '.join(random.sample(columns, k = min(2, len(columns))))
            else:
                where_index = filtered_tokens.index('where')   
                order_index = filtered_tokens.index('order') 

                if where_index < order_index:
                    where_tokens = filtered_tokens[where_index + 1:order_index]
                    order_tokens = filtered_tokens[order_index + 1:]
                elif order_index < where_index:
                    order_tokens = filtered_tokens[order_index + 1:where_tokens]
                    where_tokens = filtered_tokens[where_index + 1:]

                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens)
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, numeric_columns, random_queries, order_tokens, filtered_tokens)
                columns_used = ', '.join(set(col for col in filtered_tokens if col in numeric_columns + categorical_columns))

            if limit_value is not None and where_value is not None and str(limit_value) == str(where_value):

                where_value_index = filtered_tokens.index(str(where_value) in filtered_tokens)
                limit_value_index = filtered_tokens.index(str(limit_value) in filtered_tokens)

                if where_value_index == limit_value_index:
                    limit_value = next((int(v) for i, v in enumerate(filtered_tokens) if v.isdigit() and i != where_value_index), None)

            if where_col in categorical_columns:
                    where_value = f"'{where_value}'"


                
            if (where_col is not None and where_condition is not None and where_value is not None and order_col is not None and order_type is not None and limit_value is not None ) :
                
                query = f"Select {columns_used} from {table_choice} where {where_col} {where_condition} {where_value} order by {order_col} {order_type} LIMIT {limit_value};"
                nl = f"Select {limit_text[order_type]} {limit_value} {columns_used} from {table_choice} where {where_col} is {condition_text[where_condition]} {where_value}"

            elif (where_col is not None and where_condition is not None and where_value is not None and order_col is not None):
                query = f"Select {columns_used} from {table_choice} where {where_col} {where_condition} {where_value} order by {order_col} {order_type};"
                nl = f"{columns_used} ordered by {order_col} in {order_type.lower()} order from {table_choice} where {where_col} is {condition_text[where_condition]} {where_value}"

            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for where and order clause.\n")
                return
        
                
        # having
        elif query_i == 'having' and numeric_columns and categorical_columns:
        
            if random_queries:
                agg_col = random.choice(numeric_columns)
                group_col = random.choice(categorical_columns)
                agg_function = random.choice(agg_functions)
                condition, value = condition_value(cursor, agg_col, table_choice, conditions)
            else:
                agg_col = next((col for col in numeric_columns if col in filtered_tokens), None)
                group_col = next((col for col in categorical_columns if col in filtered_tokens), None)
                agg_function = next((token.upper() for token in filtered_tokens if token.upper() in agg_functions), None)
                condition = next((cond for cond in conditions if cond in filtered_tokens), None)    
                value = next((v for v in filtered_tokens if v.replace('.', '', 1).isdigit()), None)

        
            if (agg_col is not None and group_col is not None and agg_function is not None and condition is not None and value is not None):
                query = f"Select `{group_col}`, {agg_function}(`{agg_col}`) as `{agg_text[agg_function]}_{agg_col}` from `{table_choice}` group by `{group_col}` having {agg_function}(`{agg_col}`) {condition} {value};"
                nl = f"{agg_text[agg_function]} {agg_col} in {table_choice} group by {group_col} having {agg_text[agg_function]}_{agg_col} {condition_text[condition]} {value}"
           
            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for having clause.\n")
                return
        
            
        # group by and aggregate functions combined
        elif any(token.upper() in ['GROUP'] + agg_functions for token in filtered_tokens) and numeric_columns and categorical_columns:
            condition = None
            value = None
            if random_queries:
                agg_function = next((token.upper() for token in filtered_tokens if token.upper() in agg_functions), random.choice(agg_functions))
                if agg_function == 'COUNT':
                    agg_col = random.choice(numeric_columns+categorical_columns)
                    if agg_col in numeric_columns:
                        condition, value = condition_value(cursor, agg_col, table_choice, conditions)
                else:
                    agg_col = random.choice(numeric_columns)
                group_col = random.choice(categorical_columns)

            else:
                agg_function = next((token.upper() for token in filtered_tokens if token.upper() in agg_functions), None)
                if 'group' in filtered_tokens:
                    group_index = filtered_tokens.index('group') 
                    if agg_function == 'COUNT':
                        agg_cols = [col for col in filtered_tokens[:group_index] if col in numeric_columns+categorical_columns]
                    else:
                        agg_cols = [col for col in filtered_tokens[:group_index] if col in numeric_columns]
                    group_cols = [col for col in filtered_tokens[group_index + 1:] if col in categorical_columns]
                    agg_col = agg_cols[0] if agg_cols else None
                    group_col = group_cols[0] if group_cols else None
                else:
                    group_col = None
                    agg_col = next((col for col in filtered_tokens if col in numeric_columns), None)
                if agg_function == 'COUNT':
                    condition = next((cond for cond in conditions if cond in filtered_tokens), None)  
                    value = next((v for v in filtered_tokens if v.replace('.', '', 1).isdigit()), None)
               
         
            #print(agg_col," ", agg_function, " ", group_col,  " ", condition, " ",value)

            if agg_function == 'COUNT' and agg_col in numeric_columns and group_col:
                query = f"Select `{group_col}`, COUNT(`{agg_col}`) as count_`{agg_col}` from `{table_choice}` where `{agg_col}` {condition} {value} group by `{group_col}`;"
                nl = f"Count of {agg_col} in {table_choice} group by {group_col} where {agg_col} is {condition_text[condition]} {value}"
        

            elif (agg_function == 'COUNT' and agg_col in categorical_columns) or (agg_function!= 'COUNT' and group_col and agg_col):
                query = f"Select {group_col}, {agg_function}({agg_col}) as {agg_function}_{agg_col} from {table_choice} group by {group_col};"
                nl = f"{agg_text[agg_function]} of {agg_col} group by {group_col}"
             
              

            elif agg_function in ['SUM', 'AVG', 'MAX', 'MIN'] and agg_col in numeric_columns and not group_col:
                query = f"Select {agg_function}({agg_col}) as {agg_function}_{agg_col} from {table_choice};"
                nl = f"{agg_text[agg_function]} of {agg_col} in {table_choice}"

            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for aggregate and group by clause.\n")
                return
        

               # where
        elif query_i == 'where':
            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                columns_used = ', '.join(random.sample(columns, k = min(2, len(columns))))
                
            else:
                where_index = filtered_tokens.index('where')
                where_tokens = filtered_tokens[where_index + 1:]
                columns_used = ', '.join(set(col for col in filtered_tokens if col in numeric_columns + categorical_columns))
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens)
                    
                #print(f"Condition: {where_condition}, Value: {where_value}, agg_col: {where_col}, columns_used: {columns_used}")
    
            if where_col and where_condition and where_value:

                if where_col in categorical_columns:
                    where_value = f"'{where_value}'"
                    
                query = f"Select {columns_used} from {table_choice} where {where_col} {where_condition} {where_value};"
                nl = f"Select {columns_used} from {table_choice} where {where_col} is {condition_text[where_condition]} {where_value}"
            

            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for where clause.\n")
                return



    # order by    
        elif query_i == 'order' and numeric_columns:
            if random_queries:
                columns_used = ', '.join(random.sample(columns, k = min(2, len(columns))))
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, numeric_columns, random_queries)
            
            else:
                order_index = filtered_tokens.index('order')
                order_tokens = filtered_tokens[order_index:]

                columns_used = ', '.join(set(col for col in numeric_columns+categorical_columns if col in filtered_tokens))
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, numeric_columns, random_queries, order_tokens, filtered_tokens)

                #print(columns_used, " ", order_col, " ", value, " ", order_type)
            
            if (limit_value is not None and order_type is not None):
                query = f"Select {columns_used} from {table_choice} order by {order_col} {order_type} LIMIT {limit_value};"
                nl = f"{limit_text[order_type]} {limit_value} {columns_used} ordered by {order_col}"

            elif (limit_value is None and order_type is not None):
                query = f"Select {columns_used} from {table_choice} order by {order_col} {order_type};"
                nl = f"{columns_used} ordered by {order_col} in {limit_text[order_type]} order."

            if nl is None or query is None:
                print("Failed to recognize column, condition, type, or value for order clause.\n")
                return

    

        elif query_i == 'select':
            if random_queries:
                columns_used = ', '.join(random.sample(columns, k = min(4, len(columns))))
         
            else:
                if 'all' in filtered_tokens or all(col in filtered_tokens for col in columns):
                    columns_used = '*'
                else:
                    filtered_columns = [col for col in columns if col in filtered_tokens]
                    if filtered_columns:
                        columns_used = ', '.join(set(filtered_columns))
                    else:
                        print("Failed to recognize column, condition, or value for where and order clause.")
                        return

        
            while True:
                num_return = input("How many rows would you like to return(input 'all' to return all values)? ").strip().lower()
                if num_return == 'all':
                    query = f"Select {columns_used} from {table_choice};"
                    nl = f"Select {columns_used} from {table_choice}"
                    break
                
                try:
                    num_return = int(num_return)
                    if num_return > 0:
                        query = f"Select {columns_used} from {table_choice} limit {num_return};"
                        nl = f"Select the first {num_return} {columns_used} from {table_choice}"
                        break

                    else:
                        print("Please enter a positive integer")
                        
                except ValueError:
                    print("Please input a positive integer.")

        sample_queries.append((nl, query))

    if sample_queries:
        if random_queries:
            print("\nSample Queries\n")
            for i, (nl, query) in enumerate(sample_queries, 1):
                print(f"Sample Query {i} \n"
                      f"\n"
                      f"NL Description: {nl}\n"
                      f"\n"
                      f"Query: {query}\n")
            execute_query(cursor, query, i)

        else:
            print("\nQuery\n")
            for i, (nl, query) in enumerate(sample_queries, 1):
                print(f"NL Interpreted Query \n"
                      f"\n"
                      f"NL Description: {nl}\n"
                      f"\n"
                      f"Query: {query}\n")
            execute_query(cursor, query, i)

     
                

    else:
        print("No Queries could be generated")
            



    return sample_queries
    

    
