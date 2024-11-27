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
from prettytable import PrettyTable

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

# function to connect to SQL and for user to select database
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
            
# preprocess keywords to match query types and any non text tokens
def preprocess(user_input):
    queries = ['join', 'having', 'order', 'group', 'where', 'max', 'min', 'sum', 'avg', 'count', 'select']
    tokens = re.findall(r'<=|>=|=|<|>|\w+[\w_/.\-]*', user_input)
  
    filtered_tokens = [token.lower() if token.lower() in queries else token for token in tokens]
    #print(filtered_tokens)
    return filtered_tokens

# preprocess keywords according to a mapping so words or phrases such as "greater than" can be recognized as keywords in the SQL query
def preprocess_keywords(tokens, mapping):

    processed_tokens = []
    i = 0

    lowercase = {key.lower(): value for key, value in mapping.items()}
    
    while i < len(tokens):
        match = False
        for phrase, value in lowercase.items():
            phrase_tokens = phrase.split()
            if [token.lower() for token in tokens[i:i + len(phrase_tokens)]] == phrase_tokens:
                processed_tokens.append(value)
                i += len(phrase_tokens)
                match = True
                break
    
        if not match:
            processed_tokens.append(lowercase.get(tokens[i].lower(), tokens[i]))
            i += 1
    #print(processed_tokens)
    return processed_tokens
    
# determine column types based on the CSV user uploaded
def column_types(cursor, table):
    cursor.execute(f"Show Columns From `{table}`")
    
    columns_type = cursor.fetchall()
    columns = [col[0].strip() for col in columns_type]
    numeric_columns = [col[0].strip() for col in columns_type if col[1].strip() in ['int', 'float', 'double', 'decimal', 'smallint', 'tinyint', 'bigint']]
    categorical_columns = [col[0].strip() for col in columns_type if col[1].strip() in ['varchar(25)', 'char', 'text', 'enum', 'nchar', 'nvarchar', 'ntext']]

    return columns, numeric_columns, categorical_columns

# find any columns that match between user_input and the columns in the tables
def get_column_matches(user_input, cursor):

    filtered_tokens = preprocess(user_input)
    #print(filtered_tokens)
    cursor.execute("Show Tables")
    tables = [row[0] for row in cursor.fetchall()]
    all_columns = {}

    for table in tables:
        columns, numeric_columns, categorical_columns = column_types(cursor, table)
        #print(columns)
        all_columns[table] = columns

    matched_columns = {}
    for table, columns in all_columns.items():
        matching_columns = [col for col in filtered_tokens if col in columns]
        #print(matching_columns)
        if matching_columns:
            matched_columns[table] = matching_columns
    return matched_columns

# get the table based on the columns that match between user_input and the columns in the tables
# based on a scoring system. The table with the most words that match with user_input will be selected.
def get_table(user_input, cursor):

    matched_columns = get_column_matches(user_input, cursor)
    if matched_columns:
        number_matches = {}
        for table, columns in matched_columns.items():
            matches = len(columns)
            number_matches[table] = matches
        closest_table = max(number_matches, key = number_matches.get)
        columns, numeric_columns, categorical_columns = column_types(cursor, closest_table)
        return closest_table, columns, numeric_columns, categorical_columns
    
    cursor.execute("Show Tables")
    tables = [table[0] for table in cursor.fetchall()]

    for table in tables:
        if table.lower() in user_input.lower():
            columns, numeric_columns, categorical_columns = column_types(cursor, table)
            return table, columns, numeric_columns, categorical_columns
            
    return None, None, None, None

# select a random condition and value for SQL query generation
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

# get the where column, condition and value
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

# get the having column, condition, value, and the group by aggregate function
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

# get the order by column, type, and limit value if it exists.
def get_order_clause(cursor, table_choice, categorical_columns, numeric_columns, random_queries, order_tokens = None, filtered_tokens = None):
    
    order_col, order_type, limit_value = None, None, None

    if random_queries: 
        
        order_col = random.choice(numeric_columns + categorical_columns)
        limit_value = random.randint(1, 10) if random.choice([True, False]) else None
        order_type = random.choice(['ASC', 'DESC'])
    
    else:
        
        order_col = next((col for col in order_tokens if col in numeric_columns + categorical_columns), None)      
        if any(token.lower() in ['bottom', 'ascending', 'asc'] for token in filtered_tokens):
            order_type = 'ASC'     
        if any(token.lower() in ['top', 'descending', 'desc'] for token in filtered_tokens):
            order_type = 'DESC'
        limit_value = next((int(v) for v in filtered_tokens if v.isdigit()), None) 


    return order_col, order_type, limit_value


# ask user if they want to execute the query
def execute_query(cursor, query, query_number):
    while True:
        execute_query = input(f"Do you want to execute the query? (yes/no) ").strip().lower()
        if execute_query == 'yes':
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    
                    table = PrettyTable()
                    table.field_names = columns
                    for row in result:
                        table.add_row(row)
                    
                    print("\nQuery Result:")
                    print(table)
                else:
                    print("No results returned.")
            except Exception as e:
                print(f"Error executing query: {e}")
            break
        elif execute_query == 'no':
            break
        else:
            print("Could not recognize input. Please type 'yes' or 'no'.")


# generate sample queries based on the number of queries needed, random_queries is True for if user wants any query
# it is set to false if user asks a question
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
        print(f"Note: Available queries are ‘select’ , ‘where’, ‘group by’ with aggregate "
                      f"function (‘MAX’, ‘MIN’, ‘SUM’, ’COUNT’, ‘AVG’) , aggregate functions "
                      f"alone, ‘having’, ‘order by …. limit’, ‘where … group by … having’, and "
                      f"‘where … order by …. limit’. \nWhen asking for sample queries and writing "
                      f"your own in natural language, please try to include one of those "
                      f"keywords so our NLP can process your question.")
        user_input = input("\nWhat kind of query would you like to see or would you like ChatDB to generate one (Type 'Exit' to leave.)? ").strip()
        print(f"")

    else:
        print(f"Note: Available query NLP are ‘select ... table_name’ , ‘where’, ‘group by’ with aggregate "
                      f"function (‘MAX’, ‘MIN’, ‘SUM’, ’COUNT’, ‘AVG’) , aggregate functions "
                      f"alone, ‘group by... having... aggregate function(column)’, ‘order by …. limit’, ‘where … group by … having... aggregate_function(column)’, "
                      f"‘where … order by …. limit’, and 'select ... join table1 with table2 on column'. \nWhen asking for sample queries and writing "
                      f"your own in natural language, please try to include one of those "
                      f"keywords so our NLP can process your question. Table columns are CASE SENSITIVE.")
        user_input = input("\nAsk a question (Type 'Exit' to leave.):  ").strip()

    if user_input.lower() == 'exit':
        return[]
    
    # dtermine if a table can be found based on user input
    table_choice = None
    while table_choice is None:
        if random_queries:
            table_choice = random.choice(tables)
            columns, numeric_columns, categorical_columns = column_types(cursor, table_choice)
        else:
            table_choice, columns, numeric_columns, categorical_columns = get_table(user_input, cursor)
            if table_choice is None:
                print("Unable to find table based on user input")
                user_input = input("Rewrite your question (or type 'Exit' to leave): ")
                if user_input.lower() == 'exit':
                    return[]
                continue

    
    columns, numeric_columns, categorical_columns = column_types(cursor, table_choice)


    
    queries = ['join', 'having', 'order', 'group', 'where', 'max', 'min', 'sum', 'avg', 'count', 'select']
    conditions = ['<=', '>=', '>', '<', '=']
    condition_text = {'>': 'greater than', '<': 'less than', '=': 'equal to', '<=': 'less than or equal to', '>=': 'greater than or equal to'}
    agg_functions = ['avg', 'sum', 'max', 'min', 'count']
    agg_text = {'avg': 'average', 'sum': 'sum', 'max': 'maximum', 'min': 'minimum', 'count': 'count'}
    limit_text = {'ASC': 'Bottom', 'DESC': 'Top'}
    keyword_mapping = {'less than or equal to': '<=', 'greater than or equal to': '>=', 'more than or equal to': '>=', 'greater than': '>', 'more than': '>', 'less than': '<', 
                       'equal to': '=', 'is': '=', 'are': '=', 'average': 'avg', 'maximum': 'max', 'minimum': 'min', 'total': 'sum',
                      'find': 'select', 'show': 'select', 'grouped': 'group'}
    

    filtered_tokens = preprocess(user_input)
    filtered_tokens = preprocess_keywords(filtered_tokens, keyword_mapping)
    #print(filtered_tokens)
    sample_queries = []

    specific_query = [query for query in queries if query in filtered_tokens]
    #print(specific_query)
  
    # random queries for both if a query is specified or not
    if random_queries:

        if 'any' in filtered_tokens or not any(query in filtered_tokens for query in queries):
            print("Generating any query. You selected 'any' or had no specific query identified.")
            selected_queries = random.sample(queries, k = min(num_queries, len(queries)))
            specific_query = [query for query in selected_queries]
            #print(selected_queries)
            
        else:
            selected_queries = random.choices(specific_query, k = num_queries)
            #print(selected_queries)


         

    else:
        if specific_query:
            selected_queries = specific_query[:num_queries]
            #print(selected_queries)
  
            
        else:
            while not specific_query:
                print("Unable to idenfiy query based on user input\n")
                user_input = input("Rewrite your question (or enter 'Exit' to leave): ")
                if user_input.lower() == 'exit':
                    return[]
                
                filtered_tokens = preprocess(user_input)
                specific_query = [query for query in queries if query in filtered_tokens]
            selected_queries = specific_query[:num_queries]

    # query types that can be generated
    for query_i in selected_queries:
        # print(query_i)

        nl, query = None, None

        # where and having 
        if 'where' in specific_query and 'having' in specific_query and numeric_columns and categorical_columns:

            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                agg_function, having_col, having_condition, having_value = get_having_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, agg_functions)
                group_col = random.choice(categorical_columns)
             
                
            else: 
                # indexing in NLP to find where each keyword is to determine values, columns or conditions associated with each word
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

        # where and order by
        elif 'where' in specific_query and 'order' in specific_query and numeric_columns and categorical_columns:

            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, categorical_columns, numeric_columns, random_queries)
                columns_used = ', '.join(random.sample(columns, k = random.randint(1, len(columns))))
            else:
                 # indexing in NLP to find where each keyword is to determine values, columns or conditions associated with each word
                where_index = filtered_tokens.index('where')   
                order_index = filtered_tokens.index('order') 

                if where_index < order_index:
                    where_tokens = filtered_tokens[where_index + 1:order_index]
                    order_tokens = filtered_tokens[order_index + 1:]
                elif order_index < where_index:
                    order_tokens = filtered_tokens[order_index + 1:where_tokens]
                    where_tokens = filtered_tokens[where_index + 1:]

                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens)
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, categorical_columns, numeric_columns, random_queries, order_tokens, filtered_tokens)
                columns_used = ', '.join(set(col for col in filtered_tokens if col in numeric_columns + categorical_columns))

            if limit_value is not None and where_value is not None and str(limit_value) == str(where_value):

                where_value_index = filtered_tokens.index(str(where_value))
                limit_value_index = filtered_tokens.index(str(limit_value))

                if where_value_index == limit_value_index:
                    limit_value = next((int(v) for i, v in enumerate(filtered_tokens) if v.isdigit() and i != where_value_index), None)

            if where_col in categorical_columns:
                    where_value = f"'{where_value}'"

            # possible query types if limit is included or not
            if (where_col is not None and where_condition is not None and where_value is not None and order_col is not None and order_type is not None and limit_value is not None ) :
                
                query = f"Select {columns_used} from {table_choice} where {where_col} {where_condition} {where_value} order by {order_col} {order_type} LIMIT {limit_value};"
                nl = f"Select {limit_text[order_type]} {limit_value} {columns_used} from {table_choice} where {where_col} is {condition_text[where_condition]} {where_value}"

            elif (where_col is not None and where_condition is not None and where_value is not None and order_type is not None and order_col is not None):
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
                agg_function = next((token for token in filtered_tokens if token in agg_functions), None)
                condition = next((cond for cond in conditions if cond in filtered_tokens), None)    
                value = next((v for v in filtered_tokens if v.replace('.', '', 1).isdigit()), None)

        
            if (agg_col is not None and group_col is not None and agg_function is not None and condition is not None and value is not None):
                query = f"Select `{group_col}`, {agg_function}(`{agg_col}`) as `{agg_text[agg_function]}_{agg_col}` from `{table_choice}` group by `{group_col}` having {agg_function}(`{agg_col}`) {condition} {value};"
                nl = f"{agg_text[agg_function]} {agg_col} in {table_choice} group by {group_col} having {agg_text[agg_function]}_{agg_col} {condition_text[condition]} {value}"
           
            if nl is None or query is None:
                print("Failed to recognize column, condition, or value for having clause.\n")
                return
        
            
        # group by and aggregate functions combined
        elif 'group' in specific_query or 'max' in specific_query or 'min' in specific_query or 'sum' in specific_query or 'avg' in specific_query or 'count' in specific_query and numeric_columns and categorical_columns:
            condition = None
            value = None
            if random_queries:
                agg_function = next((token for token in filtered_tokens if token in agg_functions), random.choice(agg_functions))
                if agg_function == 'count':
                    agg_col = random.choice(numeric_columns+categorical_columns)
                    if agg_col in numeric_columns:
                        condition, value = condition_value(cursor, agg_col, table_choice, conditions)
                else:
                    agg_col = random.choice(numeric_columns)
                group_col = random.choice(categorical_columns)

            else:
                agg_function = next((token for token in filtered_tokens if token in agg_functions), None)
                if 'group' in filtered_tokens:
                    group_index = filtered_tokens.index('group') 
                    if agg_function == 'count':
                        agg_cols = [col for col in filtered_tokens[:group_index] if col in numeric_columns+categorical_columns]
                    else:
                        agg_cols = [col for col in filtered_tokens[:group_index] if col in numeric_columns]
                    group_cols = [col for col in filtered_tokens[group_index + 1:] if col in categorical_columns]
                    agg_col = agg_cols[0] if agg_cols else None
                    group_col = group_cols[0] if group_cols else None
                else:
                    group_col = None
                    agg_col = next((col for col in filtered_tokens if col in numeric_columns), None)
                if agg_function == 'count':
                    condition = next((cond for cond in conditions if cond in filtered_tokens), None)  
                    value = next((v for v in filtered_tokens if v.replace('.', '', 1).isdigit()), None)
            #print(agg_col," ", agg_function, " ", group_col,  " ", condition, " ",value)
            if agg_function is None or agg_col is None:
                print("Failed to recognize column, condition, or value for aggregate and group by clause.\n")
                return
            # possible query types using aggregate functions and group by
            if agg_function == 'count' and agg_col in numeric_columns and group_col:
                query = f"Select {group_col}, count({agg_col}) as count_{agg_col} from {table_choice} where {agg_col} {condition} {value} group by `{group_col}`;"
                nl = f"Count of {agg_col} in {table_choice} group by {group_col} where {agg_col} is {condition_text[condition]} {value}"
        

            elif (agg_function == 'count' and agg_col in categorical_columns) or (agg_function!= 'count' and group_col and agg_col):
                query = f"Select {group_col}, {agg_function}({agg_col}) as {agg_function}_{agg_col} from {table_choice} group by {group_col};"
                nl = f"{agg_text[agg_function]} of {agg_col} group by {group_col}"
             
              

            elif agg_function in ['sum', 'avg', 'max', 'min'] and agg_col in numeric_columns and not group_col:
                query = f"Select {agg_function}({agg_col}) as {agg_function}_{agg_col} from {table_choice};"
                nl = f"{agg_text[agg_function]} of {agg_col} in {table_choice}"

       
        

        # where
        elif query_i == 'where':
            if random_queries:
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions)
                columns_used = ', '.join(random.sample(columns, k = random.randint(1, len(columns))))
                
            else:
                # indexing in NLP to find where each keyword is to determine values, columns or conditions associated with each word
                where_index = filtered_tokens.index('where')
                where_tokens = filtered_tokens[where_index + 1:]
                columns_used = ', '.join(set(col for col in filtered_tokens if col in numeric_columns + categorical_columns))
                where_col, where_condition, where_value = get_where_clause(cursor, table_choice, numeric_columns, categorical_columns, columns, random_queries, conditions, where_tokens)
                    
                # print(f"Condition: {where_condition}, Value: {where_value}, agg_col: {where_col}, columns_used: {columns_used}")

            
            if where_col is not None and where_condition is not None and where_value is not None:

                if where_col in categorical_columns:
                    where_value = f"'{where_value}'"
                    
                query = f"Select {columns_used} from {table_choice} where {where_col} {where_condition} {where_value};"
                nl = f"Select {columns_used} from {table_choice} where {where_col} is {condition_text[where_condition]} {where_value}"
            
            if nl is None and query is None:
                print("Failed to recognize column, condition, or value for where clause.\n")
                return
        


        # order by    
        elif query_i == 'order' and numeric_columns:
            if random_queries:
                columns_used = ', '.join(random.sample(columns, k = random.randint(1, len(columns))))
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, categorical_columns, numeric_columns, random_queries)
            
            else:
                # indexing in NLP to find where each keyword is to determine values, columns or conditions associated with each word
                order_index = filtered_tokens.index('order')
                order_tokens = filtered_tokens[order_index:]

                columns_used = ', '.join(set(col for col in numeric_columns+categorical_columns if col in filtered_tokens))
                order_col, order_type, limit_value = get_order_clause(cursor, table_choice, categorical_columns, numeric_columns, random_queries, order_tokens, filtered_tokens)

                #print(columns_used, " ", order_col, " ", value, " ", order_type)
            
            # different query types if limit is included or not
            if (limit_value is not None and order_type is not None):
                query = f"Select {columns_used} from {table_choice} order by {order_col} {order_type} LIMIT {limit_value};"
                nl = f"{limit_text[order_type]} {limit_value} {columns_used} ordered by {order_col}"

            elif (limit_value is None and order_type is not None):
                query = f"Select {columns_used} from {table_choice} order by {order_col} {order_type};"
                nl = f"{columns_used} ordered by {order_col} in {limit_text[order_type]} order."

            if nl is None or query is None:
                print("Failed to recognize column, condition, type, or value for order clause.\n")
                return

        # join
        elif query_i == 'join':
            if random_queries:
                table_x, table_y = random.sample(tables, 2)

                columns_x, numeric_columns_x, categorical_columns_x = column_types(cursor, table_x)
                columns_y, numeric_columns_y, categorical_columns_y = column_types(cursor, table_y)

                join_x, join_y = None, None

                # to check if there are any overlapping columns that can be joined
                for col_x in columns_x:
                    for col_y in columns_y:
                        cursor.execute(f"select Count(*) from {table_x} x Join {table_y} y on x.{col_x} = y.{col_y}")

                        match = cursor.fetchone()[0]
                        if match > 0:
                            join_x, join_y = col_x, col_y
                            break
                
                    if join_x and join_y:
                        break
                
                if join_x and join_y:
                    selected_columns = random.sample(columns_x + columns_y, k = min(4, len(columns_x + columns_y)))
                    # to avoid duplicate columns in select
                    unique_columns = set()
                    columns_used = []
                    for col in selected_columns:
                        if col not in unique_columns:
                            unique_columns.add(col)
                            if col in columns_x:
                                columns_used.append(f"{table_x}.{col}")
                            elif col in columns_y:
                                columns_used.append(f"{table_y}.{col}")

                    columns_used = ', '.join(columns_used)
                        
                else:
                    print(f"No match row values found between {table_x} and {table_y}.\n")
                    return
            else:

                if "join" in filtered_tokens:
                    join_index = filtered_tokens.index("join")
                    join_token = filtered_tokens[join_index + 1:]

                join_tables = [table for table in join_token if table in tables]

                if len(join_tables) != 2: 
                    print("Please only input 2 tables.\n")
                    return
                
                table_x, table_y = join_tables[:2]
                columns_x, numeric_columns_x, categorical_columns_x = column_types(cursor, table_x)
                columns_y, numeric_columns_y, categorical_columns_y = column_types(cursor, table_y)

        
                join_columns = [column for column in join_token if column in columns_x + columns_y]

                if not join_columns:
                    print("Error: Could not find a column from input that belongs to either table mentioned\n")
                    return
                
                join_column = join_columns[0]
                
                join_x, join_y = None, None
                
                # to check if there are any overlapping columns that can be joined from user input
                for col_x in columns_x:
                    for col_y in columns_y:
                        if join_column in (col_x, col_y):
                            cursor.execute(f"select Count(*) from {table_x} x Join {table_y} y on x.{col_x} = y.{col_y}")

                            match = cursor.fetchone()[0]
                            if match > 0:
                                join_x, join_y = col_x, col_y
                                break
                
                        if join_x and join_y:
                            break

                if join_x is None or join_y is None:
                    print(f"No matching row values found for {join_column} between {table_x} and {table_y}\n")
                    return
                
                selected_columns = [token for token in filtered_tokens if token in columns_x + columns_y]
                
                columns_used = ", ".join([f"{table_x}.{col}" if col in columns_x else f"{table_y}.{col}" for col in selected_columns])

            # ask how many rows user wants returned
            while True:
                num_return = input("How many rows would you like to return(input 'all' to return all values)? ").strip().lower()
                if num_return == 'all':
                    query = (f"Select {columns_used} From {table_x} Join {table_y} on {table_x}.{join_x} = {table_y}.{join_y};")
                    nl = (f"give all {columns_used} when you join {table_x} with {table_y} on where {join_x} from {table_x}is equal to {join_y} from {table_y}")
                    break
                
                try:
                    num_return = int(num_return)
                    if num_return > 0:
                        query = (f"Select {columns_used} From {table_x} Join {table_y} on {table_x}.{join_x} = {table_y}.{join_y} limit {num_return};")
                        nl = (f"give the first {num_return} {columns_used} when you join {table_x} with {table_y} on where {join_x} from {table_x}is equal to {join_y} from {table_y}")
                        break

                    else:
                        print("Please enter a positive integer")
                        
                except ValueError:
                    print("Please input a positive integer.")


        # simple select
        elif query_i == 'select':
            if random_queries:
                columns_used = ', '.join(random.sample(columns, k = random.randint(1, len(columns))))
         
            else:
                if 'all' in filtered_tokens or all(col in filtered_tokens for col in columns):
                    columns_used = '*'
                else:
                    filtered_columns = [col for col in columns if col in filtered_tokens]
                    if filtered_columns:
                        columns_used = ', '.join(set(filtered_columns))
                    else:
                        print("Failed to recognize column, condition, or value for select clause.")
                        return

            # for select, ask how many rows user wants returned
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

    # print the queries that are generated
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
    

    
