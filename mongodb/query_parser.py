import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import pos_tag
import random
import re
from bson import ObjectId

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger_eng')

def generate_mongo_query(user_input):
    tokens = word_tokenize(user_input, language='english')
    tokens = [word.lower() for word in tokens if word.isalnum()]
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]
    tagged_tokens = pos_tag(filtered_tokens)

    if "find" in tokens or "show" in tokens:
        for word, tag in tagged_tokens:
            if tag.startswith('NN') and 'artist' in word:
                artist_name_index = tokens.index(word) + 1
                if artist_name_index < len(tokens):
                    artist_name = tokens[artist_name_index]
                    return {"type": "find", "query": {"artist_name": artist_name}}

    
    attribute = "streams"
    grouping_attribute = "artist_name"
    for word, tag in tagged_tokens:
        if tag.startswith('NN') and 'stream' in word:
            attribute = word
        elif tag.startswith('NN') and 'artist' in word:
            grouping_attribute = word
    
    pipeline = [
        {"$group": {"_id": f"${grouping_attribute}", "total_streams": {"$sum": f"${attribute}"}}}
    ]
    return pipeline

def get_collection_attributes(db, collection_name):
    # Get one document to retrieve the keys (attribute names)
    collection = db[collection_name]
    sample_document = collection.find_one()
    if sample_document:
        return list(sample_document.keys())
    return []

def generate_example_queries(user_input, db, collection_name):
    # Define templates
    keyword_templates = {
        "group by": "group by <B> and calculate average of <A>",
        "total": "total <A> by <B>",
        "find": "find <A> for each <B>",
        "sum": "sum of <A> per <B>",
        "average": "average <A> by <B>",
        "count": "count number of <B>",
        "where": "find <A> where <B> <Operator> <C>",
        "order": "find <A> and order by <B>",
        "join": "join <A> with <B>",
        "min": "find the min <A> grouped by <B>",
        "max": "find the max <A> grouped by <B>"
    }

    attributes = get_collection_attributes(db, collection_name)
    attribute_types = get_attribute_types(db, collection_name)
    if not attributes:
        return "No attributes found in the collection for query generation.", {}

    conditions = ['<=', '>=', '>', '<', '=']
    # Check if specific keywords are present in user_input
    for keyword, template in keyword_templates.items():
        if keyword in user_input.lower() and keyword == 'where':
            selected_attribute_1 = random.choice([attr for attr in attributes if attribute_types.get(attr) in ["int", "float"]])
            selected_attribute_2 = random.choice([attr for attr in attributes if attribute_types.get(attr) not in ("int", "float") and attr != selected_attribute_1])
            operator = random.choice(conditions)

            distinct_values = db[collection_name].distinct(selected_attribute_2)
        
            if distinct_values:
                random_value = random.choice(distinct_values)
            else:
                random_value = random.randint(1, 100)

            natural_language_query = template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2).replace("<Operator>", operator).replace("<C>", f"{random_value}")
            
            return natural_language_query
        elif keyword in user_input.lower():
            # selected_attribute_1 = random.choice(attributes)
            # selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])
            selected_attribute_1 = random.choice([attr for attr in attributes if attribute_types.get(attr) in ["int", "float"]])
            selected_attribute_2 = random.choice([attr for attr in attributes if attribute_types.get(attr) not in ("int", "float") and attr != selected_attribute_1])
            natural_language_query = template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

            return natural_language_query

    # If no keyword is found, select a random query template
    templates = list(keyword_templates.values())
    selected_template = random.choice(templates)
    if selected_template == "find <A> where <B> <Operator> <C>":
        selected_attribute_1 = random.choice([attr for attr in attributes if attribute_types.get(attr) in ["int", "float"]])
        selected_attribute_2 = random.choice([attr for attr in attributes if attribute_types.get(attr) not in ("int", "float") and attr != selected_attribute_1])
        natural_language_query = template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2).replace("<Operator>", operator).replace("<C>", f"{random_value}")
    else:
        selected_attribute_1 = random.choice([attr for attr in attributes if attribute_types.get(attr) in ["int", "float"]])
        selected_attribute_2 = random.choice([attr for attr in attributes if attribute_types.get(attr) not in ("int", "float") and attr != selected_attribute_1])
        natural_language_query = template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

    # selected_attribute_1 = random.choice(attributes)
    # selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])
    # natural_language_query = selected_template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

    return natural_language_query

# Execute the provided MongoDB query and return the result
def execute_query(collection, mongo_query):
    try:
        if isinstance(mongo_query, list):
            result = list(collection.aggregate(mongo_query))
        elif isinstance(mongo_query, dict):
            result = list(collection.find(mongo_query))
        else:
            return "Unsupported query type."
    except Exception as e:
        return str(e)
    display_result(result)
    return result

def get_execute_query(db, collection_name, random_query):
    if random_query:
        print("\n\033[92mTry ask me for example queries (i.e. example mongo queries, average, group, sum, total, min, max, where, order, count, find)\033[0m")
        user_input = input("prompt:\t")
        queries = []
        for iter in range(1,4):
            natural_language_query = generate_example_queries(user_input, db, collection_name)
            
            mongo_query = preprocess(natural_language_query, db, collection_name)
            print(f"\t{iter}.")
            print(f"\t\033[92mExample Query:\033[0m {natural_language_query}")
            print(f"\t\033[92mMongoDB Query:\033[0m {mongo_query}")
            queries.append(mongo_query)

        # Execute the query if user wants me to
        # execution = input("\n\033[92mDo you want me to execute the query for you? (y/n)\033[0m")
        execution = input("\n\033[92mWhich query do you want to execute? (1/2/3) or type any other key to return: \033[0m").strip()
        if execution == '1':
            mongo_pipeline = queries[0]
        elif execution == '2':
            mongo_pipeline = queries[1]
        elif execution == '3':
            mongo_pipeline = queries[2]
        else:
            return
        # if execution.lower().strip() == 'y':
        #     mongo_pipeline = mongo_query
        # else:
        #     return

    else:
        user_input = input("Enter your query: ")
        natural_language_query = user_input

        mongo_pipeline = preprocess(natural_language_query, db, collection_name)

        print(f"Generated Query: db.{collection_name}.aggregate({mongo_pipeline})")
    
    # Execute the generated MongoDB query
    collection = db[collection_name]
    result = None
    try:
        result = list(collection.aggregate(mongo_pipeline))
    except Exception as e:
        result = str(e)
    if any("$lookup" in stage for stage in mongo_pipeline):
        result = display_joined_data(result)
    display_result(result)
    return result

def display_joined_data(results):
    processed_results = []
    for result in results:
        if "joined_data" in result and isinstance(result["joined_data"], list) and result["joined_data"]:
            if len(result["joined_data"]) == 1:
                joined_doc = result["joined_data"][0]
                del result["joined_data"]  
                for key, value in joined_doc.items():
                    if key not in result:
                        result[key] = value
            else:
                result["joined_data_count"] = len(result["joined_data"]) 
        else:
            result.pop("joined_data", None)

        processed_results.append(result)

    return processed_results

def display_result(query_result):
    # print(query_result)
    if not query_result:
        print("No results found.")
    elif isinstance(query_result, str):
        print(f"Error: {query_result}")
    else:
        for i, document in enumerate(query_result, start=1):
            if i == 11:
                break
            print(f"Result {i}:")
            for key, value in document.items():
                print(f"  {key}: {value}")
            print("-" * 40)
            

def is_numeric_attribute(db, collection_name, attribute):
    collection = db[collection_name]
    sample = collection.find_one({attribute: {"$exists": True}})
    if sample and isinstance(sample.get(attribute), (int, float)):
        return True
    return False

def get_attribute_types(db, collection_name):
    collection = db[collection_name]
    sample_document = collection.find_one()

    if not sample_document:
        raise ValueError(f"No documents found in the collection '{collection_name}'.")

    # Infer types from the sample document
    return {key: type(value).__name__ for key, value in sample_document.items()}

def preprocess(user_input, db, collection_name):
    # Tokenize and preprocess user input
    tokens = word_tokenize(user_input.lower())
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.isalnum() and word not in stop_words]
    tagged_tokens = pos_tag(filtered_tokens)

    attributes = get_collection_attributes(db, collection_name) 
    attribute_types = get_attribute_types(db, collection_name)

    if not attributes:
        raise ValueError("No attributes found in the collection for executing the query.")

    keywords = []
    for key in ["sum", "total", "average", "count", "group", "where", "order", "find", "join", "max", "min", "sort", "how many"]:
        if key in tokens:
            keywords.append(key)
    
    if "how many" in user_input.lower():
        keywords.append("how many")

    selected_attributes = [attr for attr in attributes if re.search(rf"\b{attr}\b", user_input, re.IGNORECASE)]

    if not selected_attributes:
        # selected_attributes = [random.choice(attributes)]
        raise ValueError("Please enter a column to explore")

    numeric_attributes = [attr for attr in selected_attributes if attribute_types.get(attr) in ("int", "float")]
    string_attributes = [attr for attr in selected_attributes if attribute_types.get(attr) not in ("int", "float")]

    numeric_attribute = numeric_attributes[0] if numeric_attributes else None
    string_attribute = string_attributes[0] if string_attributes else None

    mongo_pipeline = []

    for keyword in keywords:
        if keyword in ["total", "sum", "average", "max", "min"]:
            # Handle aggregation operations
            if numeric_attribute:
                group_stage = {
                    "$group": {
                        "_id": {string_attribute: f"${string_attribute}"} if string_attribute else None,
                        "result": {
                            "$sum" if keyword in ["total", "sum"] else
                            "$avg" if keyword == "average" else
                            "$max" if keyword == "max" else
                            "$min": f"${numeric_attribute}"
                        },
                    }
                }
                project_stage = {
                    "$project": {
                        "result": 1,
                        string_attribute: 1 if string_attribute else None,
                    }
                }
                mongo_pipeline.append(group_stage)
                mongo_pipeline.append(project_stage)
            else:
                print(f"Cannot perform '{keyword}' on non-numeric fields.")
        elif keyword in ["count", "how many"]:
            # Handle count
            if 'unique' in tokens or 'different' in tokens:
                # Count unique items in a string attribute
                if string_attribute:
                    group_stage = {
                        "$group": {
                            "_id": f"${string_attribute}"  
                        }
                    }
                    count_stage = {
                        "$count": "unique_count" 
                    }
                    mongo_pipeline.append(group_stage)
                    mongo_pipeline.append(count_stage)
                else:
                    raise ValueError("A valid string attribute is required to count unique values.")
            else:
                # Count occurrences of each item
                if string_attribute:
                    group_stage = {
                        "$group": {
                            "_id": f"${string_attribute}", 
                            "count": {"$sum": 1},  
                        }
                    }
                    project_stage = {
                        "$project": {
                            "count": 1,
                            string_attribute: 1  
                        }
                    }
                    mongo_pipeline.append(group_stage)
                    mongo_pipeline.append(project_stage)
                else:
                    raise ValueError("A valid string attribute is required to count occurrences.")

        # elif keyword == "group":
        #     # Handle group by
        #     group_field = next((word for word in tokens if word in selected_attributes), None)
        #     print(f"Selected_field: {selected_attributes}")
        #     if not group_field:
        #         raise ValueError("No valid field found to group by.")

        #     group_stage = {
        #         "$group": {
        #             "_id": f"${group_field}"
        #         }
        #     }
        #     mongo_pipeline.append(group_stage)

        elif keyword == "where":
            # Handle where conditions
            # tokens = word_tokenize(user_input)
            # tokens = user_input.lower().split()
            tokens = user_input.split()
            conditions = {}
            # where_col = next((col for col in selected_attributes if col in tokens), None)
            if "where" in tokens:
                where_index = tokens.index("where")  # Find the index of "where"
                if where_index + 1 < len(tokens):  # Ensure there's a word after "where"
                    where_col = tokens[where_index + 1]  # Get the word directly after "where"
                    # Validate that the word is a valid attribute
                    where_col = where_col if where_col in selected_attributes else None
                else:
                    raise ValueError("No attribute specified after 'where'.")
            else:
                where_col = None
            # print(f"where_col: {where_col}")
            if where_col:
                # Map Python-style operators to MongoDB operators
                operator_map = {
                    "=": "$eq",
                    ">": "$gt",
                    "<": "$lt",
                    ">=": "$gte",
                    "<=": "$lte"
                }

                # Find the operator in the tokens
                # condition_operator = next((op for op in operator_map.keys() if op in tokens), None)
                condition_operator = next((op for op in operator_map.keys() if any(token == op for token in tokens)), None)
                # print(f"condition_operator: {condition_operator}")
                # print(tokens)
                # print(user_input)
                if condition_operator:
                    # Find the value directly following the operator
                    tokens = user_input.split()
                    # print(f"tokens: {tokens}")
                    operator_index = tokens.index(condition_operator)
                    # print("here2")
                    if operator_index + 1 < len(tokens):  # Ensure there's a token after the operator
                        condition_value = tokens[operator_index + 1]

                        # Check if the value is numeric or should remain as a string
                        try:
                            # Attempt to convert to float if numeric
                            condition_value = float(condition_value)
                        except ValueError:
                            # Leave it as a string if conversion fails
                            pass

                        # Map the operator to MongoDB syntax
                        condition_operator_mongo = operator_map[condition_operator]
                        conditions[where_col] = {condition_operator_mongo: condition_value}
                    else:
                        raise ValueError(f"No value provided after operator '{condition_operator}'.")
                else:
                    raise ValueError("Didn't find any operator")
                
            mongo_pipeline.append({"$match": conditions})

            if "find" in keywords:
                project_stage = {
                    "$project": {attr: 1 for attr in selected_attributes}
                }
                mongo_pipeline.append(project_stage)

        elif keyword in ["order", "sort"]:
            # Handle order by
            sort_order = 1 if "asc" in user_input.lower() else -1
            # Find the attribute after "by" in the user input
            if "by" in user_input.lower():
                words = user_input.split()
                words_lower = user_input.lower().split()
                by_index = words_lower.index("by")
                if by_index + 1 < len(words):  # Ensure there's a word after "by"
                    potential_sort_field = words[by_index + 1]
                    if potential_sort_field in attributes:  # Validate it's an attribute
                        sort_field = potential_sort_field
                    else:
                        raise ValueError(f"Field '{potential_sort_field}' after 'by' is not a valid attribute.")
                else:
                    raise ValueError("No field specified after 'by' for ordering.")
                # Deal with project stage
                if keyword == "order":
                    order_index = words.index("order")
                else:
                    order_index = words.index("sort")
                if order_index + 1 < len(words):  # Ensure there's a word after "order"
                    # print(f"potential sort: {potential_sort_field}")
                    # print(f"sort: {sort_field}")
                    potential_project_field = words[order_index + 1]
                    # print(f"attributes: {attributes}")
                    if potential_project_field in attributes:  # Validate it's an attribute
                        project_field = potential_project_field
                        mongo_pipeline.append({
                            "$project": {project_field: 1,
                                        sort_field: 1}
                        })
                    elif potential_project_field == 'by':
                        sort_field = potential_sort_field
                        
                    else:
                        raise ValueError(f"Field '{potential_project_field}' after 'order' is not a valid attribute.")
                else:
                    raise ValueError("No field specified after 'order' for ordering.")
            else:
                # Default to a string or numeric attribute
                sort_field = string_attribute or numeric_attribute
                # mongo_pipeline.append({
                #     "$project": {sort_field: 1}
                # })
            mongo_pipeline.append({
                "$sort": {sort_field: sort_order}
            })
            if "find" in keywords:
                project_stage = {
                    "$project": {attr: 1 for attr in selected_attributes}
                }
                mongo_pipeline.append(project_stage)
            # print(f"potential sort: {potential_sort_field}")
            # print(f"sort: {sort_field}")

            
        elif keyword == "join":
            # Handle join operation
            tokens = user_input.split()  # Tokenize the input for easier parsing

            # Find the collection to join
            if "join" in tokens:
                join_index = tokens.index("join")
                if join_index + 1 < len(tokens):  # Ensure there is a word after "join"
                    join_collection = tokens[join_index + 1]
                else:
                    raise ValueError("No collection specified to join.")

            # Find the fields to join on
            if "on" in tokens:
                on_index = tokens.index("on")
                if on_index + 1 < len(tokens):  # Ensure there are words after "on"
                    # Extract potential join conditions after 'on'
                    join_conditions = tokens[on_index + 1:]
                    local_attributes = attributes
                    # foreign_attributes = get_collection_attributes(db, join_collection)
                    # Look for 'and' separator
                    if "and" in join_conditions:
                        and_index = join_conditions.index("and")
                        if and_index > 0 and and_index + 1 < len(join_conditions):
                            if join_conditions[0] in local_attributes:
                                local_field = join_conditions[0]
                                foreign_field = join_conditions[and_index + 1]
                            else:
                                foreign_field = join_conditions[0]
                                local_field = join_conditions[and_index + 1]
                        else:
                            raise ValueError("Invalid syntax for join conditions.")
                    elif "with" in join_conditions:
                        with_index = join_conditions.index("with")
                        if with_index > 0 and with_index + 1 < len(join_conditions):
                            if join_conditions[0] in local_attributes:
                                local_field = join_conditions[0]
                                foreign_field = join_conditions[with_index + 1]
                            else:
                                foreign_field = join_conditions[0]
                                local_field = join_conditions[with_index + 1]
                        else:
                            raise ValueError("Invalid syntax for join conditions.")
                    else:
                        # If no 'and', assume a single condition and assign both to the same field
                        if len(join_conditions) >= 2:
                            local_field, foreign_field = join_conditions[0], join_conditions[1]
                        else:
                            raise ValueError("Insufficient attributes for join.")
                else:
                    raise ValueError("No fields specified after 'on' for the join.")
            else:
                raise ValueError("'on' keyword is missing in the join query.")

            # Build the $lookup stage for MongoDB
            join_stage = {
                "$lookup": {
                    "from": join_collection,   # The collection to join
                    "localField": local_field,  # Field in the current collection
                    "foreignField": foreign_field,  # Field in the joined collection
                    "as": "joined_data"
                }
            }

            # Add the $lookup stage to the pipeline
            mongo_pipeline.append(join_stage)

            # Add a $match stage to filter out documents where joined_data is empty
            match_stage = {
                "$match": {
                    "joined_data": { "$ne": [] }
                }
            }

            # Add the $match stage to the pipeline
            mongo_pipeline.append(match_stage)

        # elif keyword == "join":
        #     raise ValueError("'join' is not incorporated in this program. Try a different query.")
        elif keyword == "find" and len(keywords)==1:
            # Handle find
            conditions = {attr: {"$exists": True} for attr in selected_attributes}
            mongo_pipeline.append({"$match": conditions})
            project_stage = {
                "$project": {attr: 1 for attr in selected_attributes}
            }
            # print(f"conditions: {conditions}")
            # print(f"project_Stage: {project_stage}")
            mongo_pipeline.append(project_stage)

    if not mongo_pipeline:
        mongo_pipeline = [{"$match": {}}]

    # print(f"selected_attributes: {selected_attributes}")
    # print(f"mongo_pipeline: {mongo_pipeline}")

    return mongo_pipeline
