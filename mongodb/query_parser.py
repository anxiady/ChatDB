import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import pos_tag
import random
import re

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
        # Assume user wants a basic find query
        for word, tag in tagged_tokens:
            if tag.startswith('NN') and 'artist' in word:
                artist_name_index = tokens.index(word) + 1
                if artist_name_index < len(tokens):
                    artist_name = tokens[artist_name_index]
                    # Construct the find query
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
    # Define keywords and corresponding templates
    keyword_templates = {
        "group by": "group by <B> and calculate average of <A>",
        "total": "total <A> by <B>",
        "find": "find <A> for each <B>",
        "sum": "sum of <A> per <B>",
        "average": "average <A> by <B>",
        "count": "count number of <B>",
        "where": "find <A> where <B> equals value",
        "order by": "find <A> and order by <B>",
        "join": "join <A> with <B>"
    }

    # Retrieve collection attributes to dynamically generate queries
    attributes = get_collection_attributes(db, collection_name)
    if not attributes:
        return "No attributes found in the collection for query generation.", {}

    # Check if specific keywords are present in user_input
    for keyword, template in keyword_templates.items():
        if keyword in user_input.lower():
            selected_attribute_1 = random.choice(attributes)
            selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])
            natural_language_query = template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

            return natural_language_query

    # If no keyword is found, select a random query template
    templates = list(keyword_templates.values())
    selected_template = random.choice(templates)
    selected_attribute_1 = random.choice(attributes)
    selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])
    natural_language_query = selected_template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

    return natural_language_query

def generate_query(db_name, collection_name, natural_language_query, selected_attribute_1, selected_attribute_2):
    # Generate the corresponding complete MongoDB query
    if "total" in natural_language_query or "sum" in natural_language_query:
        mongo_pipeline = [
            {"$group": {"_id": f"${selected_attribute_2}", "total": {"$sum": f"${selected_attribute_1}"}}}
        ]
        query = f"db.{db_name}.{collection_name}.aggregate({mongo_pipeline})"
    elif "average" in natural_language_query:
        mongo_pipeline = [
            {"$group": {"_id": f"${selected_attribute_2}", "average": {"$avg": f"${selected_attribute_1}"}}}
        ]
        query = f"db.{db_name}.{collection_name}.aggregate({mongo_pipeline})"
    elif "count" in natural_language_query:
        mongo_pipeline = [
            {"$group": {"_id": f"${selected_attribute_2}", "count": {"$sum": 1}}}
        ]
        query = f"db.{db_name}.{collection_name}.aggregate({mongo_pipeline})"
    elif "group by" in natural_language_query:
        mongo_pipeline = [
            {"$group": {"_id": f"${selected_attribute_2}", "grouped_average": {"$avg": f"${selected_attribute_1}"}}}
        ]
        query = f"db.{db_name}.{collection_name}.aggregate({mongo_pipeline})"
    elif "where" in natural_language_query:
        value = "some_value"  # Replace with actual value or prompt the user for input
        find_filter = {selected_attribute_2: value}
        query = f"db.{db_name}.{collection_name}.find({find_filter})"
    elif "order by" in natural_language_query:
        sort_order = 1 if "asc" in natural_language_query.lower() else -1
        find_filter = {}
        sort = [(selected_attribute_2, sort_order)]
        query = f"db.{db_name}.{collection_name}.find({find_filter}).sort({sort})"
    elif "join" in natural_language_query:
        query = f"db.{db_name}.{collection_name}.aggregate([{{'$lookup': 'details of join'}}])"
    elif "find" in natural_language_query:
        find_filter = {selected_attribute_2: {"$exists": True}}
        query = f"db.{db_name}.{collection_name}.find({find_filter})"
    else:
        query = f"db.{db_name}.{collection_name}.find({{}})"

    return query

def execute_query_from_input(user_input, db, collection_name):
    # Extract collection attributes
    attributes = get_collection_attributes(db, collection_name)
    if not attributes:
        return "No attributes found in the collection for executing the query."

    keyword = None
    for key in ["sum", "total", "average", "count", "group by", "where", "order by", "find"]:
        if key in user_input.lower():
            keyword = key
            break

    selected_attribute_1 = None
    selected_attribute_2 = None
    for attr in attributes:
        if re.search(rf"\b{attr}\b", user_input, re.IGNORECASE):
            if not selected_attribute_1:
                selected_attribute_1 = attr
            elif not selected_attribute_2:
                selected_attribute_2 = attr
                break

    if not selected_attribute_1:
        selected_attribute_1 = random.choice(attributes)
    if not selected_attribute_2:
        selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])

    mongo_query = generate_query(db.name, collection_name, keyword, selected_attribute_1, selected_attribute_2)
    
    collection = db[collection_name]
    result = None
    try:
        exec_query = eval(mongo_query)
        result = list(exec_query)
    except Exception as e:
        result = str(e)

    return result

def execute_query(collection, mongo_query):
    # Execute the provided MongoDB query and return the result
    try:
        if isinstance(mongo_query, list):
            # If it's a pipeline for aggregation
            result = list(collection.aggregate(mongo_query))
        elif isinstance(mongo_query, dict):
            # If it's a find filter
            result = list(collection.find(mongo_query))
        else:
            return "Unsupported query type."
    except Exception as e:
        return str(e)
    display_result(result)
    return result

def get_execute_query(db, collection_name, random_query):
    if random_query:
        print("\n\033[92mTry ask me for example queries (i.e. example mongo queries, average, group by, sum, total)\033[0m")
        user_input = input("prompt:\t")

        natural_language_query = generate_example_queries(user_input, db, collection_name)
        print(f"natural language: {natural_language_query}")
        mongo_query = preprocess(natural_language_query, db, collection_name)
        # Display the generated example query
        print(f"\n\033[92mExample Query:\033[0m {natural_language_query}")
        print(f"\033[92mMongoDB Query:\033[0m {mongo_query}")
        # print(mongo_query)

        # Execute the query if user wants me to
        execution = input("\n\033[92mDo you want me to execute the query for you? (y/n)\033[0m")
        if execution.lower().strip() == 'y':
            mongo_pipeline = mongo_query
        else:
            return

    else:
        user_input = input("Enter your query: ")
        natural_language_query = user_input

        mongo_pipeline = preprocess(natural_language_query, db, collection_name)
    
        # Convert pipeline to string representation
        # query_string = convert_pipeline_to_string(db.name, collection_name, mongo_pipeline)
        # print(f"Generated Query: {query_string}")

        print(f"Generated Query: {mongo_pipeline}")
    
    # Execute the generated MongoDB query
    collection = db[collection_name]
    result = None
    try:
        result = list(collection.aggregate(mongo_pipeline))
    except Exception as e:
        result = str(e)

    display_result(result)
    return result

def display_result(query_result):
    # print(query_result)
    if not query_result:
        print("No results found.")
    elif isinstance(query_result, str):
        print(f"Error: {query_result}")
    else:
        for i, document in enumerate(query_result, start=1):
            if i == 10:
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

    # Retrieve collection attributes and their types
    attributes = get_collection_attributes(db, collection_name)  # Assume this returns a list of attribute names
    attribute_types = get_attribute_types(db, collection_name)  # Assume this returns a dict {attr_name: type}

    if not attributes:
        raise ValueError("No attributes found in the collection for executing the query.")

    # Identify keywords and attributes in the user input
    keywords = []
    for key in ["sum", "total", "average", "count", "group", "where", "order", "find", "join", "max", "min", "sort"]:
        if key in user_input.lower():
            keywords.append(key)

    # Identify the attributes present in the user input
    selected_attributes = [attr for attr in attributes if re.search(rf"\b{attr}\b", user_input, re.IGNORECASE)]

    # Set default attributes if none are identified
    if not selected_attributes:
        selected_attributes = [random.choice(attributes)]

    # Separate the attributes into numeric and string attributes
    numeric_attributes = [attr for attr in selected_attributes if attribute_types.get(attr) in ("int", "float")]
    string_attributes = [attr for attr in selected_attributes if attribute_types.get(attr) not in ("int", "float")]

    # Assign the primary attributes for numeric operations and grouping
    numeric_attribute = numeric_attributes[0] if numeric_attributes else None
    string_attribute = string_attributes[0] if string_attributes else None

    # Generate MongoDB pipeline based on identified keywords and attributes
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
        elif keyword == "count":
            # Handle count
            group_stage = {
                "$group": {
                    "_id": {string_attribute: f"${string_attribute}"} if string_attribute else None,
                    "count": {"$sum": 1},
                }
            }
            project_stage = {
                "$project": {
                    "count": 1,
                    string_attribute: 1 if string_attribute else None,
                }
            }
            mongo_pipeline.append(group_stage)
            mongo_pipeline.append(project_stage)
        elif keyword == "where":
            # Handle where conditions
            conditions = {}
            where_col = next((col for col in selected_attributes if col in filtered_tokens), None)
            if where_col:
                condition_operator = next((op for op in [">", "<", ">=", "<=", "="] if op in filtered_tokens), "=")
                condition_value = next((val for val in filtered_tokens if val.replace(".", "", 1).isdigit()), None)
                if condition_value:
                    conditions[where_col] = {f"${condition_operator}": float(condition_value)}
            mongo_pipeline.append({"$match": conditions})
        elif keyword in ["order", "sort"]:
            # Handle order by
            sort_order = 1 if "asc" in user_input.lower() else -1
            # Find the attribute after "by" in the user input
            if "by" in user_input.lower():
                words = user_input.lower().split()
                by_index = words.index("by")
                if by_index + 1 < len(words):  # Ensure there's a word after "by"
                    potential_sort_field = words[by_index + 1]
                    if potential_sort_field in attributes:  # Validate it's an attribute
                        sort_field = potential_sort_field
                    else:
                        raise ValueError(f"Field '{potential_sort_field}' after 'by' is not a valid attribute.")
                else:
                    raise ValueError("No field specified after 'by' for ordering.")
            else:
                # Default to a string or numeric attribute
                sort_field = string_attribute or numeric_attribute
            mongo_pipeline.append({
                "$sort": {sort_field: sort_order}
            })
        elif keyword == "join":
            # Handle join
            if numeric_attribute:
                mongo_pipeline.append({
                    "$lookup": {
                        "from": "other_collection",  # Replace with actual collection name
                        "localField": numeric_attribute,
                        "foreignField": "_id",
                        "as": "joined_data"
                    }
                })
        elif keyword == "find":
            # Handle find
            conditions = {attr: {"$exists": True} for attr in selected_attributes}
            mongo_pipeline.append({"$match": conditions})

    if not mongo_pipeline:
        mongo_pipeline = [{"$match": {}}]

    print(f"selected_attributes: {selected_attributes}")
    print(f"mongo_pipeline: {mongo_pipeline}")

    return mongo_pipeline
