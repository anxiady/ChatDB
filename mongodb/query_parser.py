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

            return natural_language_query, generate_query(db.name, collection_name, natural_language_query, selected_attribute_1, selected_attribute_2)

    # If no keyword is found, select a random query template
    templates = list(keyword_templates.values())
    selected_template = random.choice(templates)
    selected_attribute_1 = random.choice(attributes)
    selected_attribute_2 = random.choice([attr for attr in attributes if attr != selected_attribute_1])
    natural_language_query = selected_template.replace("<A>", selected_attribute_1).replace("<B>", selected_attribute_2)

    return natural_language_query, generate_query(db.name, collection_name, natural_language_query, selected_attribute_1, selected_attribute_2)

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

def display_result(query_result):
    if not query_result:
        print("No results found.")
    elif isinstance(query_result, str):
        print(f"Error: {query_result}")
    else:
        for i, document in enumerate(query_result, start=1):
            print(f"Result {i}:")
            for key, value in document.items():
                print(f"  {key}: {value}")
            print("-" * 40)

