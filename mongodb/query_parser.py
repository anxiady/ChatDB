import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import pos_tag

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
