import pandas as pd
from sqlalchemy import create_engine
import sys

if len(sys.argv) != 2:
    print("Error: corrent command lin argument is 'python3 upload_sql.py <csv>'")
    sys.exit(1)

    
file_path = sys.argv[1]

data = pd.read_csv(file_path, encoding = 'ISO-8859-1')

engine = create_engine('mysql+pymysql://root:Dsci-551@localhost:3306/spotify_2023')

data.to_sql(name = 'spotify_data', con = engine, if_exists = 'replace', index = False)
