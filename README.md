# ChatDB
Learning to Query Database Systems Like ChatGPT

User guide:
Navigate to the directory where you saved the project folder.
Execute “/opt/homebrew/bin/python3.11 /Users/[your own directory]/ChatDB/main.py”
Follow the instructions given.
Note: when you upload the extra dataset, please pay attention to the file type. (.csv for SQL and .json for MongoDB)

Project Structure:
	The project is divided into two parts “mongodb” and ”mysql”, where “mongodb” handles NLP processing for MongoDB queries and “mysql” handles NLP processing for SQL queries. They are stored in two separate folders called “mongodb” and “mysql”. Each of these folders contains a Python file for connection: “connection.py” and “Upload_SQL.py” which handle the connection to the local databases. In “mongodb”, “queries.py” basically handles nicely displaying columns and sample rows. The “query_parser.py” takes care of NLP processing, query execution, and result display. In “mysql” folder, “SQL_Queries.py” also takes care of processing user input, generating and executing SQL queries, and displaying results.  

    The “data” folder contains all datasets for both SQL and NoSQL databases. They will be accessed through the code in “mongodb” and ”mysql” folders. 
    
	At a higher level, “mongo_main.py” and “mysql_main.py” handle the two user interfaces for MongoDB and Mysql in Terminal. They employ functions from “mongodb” and ”mysql” python files to complete user requests. Lastly, “main.py” is the main file to execute the code. It integrates the two systems and handles the general interface for the application. 
