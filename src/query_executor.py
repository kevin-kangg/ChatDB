# Load dependent libraries.
import json

import pandas as pd

from pymongo import MongoClient

from sqlalchemy import create_engine

#
# This class executes queries and returns
# the result.
#
class QueryExecutor:

    # Constructor method to initialize class variables.
    def __init__(self):
        self.user = 'dsci551user'
        self.passwd = 'Dsci-Project'
        self.database = 'dsci551project'

    # Execute the query in the MySQL database.
    def execMySQLQuery(self, query: str):
        
        # Create the database connection string for MySQL.
        connStr = f"mysql+mysqlconnector://{self.user}:{self.passwd}@localhost/{self.database}"
        # print("Connection string:", connStr)

        # Connect to the MySQL project database.
        mysqlEngine = create_engine(connStr)

        # Verify that the connection is successful.
        try:

            with mysqlEngine.connect() as mysqlConnection:
                #print("Connected to MySQL.")
                None

            # Retrieve the result of the query.
            mysql_result_df = pd.read_sql(query, mysqlEngine)

            # Close the connection.
            mysqlEngine.dispose()

        except Exception as ex:
            print("Failed to connect to MySQL", ex)
            return None

        # Return the result set. 
        return mysql_result_df

    def execMongoFind(self, collection_name: str, query: dict, limit = 0):

        # Create the MongoDB client.
        mongo_client = MongoClient("mongodb://localhost/")

        # Access or create the database.
        my_mongo_db = mongo_client[self.database]

        # Access or create the collection.
        my_mongo_coll = my_mongo_db[collection_name]

        # Execute the find query.
        if limit <= 0:
            result_cursor = my_mongo_coll.find(query)
        else:
            result_cursor = my_mongo_coll.find(query).limit(limit)

        # Retrieve the results.
        mongo_result_df = pd.DataFrame(list(result_cursor))

        # Remove the identifier column.
        #mongo_result_df.drop('_id', axis=1, inplace=True)

        # Close the client connection.
        mongo_client.close()

        # Return the result set.
        return mongo_result_df


    def execMongoAggregate(self, collection_name: str, query: list):

        # Create the MongoDB client.
        mongo_client = MongoClient("mongodb://localhost/")

        # Access or create the database.
        my_mongo_db = mongo_client[self.database]

        # Access or create the collection.
        my_mongo_coll = my_mongo_db[collection_name]

        # Execute the aggregate query.
        result_cursor = my_mongo_coll.aggregate(query)

        # Retrieve the results.
        mongo_result_df = pd.DataFrame(list(result_cursor))

        # Remove the identifier column.
        #mongo_result_df.drop('_id', axis=1, inplace=True)

        # Close the client connection.
        mongo_client.close()

        # Return the result set.
        return mongo_result_df
        

# Test the query executor.
#qexec = QueryExecutor()

# Test MySQL interfaces.
#print(type(qexec.execMySQLQuery("SHOW TABLES").to_json(orient='values')))

# Test MongoDB interfaces.
#print(qexec.execMongoFind("loan", {"person_age": {"$gt": '22.0'}}))
#pipeline = [{"$group": {"_id": "$person_age", "total_income": {"$sum": "$person_income"} } } ]
#print(qexec.execMongoAggregate("loan", pipeline))
#print(qexec.execMongoFind('loan', {}, 3))
