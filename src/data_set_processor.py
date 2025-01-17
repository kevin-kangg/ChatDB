# Load dependent libraries.
import json
import numpy as np
import os
import pandas as pd

from pymongo import MongoClient

from sqlalchemy import create_engine

from query_executor import QueryExecutor

#
# This class processes data set operations that create
# a table to store the data set and generates insert
# statements to load the data set.
#
class DataSetProcessor:

    # Constructor method to initialize class variables.
    def __init__(self):
        self.user = 'dsci551user'
        self.passwd = 'Dsci-Project'
        self.database = 'dsci551project'

        self.query_executor = QueryExecutor()

    # Insert a new data set to the MySQL database.
    def insertSetToMySQL(self, file_name: str):
            
        # Create the database connection string for MySQL. 
        connStr = f"mysql+mysqlconnector://{self.user}:{self.passwd}@localhost/{self.database}"
        print("Connection string:", connStr)

        # Connect to the MySQL project database.
        mysqlEngine = create_engine(connStr)

        # Verify that the connection is successful.
        try:

            with mysqlEngine.connect() as mysqlConnection:
                print("Connected to MySQL.")

        except Exception as ex:
            print("Failed to connect to MySQL.", ex)
            return None

        # Retrieve the data set and load into data frame.
        # NOTE: This assumes that the first row contains the column names.
        try:

            sql_table_df = pd.read_csv(file_name, header=0)

        except Exception as ex:
            print("Failed to read the CSV data.", ex)
            return None

        # Retrieve the table name (name of the file).
        table_name = os.path.splitext(os.path.basename(file_name))[0]

        # Create the table in the MySQL database.
        ret_val = sql_table_df.to_sql(table_name, mysqlEngine, if_exists='replace', index=False)

        # Verify that the table was created.
        if ret_val is None or ret_val <= 0:
            print(f"Failed to write the table {table_name} to the database.")
            return None

        print(f"({table_name}) Data load successfully imported {ret_val} rows.")

        # Return success flag to the invoker.
        return 1

    # Insert a new data set to the MongoDB database.
    def insertSetToMongoDB(self, file_name: str):
        
        # Create the MongoDB client.
        mongo_client = MongoClient("mongodb://localhost/")

        try:

             # Open the file.
            with open(file_name, 'r', encoding='utf-8') as jf:

                # Read the JSON object.
                json_obj = json.load(jf)

        except Exception as ex:
            print(f"The file {file_name} failed to open.")
            return None

        # Access or create the database.
        my_mongo_db = mongo_client[self.database]

        # Retrieve the collection name (i.e. name of the file).
        collection_name = os.path.splitext(os.path.basename(file_name))[0]

        # Access or create the collection.
        my_mongo_coll = my_mongo_db[collection_name]

        # Write the collection to the database. 
        ret_val = my_mongo_coll.insert_many(json_obj)

        # Close the client connection.
        mongo_client.close()

        # Verify that the collection was imported.
        if not ret_val.acknowledged:
            return None
        
        # Return success flag to the invoker.
        return 1

    # Return the mapping of MySQL table names and features.
    def getMySQLSchema(self, tables: list):

        # Verify that table names were provided.
        if len(tables) == 0:
            return None

        # Define the mapping of tables to column names.
        mysql_tables = dict()
        mysql_tables['tables'] = list(tables)

        # Iterate the list of tables and retrieve column names.
        for table_name in tables:

            # Retrieve the table schema information.
            table_schema_df = self.query_executor.execMySQLQuery(f"DESCRIBE {table_name}")

            # Add the column names to the table mapping.
            mysql_tables[table_name] = table_schema_df.Field.tolist()
            
        # Return the mapping of tables to column names.
        return mysql_tables

    # Return the mapping of MongoDB collection names and features.
    def getMongoDBSchema(self):

        try:

            # Create the MongoDB client.
            mongo_client = MongoClient("mongodb://localhost/")

            # Access or create the database.
            my_mongo_db = mongo_client[self.database]

            # Add collection names to mapping.
            mongo_tables = dict()
            mongo_tables['collections'] = list(my_mongo_db.list_collection_names())

            # Iterate the list of collecitons and retrieve key names.
            for collection_name in my_mongo_db.list_collection_names():

                # Retrieve the collection document information.
                collection_schema_df = self.query_executor.execMongoFind(collection_name, {}, 1)

                # Add key names to the collection mapping.
                mongo_tables[collection_name] = collection_schema_df.keys().tolist()


            # Return the mapping of collections to key names.
            return mongo_tables 

        except Exception as ex:

            print(f"Error::getMongoDBSchema::{ex}")

            # Return failure.
            return None

        finally:

            # Close the client connection.
            mongo_client.close()
        

#########
# Test the processor.

#proc = DataSetProcessor()
#proc.insertSetToMySQL('../data/loan/loan.csv')
#proc.insertSetToMySQL('../data/salaries/salaries.csv')
#proc.insertSetToMySQL('../data/purchases/purchases.csv')
#proc.insertSetToMongoDB('../data/salaries/salaries.json')
#proc.insertSetToMongoDB('../data/loan/loan.json')
#proc.insertSetToMongoDB('../data/purchases/purchases.json')
#print(proc.getMongoDBSchema())
#print(proc.getMySQLSchema(['loan', 'purchases', 'salaries']))

