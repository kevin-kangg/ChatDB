'''
v6.0: add mongodb generate button
'''
import random
import json
import os
import requests
import streamlit as st
import pandas as pd
from bson.json_util import dumps
from sample_queries import get_sample_query
from data_set_processor import DataSetProcessor
from query_executor import QueryExecutor
from SQLCodeGenerator import generate_sql_query
from MongoDBCodeGenerator import mongo_compile

# Initialize session state for SQL tables and MongoDB collections
if "sql_tables" not in st.session_state:
    st.session_state["sql_tables"] = []  # Initialize as an empty list

if "mongo_collections" not in st.session_state:
    st.session_state["mongo_collections"] = []  # Initialize as an empty list

# Instantiate the DataSetProcessor
processor = DataSetProcessor()

# Instantiate the QueryExecutor
query_executor = QueryExecutor()

# Title and description
st.title('ChatDB Query Interface')
st.write('This interface allows users to interact with SQL and NoSQL databases.')

# Divider line for better organization
st.markdown("---")

# Section for Displaying Sample Queries
st.subheader("Sample Query")

# Initialize sample query in session state if not already present
if "sample_query" not in st.session_state:
    st.session_state["sample_query"] = get_sample_query()  # Default sample query

# Display the selected sample query
st.code(st.session_state["sample_query"], language="sql" if "SELECT" in st.session_state["sample_query"] else "json")

# Button to regenerate a single sample query
if st.button("Generate Sample Query"):
    st.session_state["sample_query"] = get_sample_query()  # Randomly select a query (SQL or NoSQL)

# Divider line for better organization
st.markdown("---")

# Section for Query Generation
st.subheader("Generate Query from Natural Language")

# Dropdown to select query type
query_type = st.selectbox("Select Query Type", ["SQL", "MongoDB"])

# Input for natural language query
natural_query = st.text_input("Enter your natural language query")

# Initialize `generated_sql` and `generated_mongo` in session state if not already present
if "generated_sql" not in st.session_state:
    st.session_state["generated_sql"] = None

if "generated_mongo" not in st.session_state:
    st.session_state["generated_mongo"] = None

# Button to generate query
if st.button("Generate Query"):
    if natural_query:
        try:
            if query_type == "SQL":
                sql_schema_dt = processor.getMySQLSchema(st.session_state["sql_tables"])
                st.session_state["generated_sql"] = generate_sql_query(natural_query, sql_schema_dt)
                st.success("Generated SQL Query:")
            elif query_type == "MongoDB":
                mongo_schema_dt = processor.getMongoDBSchema()
                method, collection, execute = mongo_compile(mongo_schema_dt, natural_query)
                st.session_state["generated_mongo"] = (method, collection, execute)
                st.success("Generated MongoDB Query:")
        except Exception as e:
            st.error(f"Error generating query: {e}")
    else:
        st.warning("Please enter a natural language query.")

# Display the generated queries (if they exist)
if st.session_state["generated_sql"] and query_type == "SQL":
    st.code(st.session_state["generated_sql"], language="sql")

if st.session_state["generated_mongo"] and query_type == "MongoDB":
    st.code(st.session_state["generated_mongo"][2], language="json")


# Logic for running the query
if st.session_state["generated_sql"] and query_type == "SQL":
    if st.button("Run Query"):
        try:
            sql_result = query_executor.execMySQLQuery(st.session_state["generated_sql"])
            if sql_result is not None:
                st.write("### SQL Query Results:")
                st.dataframe(sql_result)
            else:
                st.error("No results returned.")
        except Exception as e:
            st.error(f"Error executing SQL query: {e}")
elif st.session_state["generated_mongo"] and query_type == "MongoDB":
    if st.button("Run Query"):
        try:
            # Retrieve the generated Mongo data
            mongo_query_tp = st.session_state["generated_mongo"]
            execute_on = mongo_query_tp[0]
            collection_name = mongo_query_tp[1]
            pipeline = mongo_query_tp[2]

            # Execute the query based on the operation
            if execute_on == "find":
                mongo_result_df = query_executor.execMongoFind(collection_name, pipeline[0])
            else:
                mongo_result_df = query_executor.execMongoAggregate(collection_name, pipeline)

            # Display the results
            if mongo_result_df is not None:
                st.write("### MongoDB Query Results:")
                
                # Retrieve the result set.
                # NOTE: We are only displaying up to 10 documents.
                mongo_result_set_dt = mongo_result_df.head(10).to_dict(orient="records")
                
                # Display the results
                st.json(dumps(mongo_result_set_dt))

            else:
                st.error("No results returned.")
        except Exception as e:
            st.error(f"Error executing MongoDB query: {e}")
else:
    st.warning("No query has been generated yet. Please generate a query first.")


# Divider line for better organization
st.markdown("---")

# Section for Database Exploration
st.subheader("Explore Database")

# Dropdown for database selection
explore_db_type = st.selectbox(
    'Select Database Type to Explore',
    ('SQL', 'NoSQL')
)

if explore_db_type == 'SQL':
    st.write("### Available Tables")
    if not st.session_state["sql_tables"]:
        try:
            tables_df = query_executor.execMySQLQuery("SHOW TABLES")
            st.session_state["sql_tables"] = [table for table in tables_df.iloc[:, 0]]  # Extract table names
        except Exception as e:
            st.error(f"Error accessing MySQL database: {e}")

    table_names = st.session_state["sql_tables"]
    if table_names:
        selected_table = st.selectbox("Select a Table", table_names)
        if selected_table:
            st.write(f"### Sample Data from Table: {selected_table}")
            sample_query = f"SELECT * FROM {selected_table} LIMIT 5;"
            try:
                sample_data = query_executor.execMySQLQuery(sample_query)
                st.dataframe(sample_data)
            except Exception as e:
                st.error(f"Error fetching sample data: {e}")
    else:
        st.write("No tables found in the database.")

elif explore_db_type == 'NoSQL':
    st.write("### Available Collections")
    if not st.session_state["mongo_collections"]:
        try:
            collections = processor.getMongoDBSchema()['collections']
            st.session_state["mongo_collections"] = collections
        except Exception as e:
            st.error(f"Error accessing MongoDB: {e}")

    collections = st.session_state["mongo_collections"]
    if collections:
        selected_collection = st.selectbox("Select a Collection", collections)
        if selected_collection:
            st.write(f"### Sample Document from Collection: {selected_collection}")
            try:
                from bson.json_util import dumps  # Import BSON utilities for serialization
                sample_document = query_executor.execMongoFind(selected_collection, {}, 1)

                if isinstance(sample_document, pd.DataFrame):  # Check if result is a DataFrame
                    if not sample_document.empty:
                        st.json(dumps(sample_document.to_dict(orient="records")))
                    else:
                        st.write("No documents found in this collection.")
                else:
                    st.error("Unexpected data type returned. Expected a DataFrame.")
            except Exception as e:
                st.error(f"Error fetching sample document: {e}")
    else:
        st.write("No collections found in the database.")

# Divider line for better organization
st.markdown("---")



# Section for Data Loading
st.subheader("Load Data into Database")
load_db_type = st.selectbox('Select Database for Data Load', ('MySQL', 'MongoDB'))
uploaded_file = st.file_uploader("Choose a file to load into the database", type=["csv", "json"])
if st.button('Load Data'):
    if uploaded_file is not None:
        file_path = f"./{uploaded_file.name}"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        if load_db_type == 'MySQL' and uploaded_file.name.endswith('.csv'):
            try:
                result = processor.insertSetToMySQL(file_path)
                if result is None:
                    st.error("Failed to load data.")
                else:
                    st.session_state["sql_tables"] = []  # Clear cached tables
                    st.success(f"Data successfully loaded into MySQL. Table `{os.path.splitext(uploaded_file.name)[0]}` is now available.")
            except Exception as e:
                st.error(f"Error loading data into MySQL: {e}")
        elif load_db_type == 'MongoDB' and uploaded_file.name.endswith('.json'):
            try:
                result = processor.insertSetToMongoDB(file_path)
                if result is None:
                    st.error("Failed to load data.")
                else:
                    st.session_state["mongo_collections"] = []  # Clear cached collections
                    st.success(f"Data successfully loaded into MongoDB. Collection `{os.path.splitext(uploaded_file.name)[0]}` is now available.")
            except Exception as e:
                st.error(f"Error loading data into MongoDB: {e}")
        else:
            st.error(f"Please upload a {'CSV' if load_db_type == 'MySQL' else 'JSON'} file for {load_db_type}.")
    else:
        st.warning("Please upload a file before attempting to load data.")
