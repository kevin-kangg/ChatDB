'''
MONGO DB CODE Generator

Expected inputs: 1. query 2. api_data
Ouputs: 
    1. String: execute -> format: string indicating if this is an agg or find function.
    2. String: table_name -> format: string indicating the main table name.
    3. List: pipeline -> format: list of mongodb functions.


Pipeline prioraity is hard coded as follows:
find/aggregate
1, $match,
2, $lookup,
3, $group,
4, $sort,
5, $match (in place of having)
6, $project    

Main function:
execute, table_name, pipeline = mongo_compile
 
'''
import spacy
import nltk
nltk.download("wordnet")
from nltk.corpus import stopwords
from spacy.lang.en.stop_words import STOP_WORDS
from nltk.corpus import wordnet
nltk.download('stopwords')  # Download the stopwords list
from collections import Counter
# Load SpaCy's small English model
nlp = spacy.load("en_core_web_sm")

# Define specific SQL-related keywords and lists
aggregation_functions = {"sum", "average", "min", "max", "count", "total"}

# Define SQL action lists directly
ping_join = {'combine', 'unite', 'aggregate', 'blend', 'mix', 'fuse', 'coalesce', 'meld', 
             'merge', 'unify', 'connect', 'join', 'union', 'conjoin', 'link'}
ping_select = {'choose', 'take', 'select', 'pick'}
ping_where = {"where", "if", "when"}
ping_conditions = {'equal', 'more', 'greater', 'less'}
ping_group = {"group", "by", "for"}
ping_order = {'arrange', 'order', 'organize', 'sort', 'descending', 'ascending'}
ping_agg = {"sum", "average", "min", "max", "count", "total"}
ping_having = {"having", "with"}
order_directions = {'ascending': 1, 'descending': -1}
ping_from = {"from"}


def find_table_and_column_names(query, api_data):
    """
    Business Rules: 
    We only going to find/select coloumn names found in ther data. If not found we print a warning and print select *. 
    
    The code logic:
    Identify table and column names in the query based on API data.
    Returns a dictionary with table names as keys and lists of columns found as values.
    """
    detected_tables = [table for table in api_data['collections'] if table in query]
    columns_for_tables = {}
    print(f"collections in que: {detected_tables}")
    

    for table in detected_tables:
        detected_columns = []  # Initialize an empty list for detected columns
        for col in api_data[table]:  # Loop through each column in the current table
            if col in query:  # Check if the column is in the query
                detected_columns.append(col)  # Add the column to the detected list
        if detected_columns:  # If any columns were detected
            columns_for_tables[table] = detected_columns  # Add to the result dictionary


    # Warning logic if no column names are found for detected tables
    if not columns_for_tables:
        print("Warning: No column names found for detected tables.")
        columns_for_tables = {table: ["*"] for table in detected_tables}  # Default to SELECT * if no columns found

    # Default table_a if no tables found
    if not detected_tables:
        print("Warning: No table names found. Using default 'table_a'.")
        columns_for_tables = {"table_a": ["*"]}

    return detected_tables, columns_for_tables


def find_or_agg(query, detected_tables):
    '''
    If there is aggregate or join then were going to go with aggregate. If neither then were going to go with find.
    '''
    
    # decide between agg or find
    query_tokens = query.split()
    print(f"query tokens {query_tokens}")
    
    print(f"{detected_tables}")
    
    ftab = detected_tables[0]
    
    print(f"{detected_tables}")    
    print(f"Found table: {ftab}")
    
    #base case
    element = "aggregate"
    
    # for word in query_tokens:
    #     if word in ping_join or word in ping_agg:
    #         element  = "aggregate"
    #Check element
    print(f"element: {element}")
    
    #output 
    execute = element
    table_name = ftab
  
    return execute, table_name


def generate_match_clause(query, columns_for_tables, detected_tables):
    """
    Converts a SQL-like WHERE clause into a MongoDB $match clause.
    Produces a basic pipeline with the $match stage.
    """
    import spacy
    
    nlp = spacy.load("en_core_web_sm")

    match_conditions = {}
    
    # Tokenize
    query_tokens = query.split()
    
    # Check to continue
    contains_match = any(word in ping_where for word in query_tokens)

    print(f"Contains Match: {contains_match}")
    
    if not contains_match:
        print("No WHERE clause detected. Skipping processing.")
        return []

    # Proceed with the rest of the processing as is
    # Find the shortened query (stop at the first exit trigger)
    exit_triggers = ping_agg | ping_group | ping_order | ping_join | ping_select | ping_from
    
    start_query = []
    start_at_trigger = False
    for word in query_tokens:
        if word in ping_where:
            start_at_trigger = True
        if start_at_trigger:
            start_query.append(word)

    start_query = " ".join(start_query)

    print(f"Start Query: {start_query}", type(start_query))
    print(f"Columns for Tables: {columns_for_tables}")

    # Shorten the query
    short_query = []
    for word in start_query.split():
        if word in exit_triggers:
            break
        short_query.append(word)

    short_query = " ".join(short_query)
    print(f"Shortened Query: {short_query}", type(short_query))

    # Process the shortened query using NLP
    doc = nlp(short_query)
    tokens_without_stopwords = [token.text for token in doc if not token.is_stop]

    print(f"Tokens without stopwords: {tokens_without_stopwords}")

    # Check if any 'where' keyword is found in the query
    column_name = None
    for token in tokens_without_stopwords:
        for table in detected_tables:
            print(f"Checking if token '{token}' is a column in table '{table}'.")
            if token in columns_for_tables.get(table, []):
                column_name = f"{token}"
                print(f"Detected column name: {column_name}")
                break
        if column_name:
            break

    if not column_name:
        print(f"Available detected tables: {detected_tables}")
        print(f"Available columns for detected tables: {columns_for_tables}")
        print(f"Tokens being checked: {tokens_without_stopwords}")
    
    if 'greater' in query_tokens:
        tokens_without_stopwords.append('greater')
    
    if 'less' in query_tokens:
        tokens_without_stopwords.append('less')
        
    print(f"Tokens without stopwords2: {tokens_without_stopwords}")

    # Extract operator and value
    operator = None
    value = None
    if "equal" in tokens_without_stopwords:
        operator = "$eq"
    elif "greater" in tokens_without_stopwords:
        operator = "$gt"
    elif "less" in tokens_without_stopwords:
        operator = "$lt"
        
    print(f"value:{value}, operator:{operator}")

    # Extract the value (assumes the value is the last token)
    for token in tokens_without_stopwords:
        if token.isdigit():
            value = int(token)  # Numeric value
        elif token.isalpha() and token not in {"equal", "greater", "less"}:
            value = token  # Text value
            
    print(f"extracted col name: {column_name}")

    if operator and value is not None:
        match_conditions[column_name] = {operator: value}
    else:
        raise ValueError("Unable to construct a valid match condition.")

    return {"$match": match_conditions}

def generate_group_stage(query, columns_for_tables, detected_tables, ping_agg, ping_group):
    """
    Generate the $group stage for a MongoDB aggregation pipeline based on the input query.

    Args:
        query (str): The input query.
        columns_for_tables (dict): Mapping of table names to their respective columns.
        detected_tables (list): List of tables detected in the query.
        ping_agg (set): Set of aggregation keywords to check in the query.
        ping_group (set): Set of group-by keywords to check in the query.

    Returns:
        list: A MongoDB aggregation pipeline stage for $group.
    """
    # Tokenize the query
    query_tokens = query.split()

    print(f"Query Tokens: {query_tokens}")

    # Check for grouping or aggregation presence
    contains_group = any(word in ping_group for word in query_tokens)
    contains_aggregation = any(word in ping_agg for word in query_tokens)

    print(f"Contains Group: {contains_group}, Contains Aggregation: {contains_aggregation}")

    # Continue only if grouping or aggregation is detected
    if not (contains_group or contains_aggregation):
        print("Neither grouping nor aggregation detected. Skipping processing.")
        return []

    # Short query to extract group-by fields
    exit_triggers = {"select", "order", "sort", "from", "where", "having"}
    start_query = []
    start_at_trigger = False
    for word in query_tokens:
        if word in ping_group:
            start_at_trigger = True
        if start_at_trigger:
            start_query.append(word)

    start_query = " ".join(start_query)
    print(f"Start Query: {start_query}", type(start_query))

    # Detect group-by columns
    detected_columns = []
    for table in detected_tables:
        for column in columns_for_tables.get(table, []):
            if column in start_query:
                detected_columns.append((table, column))

    print(f"Detected Columns: {detected_columns}", f"type: {type(detected_columns)}")

    # Group-by field
    group_by_field = f"${detected_columns[0][1]}" if detected_columns else None

    # Detect aggregation fields
    start_agg = []
    start_agg_trigger = False
    for word in query_tokens:
        if word in ping_agg:
            start_agg_trigger = True
        if start_agg_trigger:
            start_agg.append(word)

    start_agg = " ".join(start_agg)
    print(f"Start Aggregation Query: {start_agg}", type(start_agg))

    # Shorten aggregation query
    short_agg = []
    for word in start_agg.split():
        if word in exit_triggers:
            break
        short_agg.append(word)

    short_agg = " ".join(short_agg)
    print(f"Shortened Aggregation Query: {short_agg}", type(short_agg))

    # Process the shortened aggregation query
    agg_function = None
    short_agg_tokens = short_agg.split()
    aggregation_fields = {}

    # Fetch all columns from detected tables
    relevant_columns = []
    for table in detected_tables:
        relevant_columns.extend(columns_for_tables.get(table, []))

    print(f"Relevant Columns for Aggregation: {relevant_columns}")

    for i, word in enumerate(short_agg_tokens):
        if word in ping_agg:  # Check for aggregation function
            agg_function = word
            agg_field = None

            print(f"agg_function: {agg_function}")

            # Look for the field to aggregate on
            for next_word in short_agg_tokens[i + 1:]:
                if next_word in relevant_columns:  # Match column from relevant columns
                    agg_field = f"${next_word}"  # Use the first detected table

                    # Map to MongoDB aggregation functions
                    if agg_function == "sum":
                        aggregation_fields[f"total_{next_word}"] = {"$sum": agg_field}
                    elif agg_function == "average":
                        aggregation_fields[f"avg_{next_word}"] = {"$avg": agg_field}
                    elif agg_function == "count":
                        aggregation_fields["count"] = {"$sum": 1}  # MongoDB count approximation

                    # Debug output
                    print(f"DEBUG: Aggregation Field: {agg_field}, Function: {agg_function}")
                    break  # Exit loop once field is matched

    # Final debug output
    print("Final Aggregation Fields:", aggregation_fields)

    # Construct the $group stage
    group_stage = {
        "$group": {
            "_id": group_by_field,
            **aggregation_fields
        }
    }

    print("Generated $group Stage:", group_stage)
    return [group_stage]

def generate_sort_clause(query, detected_tables, columns_for_tables, ping_order, order_directions):
    """
    Generate a MongoDB $sort clause based on the user query.

    Args:
        query (str): User-defined query.
        detected_tables (list): List of detected table names.
        columns_for_tables (dict): Dictionary of detected tables and their corresponding columns.
        ping_order (set): Set of keywords that indicate sorting.
        order_directions (dict): Dictionary mapping sort direction keywords to MongoDB direction values.

    Returns:
        dict: MongoDB $sort clause in the correct format.
    """
    # Split the query into words for processing
    words = query.split()
    sort_columns = []
    direction = 1  # Default to ascending
    exit_triggers = {"where", "group", "join", "aggregate", "select"}
    
    # Check if the query contains sorting keywords
    if not any(word in ping_order for word in words):
        print("No sorting keyword detected in the query. Exiting.")
        return None  # Exit if no sorting keyword is detected
    
    # Detect projection trigger and shorten query
    shortened_query = []
    for idx, word in enumerate(words):
        if word in ping_order:
            for word_after in words[idx + 1:]:
                if word_after in exit_triggers:
                    break
                shortened_query.append(word_after)
            break
    print(f"Shortened Query (raw): {shortened_query}")
    
    # Filter out stop words
    filtered_query = shortened_query  # Simplified for clarity
    print(f"Filtered Query: {filtered_query}")
    
    # Identify columns for sorting
    sort_columns = []
    for word in filtered_query:
        for table in detected_tables:
            if word in columns_for_tables.get(table, []):
                sort_columns.append(word)
                break  # Stop searching once a match is found for this word
    
    # Detect sorting direction in the query
    for word in words:
        if word in order_directions:
            direction = order_directions[word]
            break
    
    # Special handling for grouped fields (_id)
    if "group" in query and "by" in query:
        # If grouped, sort by `_id` and any aggregated fields
        result = {"$sort": {"_id": direction, "total_loan_amnt": direction}}
    else:
        # Regular sorting for detected columns
        if len(sort_columns) == 1:
            result = {"$sort": {sort_columns[0]: direction}}
        elif len(sort_columns) > 1:
            result = {"$sort": {col: direction for col in sort_columns}}
        else:
            # Default case: Sort by `_id` if no valid columns detected
            result = {"$sort": {"_id": direction}}
    
    print(f"Generated $sort clause: {result}")
    return result

def generate_have_clause(query, columns_for_tables, detected_tables, group_stage):
    """
    Converts a SQL-like HAVING clause into a MongoDB $match clause using the outputs of a $group stage.
    Produces a $match stage for the pipeline.
    
    Args:
        query (str): The input query.
        columns_for_tables (dict): Detected tables and their columns.
        detected_tables (list): List of detected tables in the query.
        group_stage (dict): The output of a $group stage to reference aggregated fields.

    Returns:
        dict: A MongoDB $match clause.
    """
    import spacy
    
    nlp = spacy.load("en_core_web_sm")

    match_conditions = {}
    
    # Tokenize the query
    query_tokens = query.split()

    # Check for HAVING clause trigger
    contains_have = any(word in ping_having for word in query_tokens)
    print(f"HAVING: Ping list? {contains_have}")

    if not contains_have:
        print("No HAVING clause detected. Skipping processing.")
        return []

    # Find the shortened query after HAVING
    exit_triggers = ping_agg | ping_group | ping_order | ping_join | ping_select | ping_from
    start_query = []
    start_at_trigger = False
    for word in query_tokens:
        if word in ping_having:
            start_at_trigger = True
        if start_at_trigger:
            start_query.append(word)

    start_query = " ".join(start_query)
    print(f"Start Query: {start_query}")

    # Shorten the query to remove exit triggers
    short_query = []
    for word in start_query.split():
        if word in exit_triggers:
            break
        short_query.append(word)

    short_query = " ".join(short_query)
    print(f"Shortened Query: {short_query}")

    # Process the shortened query using NLP
    doc = nlp(short_query)
    tokens_without_stopwords = [token.text for token in doc if not token.is_stop]
    print(f"Tokens without stopwords: {tokens_without_stopwords}")
 # Extract necessary components for $match
    if len(tokens_without_stopwords) < 3:
        raise ValueError("Insufficient tokens to construct a HAVING clause.")
    
    # Assume the tokens are in the order: ['having', <field_name>, <value>]
    _, field_name, value = tokens_without_stopwords
    
    # Try to parse the value as a number; if not, treat as string
    try:
        value = float(value) if '.' in value else int(value)
    except ValueError:
        pass  # Value remains as a string if it cannot be parsed

    # Determine the operator from context or default to $gte
    # You can extend this logic if more operators are needed
    operator = "$gte"  # Default assumption; change based on query requirements

    # Construct the $match condition
    match_conditions[field_name] = {operator: value}
    print(f"Constructed HAVING $match condition: {match_conditions}")

    return {"$match": match_conditions}


def generate_project_clause(query, columns_for_tables, detected_tables, group_stage, execute):
    """
    Generate a MongoDB $project clause based on the query and conditions.

    Args:
        query (str): The input query string.
        group_stage (list): The $group stage if aggregation is present.
        columns_for_tables (dict): A dictionary of detected tables and their respective columns.
        detected_tables (list): A list of tables detected in the query.
        execute (str): Determines the type of operation ('aggregate' or 'find').

    Returns:
        dict: The MongoDB $project clause with a flag indicating the processing path.
    """
    project_clause = {}
    processing_path = None  # Flag to indicate which condition was processed

    # Step 1: Always Include Grouping Field in the $project Clause (if exists)
    if group_stage and len(group_stage) > 0 and "$group" in group_stage[0] and execute == "aggregate":
        group_fields = group_stage[0]["$group"]
        print(f"1. group_fields: {group_fields}")
 
        # Include grouping field (_id) in the project clause
        if "_id" in group_fields:
            _id_value = group_fields["_id"]
            if isinstance(_id_value, str) and _id_value.startswith("$"):
                # Extract field name and clean leading '$'
                field_name = _id_value.lstrip("$")
                project_clause[field_name] = "$_id"  # Map the cleaned name to '$_id'
            else:
                project_clause["grouped_by"] = "$_id"

        print(f"2. Project clause 1: {project_clause}")

        # Include all other aggregated fields in the project clause
        for field in group_fields:
            if field != "_id":
                project_clause[field] = f"${field}"  # Include aggregation fields

        project_clause["_id"] = 0
        processing_path = "aggregation"
        
        print(f"3. Project clause 2: {project_clause}")

    # Step 2: Fallback to Column Detection if `project_clause` is Empty or if `execute == 'find'`
    if not project_clause or execute == "find":
        query_lower = query.lower()
        query_tokens = query_lower.split()
        print(f"check project from agg: {project_clause}")

        # Shorten the query: Extract everything after `SELECT` and stop at `FROM` or other exit triggers
        exit_triggers = {"from", "where", "group", "order", "join"}
        shortened_query = []
        start_query = []
        start_collecting = False
        for word in query_tokens:
            # Start collecting after a `ping_select` keyword
            if word in ping_select:
                start_collecting = True
                start_query.append(word)
                continue

            # Stop collecting at an exit trigger
            if start_collecting and word in exit_triggers:
                break
            if start_collecting:
                shortened_query.append(word)

        shortened_query = " ".join(shortened_query).strip()
        print(f"Project Shortened Query: {shortened_query}")

        # Detect columns in the shortened query
        detected_columns = []
        for table in detected_tables:
            for column in columns_for_tables.get(table, []):
                if column in shortened_query.split():  # Match columns in the shortened query
                    detected_columns.append((table, column))

        print(f"Detected Columns: {detected_columns}")

        # Add detected columns to the $project clause
        for table, column in detected_columns:
            project_clause[column] = f"${column}"

        # Ensure `_id` is excluded
        project_clause["_id"] = 0
        processing_path = "default_select" if execute == "find" else "aggregation_fallback"
    
    print(f"Processed By: {processing_path}")
    return {"$project": project_clause}


def generate_lookup_clause(query, api_data, ping_join):
    """
    Generate a MongoDB $lookup clause for joining two tables based on a natural language query.

    Args:
        query (str): The input query string.
        api_data (dict): API dictionary containing table names and their columns.
        ping_join (set): Set of keywords indicating a join operation.

    Returns:
        list: A MongoDB $lookup stage and any subsequent stages for the pipeline,
              or None if no ping join keyword is found.
    """

    # Tokenize the query
    words = query.split()  # Basic tokenization for simplicity
    
    # Step 1: Check for ping join keywords
    if any(word in ping_join for word in words):
        print("Ping join keyword found. Proceeding with query analysis.")
    else:
        print("No ping join keyword detected in the query. Exiting.")
        return None  # Exit if no keyword from ping_join is detected
    
    # Step 2: Detect all known tables in the query
    detected_tables = []  # To store identified table names
    for word in words:
        if word in api_data['collections']:
            detected_tables.append(word)
            detected_tables = list(dict.fromkeys(detected_tables))  # Remove duplicates
            
    print(f"dec tables {detected_tables}")

    # Step 3: Handle scenarios for detected tables
    table_count = len(detected_tables)
    if table_count > 1:
        # Case: Two tables detected
        primary_table = detected_tables[0]
        secondary_table = detected_tables[1]

        # Step 4: Find the ON condition
        join_condition = None
        for idx, word in enumerate(words):
            if word.lower() == "on":
                # Look for the column name following "ON"
                if idx + 1 < len(words):
                    potential_column = words[idx + 1]
                    if (potential_column in api_data[primary_table] and
                            potential_column in api_data[secondary_table]):
                        join_condition = potential_column
                        break

        # Default to 'id' if no condition is found
        if not join_condition:
            print(f"WARNING: No ON condition found for {primary_table} and {secondary_table}. Defaulting to 'id'.")
            join_condition = "id"

        # Step 5: Generate the $lookup stage
        lookup_stage = {
            "$lookup": {
                "from": secondary_table,
                "localField": join_condition,
                "foreignField": join_condition,
                "as": f"{secondary_table}_joined"
            }
        }

        return [lookup_stage]
    else:
        print("Insufficient tables detected for a join operation.")
        return None

def assemble_pipeline(*pipeline_stages):
    """
    Assemble the MongoDB aggregation pipeline by including only non-empty stages.

    Args:
        *pipeline_stages: Variable-length arguments of pipeline stages (dictionaries or lists).

    Returns:
        list: A complete MongoDB aggregation pipeline containing only non-empty stages.
    """
    pipeline = []

    for stage in pipeline_stages:
        if stage:  # Check if the stage is non-empty
            if isinstance(stage, list):  # If the stage is a list of stages
                pipeline.extend(stage)
            else:  # If the stage is a single dictionary stage
                pipeline.append(stage)

    return pipeline


# Example usage

'''
Pipeline prioraity is hard coded as follows:
find/aggregate
1, $match,
2, $lookup,
3, $group,
4, $sort,
5, $match (as place holder foe a 'having' equivalent)
6, $project

'''

# # query = "blah blah blah where person_age less than 25 from loan select person_gender"
# detected_tables, columns_for_tables = find_table_and_column_names(query, api_data)
# execute_on = find_or_agg(query)
# pipe_group = generate_group_stage(query, columns_for_tables, detected_tables, ping_agg, ping_group)
# pipe_match = generate_match_clause(query, columns_for_tables, detected_tables)
# pipe_sort = generate_sort_clause(query, detected_tables, columns_for_tables, ping_order, order_directions)
# pipe_proj = generate_project_clause(query, columns_for_tables, detected_tables, group_stage= pipe_group)
# pipe_look = generate_lookup_clause(query, api_data, ping_join)
# pipeline = assemble_pipeline(pipe_match, pipe_look, pipe_group, pipe_sort, pipe_proj)

# print(execute_on, pipeline)

def mongo_compile(api_data, query):
    """
    Compile a MongoDB query pipeline and determine execution context.

    Args:
        api_data (dict): The metadata of available tables and columns.
        query (str): The query string to process.

    Returns:
        tuple: A tuple containing:
            - execute_on (str): The determined execution context (find or aggregation).
            - pipeline (list): The MongoDB pipeline to execute.
    """
    # Step 1: Detect tables and columns
    detected_tables, columns_for_tables = find_table_and_column_names(query, api_data)

    # Step 2: Determine execution context
    execute, table_name = find_or_agg(query, detected_tables)

    # Step 3: Generate stages
    pipe_group = generate_group_stage(query, columns_for_tables, detected_tables, ping_agg, ping_group)
    pipe_match = generate_match_clause(query, columns_for_tables, detected_tables)
    pipe_sort = generate_sort_clause(query, detected_tables, columns_for_tables, ping_order, order_directions)
    pipe_have = generate_have_clause(query, columns_for_tables, detected_tables, group_stage= pipe_group)
    pipe_proj =  generate_project_clause(query, columns_for_tables, detected_tables, group_stage= pipe_group, execute = execute)
    pipe_look = generate_lookup_clause(query, api_data, ping_join)

    # Step 4: Assemble the pipeline
    pipeline = assemble_pipeline(pipe_match, pipe_look, pipe_group, pipe_sort, pipe_have, pipe_proj)

    return execute, table_name, pipeline

# my_data = {'collections': ['salaries', 'loan', 'purchases'], 'salaries': ['work_year', 'person_gender', 'experience_level', 'employment_type', 'job_title', 'salary', 'income_class', 'salary_currency', 'salary_in_usd', 'employee_residence', 'remote_ratio', 'company_location', 'company_size'], 
#            'loan': ['person_age', 'person_gender', 'person_education', 'person_income', 'income_class', 'person_emp_exp', 'person_home_ownership', 'loan_amnt', 'loan_intent', 'loan_int_rate', 'loan_percent_income', 'cb_person_cred_hist_length', 'credit_score', 'previous_loan_defaults_on_file', 'loan_status'], 'purchases': ['person_age', 'person_gender', 'person_income', 'income_class', 'number_of_purchases', 'product_category', 'time_on_site', 'loyalty_program', 'discounts', 'purchase_status']}

# my_query = "group by person_home_ownership having person_gender equal to male from loan"
# mongo_compile(my_data, my_query)