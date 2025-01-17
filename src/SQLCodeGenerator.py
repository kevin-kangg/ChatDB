'''
Expected input: 
    Dictionary
    api_data = {
        'tables': ['income', 'customer', 'debt'],
        'income': ['id', 'amount', 'date', 'city', 'zip'],
        'customer': ['customer_id', 'name', 'email', 'city'],
        'debt': ['debt_id', 'amount', 'income', 'city', 'zip']
        }

Process Details:
Seven functions results compiled into string by "generate_sql_query" function.
    1. find_table_and_column_names(query, api_data)
    2. generate_select_clause(query, api_data, columns_for_tables, detected_tables)
    3. generate_from_and_joins(query, api_data, ping_join)  # Pass the query and columns_for_tables
    4. generate_where_clause(query, columns_for_tables, detected_tables)
    5. generate_group_by_clause(query, columns_for_tables, detected_tables, ping_agg)
    6. generate_having_clause(query, columns_for_tables, detected_tables, group_by_clause)
    7. generate_order_by_clause(query, columns_for_tables, detected_tables, select_clause_parts)
    
Output:
String = sql_query

'''
import spacy
import nltk
from spacy.lang.en.stop_words import STOP_WORDS
from nltk.corpus import wordnet
from collections import Counter

# Load SpaCy's small English model
nlp = spacy.load("en_core_web_sm")

# Download WordNet data if not already downloaded
nltk.download("wordnet")

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
ping_from = {"from"}


def find_table_and_column_names(query, api_data):
    """
    Business Rules: 
    We only going to find/select coloumn names found in ther data. If not found we print a warning and print select *. 
    
    The code logic:
    Identify table and column names in the query based on API data.
    Returns a dictionary with table names as keys and lists of columns found as values.
    """
    detected_tables = [table for table in api_data['tables'] if table in query]
    columns_for_tables = {}

    for table in detected_tables:
        detected_columns = [col for col in api_data[table] if col in query]
        if detected_columns:
            columns_for_tables[table] = detected_columns

    # Warning logic if no column names are found for detected tables
    if not columns_for_tables:
        print("Warning: No column names found for detected tables.")
        columns_for_tables = {table: ["*"] for table in detected_tables}  # Default to SELECT * if no columns found

    # Default table_a if no tables found
    if not detected_tables:
        print("Warning: No table names found. Using default 'table_a'.")
        columns_for_tables = {"table_a": ["*"]}

    return detected_tables, columns_for_tables

def generate_select_clause(query, api_data, columns_for_tables, detected_tables):
    """
    Generate the SELECT clause of an SQL query based on a natural language query.

    Steps:
    1. Detect the SELECT list trigger. If found:
       - Shorten the query to start at the SELECT trigger and end at the next exit trigger or query end.
    2. Look for known column names from identified tables in the shortened query.
    3. If an aggregation function is found, prioritize and include only the aggregation.
    4. If no SELECT trigger is found, gather all known column names from identified tables.
    5. If no column names are matched, default to SELECT *.

    Args:
        query (str): The input query string.
        api_data (dict): API data containing table names and their columns.
        columns_for_tables (dict): A dictionary of detected tables and their respective columns.
        detected_tables (list): A list of tables detected in the query.

    Returns:
        str: The SQL SELECT clause.
        list: A list of selected columns.
    """
    select_clause_parts = []
    doc = nlp(query)

    # Define clause triggers
    select_triggers = ping_select
    exit_triggers = ping_where | ping_group | ping_order | ping_join

    # Step 1: Detect SELECT trigger and shorten query
    shortened_query = ""
    select_trigger_found = False
    for idx, token in enumerate(doc):
        if token.lemma_ in select_triggers:
            select_trigger_found = True

            for token_after in doc[idx:]:
                if token_after.lemma_ in exit_triggers:
                    break
                shortened_query += token_after.text + " "
            shortened_query = shortened_query.strip()
       
            break

    # Step 2: Look for known column names in shortened query
    if shortened_query:
        for table, columns in columns_for_tables.items():
            for col in columns:
                if col in shortened_query.split():
                    select_clause_parts.append(f"{table}.{col}")

    # Step 3: Check for aggregation functions
    if any(token.lemma_ in aggregation_functions for token in doc):
        aggregation_parts = []
        for idx, token in enumerate(doc):
            if token.lemma_ in aggregation_functions:

                for token_after in doc[idx + 1:]:
                    if token_after.lemma_ in exit_triggers:
                        break
                    for table, columns in columns_for_tables.items():
                        if token_after.text in columns:
                            aggregation_parts.append(f"{token.lemma_}({table}.{token_after.text})")
                break
        if aggregation_parts:
            select_clause_parts = aggregation_parts

    # Step 4: If no SELECT trigger, gather known columns from all detected tables
    if not select_trigger_found and not select_clause_parts:

        for table, columns in columns_for_tables.items():
            for col in columns:
                select_clause_parts.append(f"{table}.{col}")

    # Step 5: Default to SELECT * if no columns are found
    if not select_clause_parts:
        print("Warning: No column names found, defaulting to SELECT *.")
        select_clause_parts = ["*"]

    # Create the SELECT clause
    return f"SELECT {', '.join(select_clause_parts)}", select_clause_parts



def generate_from_and_joins(query, api_data, ping_join):
    """
    Parse the SQL query to detect tables and generate a FROM clause with LEFT JOINs based on the primary table.

    Args:
        query (str): The input SQL query.
        api_data (dict): API dictionary containing table names and their columns.
        ping_join (set): Set of keywords indicating a join operation.

    Returns:
        str: The SQL FROM and JOIN clauses as a single string.
    """
    # Tokenize the query
    doc = nlp(query)
    detected_tables = []  # To store identified table names
    join_conditions = []  # To store identified ON conditions

    # Step 1: Detect all known tables in the query
    for token in doc:
        if token.text in api_data['tables']:
            detected_tables.append(token.text)
    detected_tables = list(dict.fromkeys(detected_tables))  # Remove duplicates

    # Step 2: Count tables and handle scenarios
    table_count = len(detected_tables)

    # Case 1: Single table
    if table_count == 1:
        if "from" in query.lower():  # Check for FROM clause
            return f"FROM {detected_tables[0]}"
        else:
            print("WARNING: FROM clause not clear. Defaulting to table_a.")
            return "FROM table_a"

    # Case 2 and beyond: Multiple tables
    primary_table = detected_tables[0]
    join_clauses = []

    for secondary_table in detected_tables[1:]:
        # Look for "ON [known column]" phrases
        on_condition = None
        for token in doc:
            if token.text.lower() == "on":
                # Collect the known column name following "ON"
                column_index = token.i + 1  # Use token.i to get the index
                if column_index < len(doc):
                    column = doc[column_index].text
                    if column in api_data[primary_table] and column in api_data[secondary_table]:
                        on_condition = column
                        break
        # Use the found column or default to a generic column
        if on_condition:
            join_clauses.append(f"INNER JOIN {secondary_table} as {secondary_table} ON {primary_table}.{on_condition} = {secondary_table}.{on_condition}")
        else:
            print(f"WARNING: No ON condition found for {primary_table} and {secondary_table}. Defaulting to 'id'.")
            join_clauses.append(f"INNER JOIN {secondary_table} as {secondary_table} ON {primary_table}.id = {secondary_table}.id")

    # Assemble the FROM and JOIN clauses
    from_clause = f"FROM {primary_table} as {primary_table} " + " ".join(join_clauses)
    return from_clause


def generate_where_clause(query, columns_for_tables, detected_tables):
    '''
    Business Rules:
    If a where-related keyword is triggered, search the query for the next column name
    based on `columns_for_tables`, then search for a condition keyword, and finally locate
    the next noun or number for the value.
    '''
    doc = nlp(query)
    where_conditions = []
    
    print(f"WHERE doc: {doc}")
    
    # Check if any 'where' keyword is found in the query
    if any(token.lemma_ in ping_where for token in doc):
        for token in doc:
            # Check for 'where' keywords without skipping them
            if token.lemma_ in ping_where:
     
                # Look for the nearest column name after 'where' keyword
                column_name = None
                for next_token in doc[token.i + 1:]:
                    # Find first matching column for any detected table
                    for table in detected_tables:
                        if next_token.text in columns_for_tables.get(table, []):
                            column_name = f"{table}.{next_token.text}"
                            print(f"WHERE column_name in loop: {column_name}")
                            break
                    if column_name:
                        # Look for condition words after finding a column
                        operator = None
                        for condition_token in doc[next_token.i + 1:]:
                            # Check both .text and .lemma_ for condition matching
                            if condition_token.text in ping_conditions or condition_token.lemma_ in ping_conditions:
                                # Map condition keywords to SQL operators
                                if condition_token.lemma_ == "equal":
                                    operator = "="
                                elif condition_token.lemma_ in {"more", "greater"} or condition_token.text in {"more", "greater"}:
                                    operator = ">"
                                elif condition_token.lemma_ == "less" or condition_token.text == "less":
                                    operator = "<"
                                    
                                print(f"WHERE condition in loop: {condition_token.lemma_}")
                                print(f"WHERE operator in loop: {operator}")
                                break
                        
                        # Only proceed to find value if a condition was found
                        if operator:
                            value = None
                            for value_token in doc[condition_token.i + 1:]:
                                print(f"WHERE value token in loop: {value_token}")
                                if value_token.is_stop:
                                    continue  # Skip stop words
                                
                                if value_token.like_num:
                                    value = value_token.text
                                    break
                                elif value_token.pos_ in {"NOUN", "PROPN", "ADJ"}:
                                    value = f"'{value_token.text}'"  # Quote strings
                                    break
                            if value:  # Append the full condition if value is found
                                where_conditions.append(f"{column_name} {operator} {value}")
                        break
                break

    return f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

def generate_group_by_clause(query, columns_for_tables, detected_tables, ping_agg):
    """
    Generate the GROUP BY clause for an SQL query based on identified aggregations and column names.

    Args:
        query (str): The input SQL query.
        columns_for_tables (dict): Mapping of table names to their respective columns.
        detected_tables (list): List of tables detected in the query.
        ping_agg (list): List of aggregation keywords to check in the query.

    Returns:
        str: The generated GROUP BY clause, or an empty string if no aggregation is found.
    """
    # Define exit trigger keywords
    exit_triggers = {"select", "sum", "order", "sort", "from", "where", "having"}

    # # Step 1: Check for aggregation keywords in the query
    doc = nlp(query)
    # contains_aggregation = any(token.text.lower() in ping_agg for token in doc)
    # print(f"contains agg {contains_aggregation}")

    # if not contains_aggregation:
    #     return ""  # Skip GROUP BY clause generation if no aggregation detected

    # Step 2: Determine the starting point for shortening the query
    start_keyword = None
    for keyword in ["group", "by", "for"]:
        for token in doc:
            if token.text.lower() == keyword:
                start_keyword = keyword
                break
        if start_keyword:
            break

    if not start_keyword:

        return ""
    
    print(f"group start keyword: {start_keyword}")

    # Step 3: Shorten the query based on the starting keyword and exit triggers
    start_index = next((idx for idx, token in enumerate(doc) if token.text.lower() == start_keyword), -1)
    if start_index == -1:

        return ""

    # Collect tokens until an exit trigger is found
    shortened_query_tokens = []
    for token in doc[start_index:]:
        if token.text.lower() in exit_triggers:

            break
        shortened_query_tokens.append(token.text)

    shortened_query = " ".join(shortened_query_tokens)
    print(f"Group func short que: {shortened_query}")

    # Step 4: Remove stop words from the shortened query
    cleaned_tokens = [token.text for token in nlp(shortened_query) if not token.is_stop]
    cleaned_query = " ".join(cleaned_tokens)
    print(f"Group func no stop : {cleaned_query}")

    # Step 5: Identify columns for GROUP BY from the cleaned query
    cleaned_doc = cleaned_query.lower().split()
    group_by_columns = []
    
    print(f"Group func nlp doc : {cleaned_doc}")
    print(f"known tables: {detected_tables}")
    print(f"pre loop cols: {columns_for_tables}")

    # Loop through tokens to match columns
    for token in cleaned_doc:
        for table, columns in columns_for_tables.items():
            if token in columns:  # Match token to columns
                column = f"{table}.{token}"  # Fully qualified column name
                print(f"Matched column: {column}")
                if column not in group_by_columns:  # Avoid duplicates
                    group_by_columns.append(column)

        print(f"Group func for known cols: {group_by_columns}")

    # Step 6: Resort to the next noun if no columns are found
    if not group_by_columns:
        for token in cleaned_doc:
            if token.pos_ == "NOUN":
                group_by_columns.append(token.text)
                print(f"DEBUG: Falling back to noun: {token.text}")
                break

    # Step 7: Return the GROUP BY clause
    if group_by_columns:
        group_by_clause = f"GROUP BY {', '.join(group_by_columns)}"

        return group_by_clause

    return ""

# ORDER BY clause generation function
def generate_order_by_clause(query, columns_for_tables, detected_tables, select_clause_parts):
    doc = nlp(query)
    order_by_columns = []
    order_direction = ""  # ASC or DESC based on "ascending" or "descending"
    
    # Trigger ORDER BY clause on detecting relevant keywords
    if any(token.lemma_ in ping_order for token in doc):
        for idx, token in enumerate(doc):
            # Set direction if "ascending" or "descending" is found
            if token.text == "ascending":
                order_direction = "ASC"
            elif token.text == "descending":
                order_direction = "DESC"
            elif token.lemma_ in ping_order:
                # Situation 1: Look for the next valid column for ORDER BY across all detected tables
                # Skip stop words and proceed to the next non-stop word
                next_token = None
                for j in range(idx + 1, len(doc)):
                    if not doc[j].is_stop:  # Check if the token is not a stop word
                        next_token = doc[j]
                        break

                column_added = False
                if next_token:
                    for table in detected_tables:
                        if next_token.text in columns_for_tables.get(table, []):
                            order_by_columns.append(f"{table}.{next_token.text}")
                            column_added = True
                            break
                
                # Situation 2: Check for "and" followed by a second column name, skipping stop words
                if column_added and idx + 2 < len(doc) and doc[idx + 1].text == "and":
                    second_token = None
                    for k in range(idx + 2, len(doc)):
                        if not doc[k].is_stop:
                            second_token = doc[k]
                            break
                    for table in detected_tables:
                        if second_token and second_token.text in columns_for_tables.get(table, []):
                            order_by_columns.append(f"{table}.{second_token.text}")
                            break
                
                # Situation 3: No explicit ORDER BY column, default to SELECT column if only one
                elif not column_added and select_clause_parts and len(select_clause_parts) == 1:
                    select_column = select_clause_parts[0].split("(")[-1].rstrip(")")
                    order_by_columns.append(select_column)

    # Construct ORDER BY clause
    if order_by_columns:
        return f"ORDER BY {', '.join(order_by_columns)} {order_direction}".strip()
    else:
        return ""
    
def generate_having_clause(query, columns_for_tables, detected_tables, group_by_clause):
    """
    Generate the HAVING clause for an SQL query.

    Logic:
    - Only proceed if `group_by_clause` is not blank.
    - Detect `HAVING` keyword in the query.
    - Process tokens until the next exit trigger or end of query.
    - Identify column names, conditions, and values.
    """
    if not group_by_clause:
        return ""

    doc = nlp(query)
    having_conditions = []
    
    # Check for HAVING keyword
    having_triggered = any(token.text.lower() in ping_having for token in doc)
    if not having_triggered:
        return ""

    # Exit triggers to stop processing
    exit_triggers = {"order", "by", "group", "where", 'combine', 'unite', 'aggregate', 'blend', 'mix', 'fuse', 'coalesce', 'meld', 
             'merge', 'unify', 'connect', 'join', 'union', 'conjoin', 'link', 'choose', 'take', 'select', 'pick', "where", "if", "when",
             "group", "by", "for", 'arrange', 'order', 'organize', 'sort', 'descending', 'ascending', "sum", "average", "min", "max", "count", "total"}

    column_name = None
    operator = None
    value = None

    # Process tokens after the HAVING keyword
    for idx, token in enumerate(doc):
        if token.text.lower() in ping_having:  # Detect HAVING keyword
            for next_token in doc[idx + 1:]:
         
                # Step 1: Find the next known column name
                if column_name is None:
                    for table in detected_tables:
                        if next_token.text.lower() in map(str.lower, columns_for_tables.get(table, [])):
                            column_name = f"{table}.{next_token.text}"
                            break

                # Step 2: Find a condition keyword
                if column_name and operator is None:
                    if next_token.text.lower() in ping_conditions:
                        operator = "=" if next_token.text.lower() == "equal" else (
                            ">" if next_token.text.lower() in {"more", "greater"} else (
                                "<" if next_token.text.lower() == "less" else None
                            )
                        )
                        continue

                # Step 3: Find the next noun or number as the value
                if column_name and operator and value is None:
                    if next_token.like_num:
                        value = next_token.text
                        break
                    elif next_token.pos_ in {"NOUN", "PROPN"}:
                        value = f"'{next_token.text}'"
                        break

            # Build the condition
            if column_name and operator and value:
                having_conditions.append(f"sum({column_name}) {operator} {value}")
            break  # Process only one HAVING clause

    # Return the HAVING clause
    return f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""


# Main function to generate SQL query
def generate_sql_query(query, api_data):
    # Detect tables and columns in the query
    detected_tables, columns_for_tables = find_table_and_column_names(query, api_data)
    join_column = None  # Define a default or optional join column based on context
    
    # Generate each clause
    select_clause, select_clause_parts = generate_select_clause(query, api_data, columns_for_tables, detected_tables)
    from_clause = generate_from_and_joins(query, api_data, ping_join)  # Pass the query and columns_for_tables
    where_clause = generate_where_clause(query, columns_for_tables, detected_tables)
    group_by_clause = generate_group_by_clause(query, columns_for_tables, detected_tables, ping_agg)
    having_clause = generate_having_clause(query, columns_for_tables, detected_tables, group_by_clause)
    order_by_clause = generate_order_by_clause(query, columns_for_tables, detected_tables, select_clause_parts)
    
    # Handle case where ping_join is in the query
    if isinstance(ping_join, (set, list)):
        if any(join_word in query for join_word in ping_join):  # Check for any join word in query
            # Extract the first item from the SELECT clause
            first_select_item = select_clause.replace("SELECT", "").split(",")[0].strip()  # Clean and get the first item
            select_clause = f"SELECT {first_select_item}"  # Rebuild the SELECT clause with only the first item
    elif isinstance(ping_join, str):
        if ping_join in query:
            # Extract the first item from the SELECT clause
            first_select_item = select_clause.replace("SELECT", "").split(",")[0].strip()  # Clean and get the first item
            select_clause = f"SELECT {first_select_item}"  # Rebuild the SELECT clause with only the first item
    
    # Check if there is a HAVING clause
    if having_clause and group_by_clause:
        # Extract the GROUP BY columns
        group_by_columns = group_by_clause.replace("GROUP BY", "").strip()
        
        # Add the GROUP BY columns to the SELECT clause if not already present
        for column in group_by_columns.split(","):
            column = column.strip()
            if column not in select_clause:
                select_clause = select_clause.replace(";", "")  # Remove any trailing semicolon
                select_clause += f", {column}"
    
    # Combine all clauses into the final SQL query
    sql_query = f"{select_clause} {from_clause} {where_clause}{group_by_clause} {having_clause}{order_by_clause};"

    return sql_query.strip()



# Example usage "credit_score", "previous_loan_defaults_on_file", "loan_status"
# my_query = "sort on person_age and sum loan_amnt group by  person_age from loan"
# my_api =  {'tables': ['salaries', 'loan', 'purchases'], 'salaries': ['work_year', 'person_gender', 'experience_level', 'employment_type', 'job_title', 'salary', 'income_class', 'salary_currency', 'salary_in_usd', 'employee_residence', 'remote_ratio', 'company_location', 'company_size'], 
#            'loan': ['person_age', 'person_gender', 'person_education', 'person_income', 'income_class', 'person_emp_exp', 'person_home_ownership', 'loan_amnt', 'loan_intent', 'loan_int_rate', 'loan_percent_income', 'cb_person_cred_hist_length', 'credit_score', 'previous_loan_defaults_on_file', 'loan_status'], 
#            'purchases': ['person_age', 'person_gender', 'person_income', 'income_class', 'number_of_purchases', 'product_category', 'time_on_site', 'loyalty_program', 'discounts', 'purchase_status']}

# sql_query = generate_sql_query(my_query, my_api)
# print("Generated SQL Query:", sql_query)

