import random

# Predefined example queries for SQL and NoSQL
# sample_sql_queries = [
#     "choose loan_amnt from loan where person_income is greater than 20000",
#     "sum loan amount from debt by city",
#     "select amount from debt by city having income equal to 10000",
#     "select debt_id from debt and sort by income",
#     "select zip from income where city equal irvine",
#     "take salary from salaries where person_gender equal female",
#     "pick Age and person_income from purchases where person_income sort by person_income descending"
# ]

# sample_nosql_queries = [
#     '{"field": "value"}',  
#     '[{"$group": {"_id": "$category", "count": {"$sum": 1}}}]',  
#     '{"age": {"$gt": 30}}',  
#     '[{"$match": {"status": "active"}}, {"$group": {"_id": "$role", "count": {"$sum": 1}}}]',  
#     '{"location.city": "New York"}'  
# ]

sample_queries = [
    # Where
    "take salary from salaries where person_gender equal female",
    "choose loan_amnt from loan where person_income is greater than 20000",
    "choose job_title and salary from salaries where work_year is 2021",
    "pick age and person_income from purchases where income_class is middle sort by person_income descending",
    # Sum/Group by
    "sum number_of_purchases from purchases by product_category", 
    "sum loan_amnt  from loan by person_home_ownership",
    

    # Join

    


    
]

'''
These ones don't work
"combine salaries and purchases to find the sum of salary_in_usd grouped by company_location where ProductCategory is 2"
'''

# Function to get a random sample query
def get_sample_query():
    return random.choice(sample_queries)
    # if query_type == "SQL":
    #     return random.choice(sample_sql_queries)
    # elif query_type == "NoSQL":
    #     return random.choice(sample_nosql_queries)
