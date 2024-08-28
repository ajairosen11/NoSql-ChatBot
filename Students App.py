import os
from dotenv import load_dotenv
import streamlit as st
import google.generativeai as genai
from pymongo import MongoClient
import json
from pymongo.cursor import Cursor
from bson import json_util

# Load environment variables
load_dotenv()

# Configure Gemini API
genai.configure(api_key=os.getenv("gemini_api_key"))

def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text

# Define the prompt for Gemini API
prompt = [
    """
    You are an expert in converting English Questions to Pymongo Query!
    The MongoDB Collection has the name Statistics and has the following Keys - Name, Age, Department, GPA, Salary.

    Here are some examples:
    
    1. Who has the highest GPA?
    collection.find_one(sort=[('GPA', -1)], projection={'_id': 0, 'Name': 1})
    
    2. What is the average salary of the Mechanical department?
    collection.aggregate([{'$match': {'Department': 'Mech'}}, {'$group': {'_id': None, 'avgSalary': {'$avg': '$Salary'}}}])
    
    3. How many employees are there in the Statistics department?
    collection.count_documents({'Department': 'Statistics'})
    
    4. List the names of all employees aged 25 and above.
    collection.find({'Age': {'$gte': 25}}, {'_id': 0, 'Name': 1})
    
    5. What is the total salary of all employees combined?
    collection.aggregate([{'$group': {'_id': None, 'totalSalary': {'$sum': '$Salary'}}}])
    
    6. Who has the lowest GPA in the Mechanical department?
    collection.find_one({'Department': 'Mech'}, sort=[('GPA', 1)], projection={'_id': 0, 'Name': 1})
    
    7. List the names and salaries of employees sorted by GPA.
    collection.find({}, {'_id': 0, 'Name': 1, 'Salary': 1}).sort('GPA', -1)
    
    8. What is the average GPA of the employees in the English department?
    collection.aggregate([{'$match': {'Department': 'English'}}, {'$group': {'_id': None, 'avgGPA': {'$avg': '$GPA'}}}])
    
    9. Find the employee with the highest salary.
    collection.find_one(sort=[('Salary', -1)], projection={'_id': 0, 'Name': 1})
    
    10. List the names of employees with a GPA greater than 7.5.
    collection.find({'GPA': {'$gt': 7.5}}, {'_id': 0, 'Name': 1})
    
    11. Count how many employees are there in total.
    collection.count_documents({})
    
    12. What is the total salary of employees aged below 25?
    collection.aggregate([{'$match': {'Age': {'$lt': 25}}}, {'$group': {'_id': None, 'totalSalary': {'$sum': '$Salary'}}}])
    
    13. Find the average GPA of employees aged 23.
    collection.aggregate([{'$match': {'Age': 23}}, {'$group': {'_id': None, 'avgGPA': {'$avg': '$GPA'}}}])
    
    14. Which department has the highest average salary?
    collection.aggregate([{'$group': {'_id': '$Department', 'avgSalary': {'$avg': '$Salary'}}}, {'$sort': {'avgSalary': -1}}, {'$limit': 1}])
    
    15. What is the total number of employees in each department?
    collection.aggregate([{'$group': {'_id': '$Department', 'count': {'$sum': 1}}}])
    
    16. List all employees whose names start with 'A'.
    collection.find({'Name': {'$regex': '^A'}}, {'_id': 0, 'Name': 1})
    
    17. Find the sum of salaries of employees with a GPA less than 6.0.
    collection.aggregate([{'$match': {'GPA': {'$lt': 6.0}}}, {'$group': {'_id': None, 'totalSalary': {'$sum': '$Salary'}}}])
    
    18. What is the maximum GPA in the English department?
    collection.aggregate([{'$match': {'Department': 'English'}}, {'$group': {'_id': None, 'maxGPA': {'$max': '$GPA'}}}])
    
    19. List the names of employees who are not in the Statistics department.
    collection.find({'Department': {'$ne': 'Statistics'}}, {'_id': 0, 'Name': 1})
    
    20. What is the minimum age of employees in the Mechanical department?
    collection.aggregate([{'$match': {'Department': 'Mech'}}, {'$group': {'_id': None, 'minAge': {'$min': '$Age'}}}])
    
    The MongoDB query code should not have ``` in the output.

    The MongoDB query code should not have any SQL syntax in the output.
    """
    
]

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['College']
collection = db['Statistics']

# Streamlit app
st.title("MongoDB Query Generator and Executor")

# Input field for the question
question = st.text_input("Enter your question:", "")

if st.button("Generate and Execute Query"):
    if question:
        # Generate the MongoDB query from the question
        query_code = get_gemini_response(question=question, prompt=prompt)
        st.write("Generated MongoDB Query:")
        st.code(query_code, language='python')
        
        try:
            # Validate the generated query
            if not query_code.startswith("collection"):
                raise ValueError("Invalid query: Query should start with 'collection'.")
            
            # Execute the query
            results = eval(query_code, {'collection': collection, 'doc': collection.find_one})
            
            # Check if the result is a Cursor (which can occur with aggregate or find queries)
            if isinstance(results, Cursor):
                results = list(results)
            
            # Convert the results to JSON format using bson's json_util
            json_results = json_util.dumps(results, indent=4)
            
            # Display the JSON results in Streamlit
            st.write("Query Results (JSON):")
            st.json(json_results)
        except SyntaxError as e:
            st.error(f"Syntax error in the generated query: {e}")
        except ValueError as e:
            st.error(f"Validation error: {e}")
        except Exception as e:
            st.error(f"Error executing query: {e}")
    else:
        st.write("Please enter a question.")