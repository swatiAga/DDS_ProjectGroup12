from pymongo import MongoClient
import pymongo
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
import os
import json

os.environ['NO_PROXY'] = '*'

default_args = {
    'start_date': datetime(2025, 2, 10),
}

# Function to insert data into MongoDB
def insert_into_mongodb(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='http_test')  # Retrieve JSON response from XCom

    if not response:
        raise ValueError("No data retrieved from API")

    # MongoDB connection settings
    mongo_uri = "mongodb://localhost:27017/"  # Adjust if needed
    db_name = "msds-697-section2"  # Database name
    collection_name = "movies_import"  # Collection name

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert data into MongoDB
    if isinstance(response, str):  # Ensure JSON is properly loaded
        response = json.loads(response)

    collection.insert_many(response)  # Insert the JSON list into MongoDB

    print(f"Inserted {len(response)} records into MongoDB collection '{collection_name}'.")

with DAG(
    dag_id='movie_recommendation_pipeline',
    default_args=default_args,
    schedule_interval='@once'
    ) as dag:
    task_test = HttpOperator(
        task_id='http_test',
        http_conn_id='tmdb_api', # references connection id that was set up in airflow
        method='GET', # the request is of GET type
        endpoint='/3/discover/movie?include_adult=false&include_video=false&language=en-US&page=1&sort_by=popularity.desc',
        headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI3OGI2NTkwMjdkOTUxZWU0OGUwY2RkMzZjMjY1NzlmMSIsIm5iZiI6MTczODk1ODM5NS42MzkwMDAyLCJzdWIiOiI2N2E2NjYzYjRkNTM2Y2I5MzI2NmY0OWIiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.Pmx7Pa4ohTS6-3XvL1ZcE42Nqi5OmumNLrNG8r3S1WQ"},
        response_check=lambda response: response.status_code == 200 and 'results' in response.json(),
        response_filter=lambda response: json.dumps(response.json().get("results", [])[:10]),
        log_response=True
    )
    # Define MongoDB insertion task
    insert_mongodb_task = PythonOperator(
        task_id='insert_into_mongodb',
        python_callable=insert_into_mongodb,
        provide_context=True
    )
    
    # Define task dependencies
    task_test >> insert_mongodb_task