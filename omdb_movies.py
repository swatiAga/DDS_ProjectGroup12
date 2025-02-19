import os
os.environ['NO_PROXY'] = '*'
import json
import gzip
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define directory to save movies
OUTPUT_DIR = '/Users/andreamellany/data'  # Replace with your actual path
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'omdb_bulk_movie_data_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def process_and_save_movies(**kwargs):
    """Process and save all movie data into a single compressed JSON file."""
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='fetch_movies')

    if not response_data:
        raise ValueError("No data received from OMDb API.")

    all_movies = response_data.get('Search', [])

    # Define output file path
    output_file_path = os.path.join(OUTPUT_DIR, 'movies.json.gz')

    # Save all movies to a single compressed JSON file
    with gzip.open(output_file_path, 'wt', encoding='utf-8') as f:
        json.dump(all_movies, f, indent=4)

    print(f"Saved {len(all_movies)} movies to {output_file_path}")

fetch_movies = HttpOperator(
    task_id='fetch_movies',
    http_conn_id='omdb_api',
    endpoint='',
    method='GET',
    data={
        's': 'movie', 
        'type': 'movie', 
        'page': 1, 
        'apikey': 'eb83abbc'  # Ensure the API key is included
    },
    headers={"Content-Type": "application/json"},
    log_response=True,
    response_filter=lambda response: response.json(),  # Extracts JSON data and stores in XCom
    dag=dag,
)

process_and_save_task = PythonOperator(
    task_id='process_and_save_movies',
    python_callable=process_and_save_movies,
    provide_context=True,
    dag=dag,
)

fetch_movies >> process_and_save_task