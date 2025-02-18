import os
os.environ['NO_PROXY'] = '*'  # Ensure that no proxy interferes with HTTP requests

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'omdb_movie_data_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as needed based on API call rate limits
    catchup=False,
)

def process_movie_data(**kwargs):
    ti = kwargs['ti']
    # Pull the response text pushed by the HTTP task from XCom
    response_text = ti.xcom_pull(task_ids='get_movie_data')
    if response_text:
        movie_data = json.loads(response_text)
        # Remove the "Poster" field if present
        movie_data.pop("Poster", None)
        # Process the cleaned data as needed (e.g., store it in a DB or file)
        print("Cleaned movie data:", movie_data)
    else:
        raise ValueError("No data received from OMDb API.")

get_movie_data = HttpOperator(
    task_id='get_movie_data',
    http_conn_id='omdb_api',  # Set up this connection in the Airflow UI (host: "http://www.omdbapi.com/")
    endpoint='?apikey=eb83abbc&t=Blade+Runner+2049&plot=full',
    method='GET',
    headers={"Content-Type": "application/json"},
    # Optionally, you can add a response check to ensure you got a 200 OK status
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_movie_data',
    python_callable=process_movie_data,
    provide_context=True,
    dag=dag,
)

get_movie_data >> process_data