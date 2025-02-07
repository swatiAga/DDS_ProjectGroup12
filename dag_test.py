from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

with DAG(dag_id="dag_test", 
         start_date=datetime(2025, 1, 23), 
         end_date=datetime(2025, 12, 31),
         schedule="@once") as dag:
         
     task3 = BashOperator(task_id="task3",
                          bash_command="echo 'Hello Mahesh!!! Youre the coolest'")
     
     task3