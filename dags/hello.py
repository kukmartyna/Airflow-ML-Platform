from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

def print_hello():
    print("Hello World!")

with DAG(
    dag_id='hello',
    description='Hello world dag.',
    start_date=datetime.now(),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=print_hello
    )

    task2 = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Hello world!"'
    )

    task1.set_downstream(task2)