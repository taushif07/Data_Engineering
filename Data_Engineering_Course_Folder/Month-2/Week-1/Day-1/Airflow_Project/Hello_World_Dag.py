from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def helloWorld():
    print("Hello World")


def helloAirflow():
    print("Hello Airflow")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 5, 5),
    schedule="@hourly",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="hello_world", python_callable=helloWorld)
    task2 = PythonOperator(task_id="hello_airflow", python_callable=helloAirflow)

task1 >> task2
