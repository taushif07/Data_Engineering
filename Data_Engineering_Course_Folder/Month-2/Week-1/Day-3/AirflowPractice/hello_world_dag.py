from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def print_hello():
    return "Hello World!"


dag = DAG(
    "hello_world",
    description="Hello World DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
)

dummy_operator = DummyOperator(task_id="dummy_task", retries=3, dag=dag)

hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

dummy_operator >> hello_operator
