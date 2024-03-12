from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return "Hello from the first DAG!"


dag1 = DAG(
    "hello_world_dag",
    description="Simple hello world DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

start_task = DummyOperator(task_id="start_task", dag=dag1)
hello_task = PythonOperator(
    task_id="print_hello", python_callable=print_hello, dag=dag1
)
end_task = DummyOperator(task_id="end_task", dag=dag1)

start_task >> hello_task >> end_task
