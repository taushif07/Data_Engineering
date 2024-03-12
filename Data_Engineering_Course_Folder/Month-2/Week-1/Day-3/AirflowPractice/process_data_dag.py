from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


def choose_path():
    condition = True
    if condition:
        return "process_path_a"
    else:
        return "process_path_b"


def process_data():
    return "Data processed!"


dag2 = DAG(
    "process_data_dag",
    description="DAG with branching logic",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

start = DummyOperator(task_id="start", dag=dag2)
choose_branch = BranchPythonOperator(
    task_id="choose_branch", python_callable=choose_path, dag=dag2
)
process_path_a = PythonOperator(
    task_id="process_path_a", python_callable=process_data, dag=dag2
)
process_path_b = PythonOperator(
    task_id="process_path_b", python_callable=process_data, dag=dag2
)
end = DummyOperator(task_id="end", dag=dag2)

start >> choose_branch
choose_branch >> process_path_a >> end
choose_branch >> process_path_b >> end
