from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    "spark_jobs_dag",
    default_args=default_args,
    description="A simple code to run simple spark jobs",
    schedule_interval=None,
)

convert_csv_to_parquet = BashOperator(
    task_id="convert_csv_to_parquet",
    bash_command="spark-submit ./SPARK_JOBS/CSV_TO_PARQUET.py ",
    dag=dag,
)

add_columns_to_parquet = BashOperator(
    task_id="add_columns_to_parquet",
    bash_command="spark-submit ./SPARK_JOBS/ADD_COLUMN_TO_PARQUET.py ",
    dag=dag,
)

convert_csv_to_parquet >> add_columns_to_parquet
