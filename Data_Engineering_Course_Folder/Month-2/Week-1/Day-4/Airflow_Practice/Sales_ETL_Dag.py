from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    "Sales_Project_Dag",
    default_args=default_args,
    description="Sales project code",
    schedule_interval=None,
)

convert_txt_to_csv = BashOperator(
    task_id="convert_txt_to_csv",
    bash_command="spark-submit ./PySpark_Jobs/convert_txt_to_csv.py",
    dag=dag,
)

read_csv_and_join_dfs = BashOperator(
    task_id="read_csv_and_join_dfs",
    bash_command="spark-submit ./PySpark_Jobs/read_csv_and_join_df.py",
    dag=dag,
)

Total_Amount_Spent_By_Each_Customer = BashOperator(
    task_id="Total_Amount_Spent_By_Each_Customer",
    bash_command="spark-submit ./PySpark_Jobs/Total_Amount_Spent_By_Customer.py",
    dag=dag,
)

Total_Amount_Spent_On_Each_Food_Category = BashOperator(
    task_id="Total_Amount_Spent_On_Each_Food_Category",
    bash_command="spark-submit ./PySpark_Jobs/Total_Amount_Spent_On_Each_Food_Category.py",
    dag=dag,
)

Total_Amount_Of_Sales_Each_Month = BashOperator(
    task_id="Total_Amount_Of_Sales_Each_Month",
    bash_command="spark-submit ./PySpark_Jobs/Total_Amount_Of_Sales_Each_Month.py",
    dag=dag,
)

Total_Amount_Of_Sales_Yearly = BashOperator(
    task_id="Total_Amount_Of_Sales_Yearly",
    bash_command="spark-submit ./PySpark_Jobs/Total_Amount_Of_Sales_Yearly.py",
    dag=dag,
)

Top_Five_Order = BashOperator(
    task_id="Top_Five_Order",
    bash_command="spark-submit ./PySpark_Jobs/Top_Five_Order.py",
    dag=dag,
)


convert_txt_to_csv >> read_csv_and_join_dfs

read_csv_and_join_dfs >> Total_Amount_Spent_By_Each_Customer

read_csv_and_join_dfs >> Total_Amount_Spent_On_Each_Food_Category

(
    read_csv_and_join_dfs
    >> Total_Amount_Of_Sales_Yearly
    >> Total_Amount_Of_Sales_Each_Month
)

read_csv_and_join_dfs >> Top_Five_Order
