 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_data_processing',
    default_args=default_args,
    description='A simple DAG to process e-commerce data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

t1 = BashOperator(
    task_id='load_data_to_hdfs',
    bash_command='hdfs dfs -put /path/to/card_transactions.csv /ccard/',
    dag=dag,
)

t2 = BashOperator(
    task_id='run_mapreduce_job',
    bash_command='hadoop jar /path/to/your_mapreduce_job.jar input output',
    dag=dag,
)

t3 = BashOperator(
    task_id='extract_insights',
    bash_command='python /path/to/extract_insights.py',
    dag=dag,
)

t1 >> t2 >> t3