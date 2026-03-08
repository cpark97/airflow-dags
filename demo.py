from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

one_day_ago = datetime.today() - timedelta(days=1)

def say_hello():
    print("Hello Airflow!")

with DAG(
    dag_id='demo',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='demo',
    start_date=one_day_ago,
    schedule=None,
) as dag:
    start = EmptyOperator(task_id='start')

    # PythonOperator 태스크
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=say_hello,
    )

    end = EmptyOperator(task_id='end')

start >> hello_task >> end
