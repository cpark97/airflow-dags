from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

one_day_ago = datetime.today() - timedelta(days=1)

def say_hello(date):
    print(f"Hello Airflow! : {date}")

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

    prev_task = start
    end_date = datetime.strptime('2026-03-07', '%Y-%m-%d')
    date = end_date - timedelta(days=7)

    while date <= end_date:
        date_str = date.strftime('%Y-%m-%d')
        hello_task = PythonOperator(
            task_id=f'hello_task_{date_str}',
            python_callable=say_hello,
            op_kwargs={"date": date_str}
        )
        prev_task >> hello_task
        prev_task = hello_task
        date += timedelta(days=1)

    end = EmptyOperator(task_id='end')

prev_task >> end
