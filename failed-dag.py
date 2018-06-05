from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import time

dag_args = {
    'concurrency': 1,
    'max_active_runs': 10,
    'schedule_interval': '@daily',
    'catchup': False
}


default_args = {
    'owner': 'tfeng',
    'start_date': datetime(2018, 5, 30),
    'depends_on_past': False,
    'email': ['tfeng@lyft.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'priority_weight': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(seconds=1)
}

def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)

with DAG('failed-dag', default_args=default_args, **dag_args) as dag:
    for i in range(2):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i) * 60},
            dag=dag)
