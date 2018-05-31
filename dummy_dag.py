from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

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
    'retries': 1,
    'priority_weight': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=1)
}

with DAG('dummy-dag-single-op', default_args=default_args, **dag_args) as dag:
    t1 = DummyOperator(
        task_id='single-task-dummy-1',
        trigger_rule='one_success',
        dag=dag
    )
    t2 = DummyOperator(
        task_id='single-task-dummy-2',
        trigger_rule='one_success',
        dag=dag
    )
    t1 >> t2
