from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor
from airflow import DAG
from datetime import datetime, timedelta

NUM_SUBDAGS = 30
NUM_OPS_PER_SUBDAG = 10

dag_args = {
    'concurrency': 3,
    'max_active_runs': 1,
    'schedule_interval': '@daily',
    'params': {'s3': 'aaaa'}
}


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 23),
    'depends_on_past': False,
    'email': ['tfeng@lyft.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def logging_func(id):
  log.info("Now running id: {}".format(id))


def build_dag(dag_id, num_ops):
  dag = DAG(dag_id, default_args=default_args, **dag_args)
  #start_op = DummyOperator(task_id='start', dag=dag)

  for i in range(num_ops):
    op = PythonOperator(
      task_id=str(i),
      python_callable=logging_func,
      op_args=[i],
      dag=dag,
    )

    #start_op >> op

  return dag

parent_id = 'consistent_failure1'
with DAG(
  parent_id,
  default_args=default_args,
  **dag_args
) as dag:


  start_op = DummyOperator(task_id='test-stat')

  for i in range(NUM_SUBDAGS):
    task_id = "subdag_{}".format(i)
    op = SubDagOperator(
      task_id=task_id,
      subdag=build_dag("{}.{}".format(parent_id, task_id), NUM_OPS_PER_SUBDAG),
      executor=LocalExecutor()
    )

    #start_op >> op
