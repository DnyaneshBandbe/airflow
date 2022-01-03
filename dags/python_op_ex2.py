from airflow import DAG
from airflow.operators.python import PythonOperator,PythonVirtualenvOperator

from pprint import pprint
from datetime import datetime
import time
import json

with DAG(
    dag_id = 'python_op_ex2',
    schedule_interval=None,
    start_date = datetime(2021,11,25),
    catchup = False,
    tags = ['python_example'],
) as dags:

    def push_function(**kwargs):
        ls = ['a', 'b', 'c']
        return ls

    def pull_function(**kwargs):
        ti = kwargs['ti']
        ls = ti.xcom_pull(task_ids='push_task')
        print(ls)

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True,
        )

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function,
        provide_context=True,
        )        

    push_task >> pull_task
