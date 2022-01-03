from airflow import DAG
from airflow.operators.python import PythonOperator,PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from pprint import pprint
from datetime import datetime
import time
import json
from datetime import timedelta
import os

# These args will get passed on to the python operator
default_args = {
    'owner': 'dnyaneshbandbe',
    'depends_on_past': False,
    'email': ['dnyanesh.bandbe88@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    dag_id = 'python_file_call_ex1',
    default_args = default_args,
    schedule_interval=None,
    start_date = datetime(2021,11,25),
    catchup = False,
    tags = ['python_example'],
) as dags:


    def print_func():
        print(os.path.abspath(__file__))

    print_task = PythonOperator(
        task_id = 'print_func',
        python_callable = print_func,
    )


    filepath_task = BashOperator(
        task_id = 'print_file_path',
        bash_command = 'python3 /c/Users/dnyan/airflow/dags/print_func.py',
    )


    print_task >> filepath_task
