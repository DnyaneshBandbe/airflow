from airflow import DAG
from airflow.operators.python import PythonOperator,PythonVirtualenvOperator

from pprint import pprint
from datetime import datetime
import time
import json

with DAG(
    dag_id = 'python_op_ex1',
    schedule_interval=None,
    start_date = datetime(2021,11,25),
    catchup = False,
    tags = ['python_example'],
) as dags:

    def print_context(ds,**kwargs):
        pprint(kwargs)
        print("Next Print context for this task :: ")
        pprint(ds)

    print_task = PythonOperator(
        task_id = 'print_the_context',
        python_callable = print_context,
    )

    def extract():
        data_string={"1001": 301.27, "1002": 433.21, "1003": 502.22}
        print('data_string: ',data_string)
        return data_string

    def transform_data(**kwargs):
        ti = kwargs['ti']
        order_data = ti.xcom_pull(task_ids='extract_data')
        pprint(type(order_data))
        pprint(order_data)
        total_order_value = 0
        for key,value in order_data.items():
            total_order_value += value
        return {'total order value: ', total_order_value}

    extract_task = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract,
    )

    transform_task = PythonOperator(
        task_id = 'transform_data',
        op_kwargs = {'order_data':"{ti.xcom_pull('extract_data')}}"},
        python_callable = transform_data
    )

    print_task >> extract_task >> transform_task
