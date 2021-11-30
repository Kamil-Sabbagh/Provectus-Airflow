from datetime import datetime, timedelta
from textwrap import dedent
from classes import MapOperator
from classes import ReduceOperator
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import csv
import json
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
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
    'map-reduce',
    default_args=default_args,
    description='A DAG to do map-reduce operation',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    data = ''
    file = open("tweets.csv", 'r')
    csvreader = csv.reader(file)

    for rows in csvreader:
        for col in rows:
            data += str(col) + " "


    data = data.split()
    size = len(data)//3

    map1 = MapOperator(task_id='map1', name='map1', data=data[:size])
    map2 = MapOperator(task_id='map2', name='map2', data=data[size:(size*2)])
    map3 = MapOperator(task_id='map3', name='map3', data=data[(size*2):])
    reduce = ReduceOperator(task_id='reduce', name="reduce", data="")


    [map1, map2, map3] >> reduce
