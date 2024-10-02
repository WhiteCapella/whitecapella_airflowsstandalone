from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator,
)
import pandas as pd

def start_worker():
    from mnist.worker import run  # mnist 패키지에서 run 함수 임포트
    run() 

with DAG(
    'ml_worker',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        'execution_timeout': timedelta(hours=2),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='machine learning worker test',
    schedule="3 * * * *",
    start_date=datetime(2024, 10, 1),
    catchup=True,
    tags=['machine-learning','worker'],
) as dag:
    
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    notify_ping = BashOperator(
        task_id ='line.ping',
        bash_command="""
        curl -X POST -H 'Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52' -F 'message=pong' https://notify-api.line.me/api/notify
        """,
        )

    worker = PythonVirtualenvOperator(
        task_id='worker',
        python_callable=start_worker,
        requirements=['git+https://github.com/WhiteCapella/mnist.git@1.0/CNN'],
        system_site_packages=False,
    )   
start >> worker >> notify_ping >> end
