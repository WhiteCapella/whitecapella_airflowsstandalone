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

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 12, 31),
    catchup=True,
    tags=['movie'],
) as dag:
    # Define Functions
    def repart():
        from movie_spark_module.data import re_partition
        re_partition(ds_nodash)

    # Tasks
    start = EmptyOperator(task_id='start')
    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=repart,
            requirements=["git+https://github.com/WhiteCapella/movie_spark_module.git"],
    )
    join_df = BashOperator(
            task_id='join.df',
            bash_command="""
            $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/movie_join_df.py
            """
    )
    agg = BashOperator(
            task_id='agg',
            bash_command="""
            $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/movie_agg.py
            """,
            )
    end = EmptyOperator(task_id='end')

    start >> re_partition >> join_df >> agg >> end

