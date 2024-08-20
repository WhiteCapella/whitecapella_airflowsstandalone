from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator, 
        PythonVirtualenvOperator, 
        BranchPythonOperator
        )

req = "git+https://github.com/WhiteCapella/mvstar.git"

with DAG(
    'movies-dynamic-json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule_interval='@once',
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015,1,2),
    catchup=True,
    tags=["movies","dynamic","json"],
) as dag:
    # API 호출 후 10 페이지 저장
    def mkdyna(execution_date):
        from mvstar.dynamic import mkdynamic
        # year 추출
        dt = int(execution_date.year)
        mkdynamic(dt = dt, sleep_time = 0.1)

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    get_data = PythonVirtualenvOperator(
            task_id = "get.data",
            python_callable=mkdyna,
            requirements=req,
            )

    pars_parq = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
            spark-submit $AIRFLOW_HOME/pyspark_df/movie-dynamic-sp.py {{ execution_date.year }}
            """,
        trigger_rule='all_done'
        )

    select_parq = BashOperator(
            task_id = "select.parquet",
            bash_command="""
            spark-submit $AIRFLOW_HOME/pyspark_df/select-dynamic.py {{ execution_date.year }}
            """,
            trigger_rule='all_done'
    
    )
    start >> get_data >> pars_parq >> select_parq >> end
