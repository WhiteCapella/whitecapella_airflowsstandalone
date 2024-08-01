from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow.utils.trigger_rule import TriggerRule

# The DAG object; we'll need this to instantiate a DAG 
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        ExternalPythonOperator,
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator,
        is_venv_installed,
        )

with DAG( 
        'movie',  
        default_args={  
            'depends_on_past': True,  
            'email_on_failure': False,
            'email_on_retry': False,  
            'retries': 1,
            'retry_delay': timedelta(minutes=5) 
            },
        description='movie',
        schedule="10 2 * * *", 
        start_date=datetime(2024, 7, 24),  
        catchup=True,
        tags=['api', 'movie', 'amt'],
        ) as dag:

# def, functions
    def apply_type(ds_nodash):
        print("apply_type function called by admin at {ds_nodash}")
    def merge_df(ds_nodash):
        print("merge_df function called by admin at {ds_nodash}")
    def de_dup(ds_nodash):
        print("de_dup function called by admin at {ds_nodash}")
    def summary_df(ds_nodash):
        print("summary_df function called by admin at {ds_nodash}")

# tasks
    #start_task
    start = EmptyOperator(task_id='start')

    #data_process_tasks
    t_apply = PythonVirtualenvOperator(
            task_id='t.apply',
            python_callable=apply_type,
            requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
            system_site_packages=False,
            venv_cache_path="/home/whitecapella/tmp/airflow_venv/movie_summary"
            )
    t_merge = PythonVirtualenvOperator(
            task_id='t.merge',
            python_callable=merge_df,
            requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
            system_site_packages=False,
            venv_cache_path="/home/whitecapella/tmp/airflow_venv/movie_summary"
            )
    t_de = PythonVirtualenvOperator(task_id='t.de',
            python_callable=de_dup,
            requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
            system_site_packages=False,
            venv_cache_path="/home/whitecapella/tmp/airflow_venv/movie_summary"
            )
    t_summary = PythonVirtualenvOperator(task_id='t.summary',
            python_callable=summary_df,
            requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
            system_site_packages=False,
            venv_cache_path="/home/whitecapella/tmp/airflow_venv/movie_summary"
            )

    #end_task
    end = EmptyOperator(task_id='end')

# pipeline

    start >> t_apply >> t_merge >> t_de >> t_summary >> end
