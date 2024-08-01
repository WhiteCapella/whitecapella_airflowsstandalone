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
    
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash, path="~/tmp/test_parquet")
        print("=" * 33)
        print(df.head(10))
        print("=" * 33)
        print(df.dtypes)
        print("=" * 33)
        
        print("개봉일 기준, 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt':'sum'}).reset_index()
        print(sum_df)


    
    def branch_function(ds_nodash):
        import os
        if os.path.exists(os.path.expanduser(f"~/tmp/test_parquet/load_dt={ds_nodash}")):
            return 'rm.dir'
        else:
            return 'get.data'
    
    # branch_line
    branch_op = BranchPythonOperator(
        task_id = "branch.op",
        python_callable=branch_function,
    )
    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    )
    get_start = EmptyOperator( task_id='get.start', trigger_rule='one_success')
    
    #data_process_line
    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
        system_site_packages=False,
        venv_cache_path="/home/whitecapella/tmp/airflow_venv/get_data"
    )
    nation_k = EmptyOperator(task_id='nation.k')
    nation_f = EmptyOperator(task_id='nation.f')
    multi_n = EmptyOperator(task_id='multi.n')
    multi_y = EmptyOperator(task_id='multi.y')
    get_end = EmptyOperator(task_id='get.end')

    #data_save_process_line
    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        requirements=["git+https://github.com/WhiteCapella/mov.git@fix/0.3.1"],
        system_site_packages=False,
        trigger_rule='one_success',
        venv_cache_path="/home/whitecapella/tmp/airflow_venv/get_data"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> branch_op
    branch_op >> [rm_dir, get_start]
    rm_dir >> get_start
    get_start >> [nation_k, nation_f, get_data, multi_n, multi_y] >> get_end
    get_end >> save_data >> end

