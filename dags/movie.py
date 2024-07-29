from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
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
    
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)
        from mov.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD = kwargs['ds_nodash']
        df = save2df(YYYYMMDD)
        print(df.head(5))
    
    
    get_data = PythonVirtualenvOperator(
        task_id='get_data',
        python_callable=get_data,
        requirements=["git+https://github.com/WhiteCapella/mov.git@0.3"],
        system_site_packages=False,
    )

    save_data = BashOperator(
        task_id='save_data',
        bash_command='date',
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> get_data >> save_data >> end
