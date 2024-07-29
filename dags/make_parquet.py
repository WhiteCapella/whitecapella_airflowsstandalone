from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def gen_emp(tid,trg='all_success'):
    op = EmptyOperator(task_id=tid, trigger_rule="all_done")
    return op
def to_parquet(ds_nodash, **kwargs):
    log_dir = f"/home/whitecapella/data/count/{ds_nodash}"
    log_file = f"{log_dir}/count.log"
    parquet_file = f"/home/whitecapella/data/parquet/{ds_nodash}/count.parquet"
    
    with open(log_file, 'r') as f:
        lines = f.readlines()

        data = []
        for line in lines:
            line = line.strip()
            cnt, command = line.split(' ',1)
            try:
                cnt = int(cnt)
            except ValueError:
                cnt = 0
            data.append({'cnt':int(cnt), 'command': command, 'dt': pd.to_datetime(ds_nodash, format="%Y%m%d")})
            
            df = pd.DataFramd(data)
            table = pa.table(df)
            pq.write_table(table, parquet_file, encoding='utf-8')

with DAG(
        'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='history log 2 mysql db',
    schedule="20 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop', "db", "history", "parquet"],
) as dag:



    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            DONE_FILE=/home/whitecapella/data/import_signal/{{ds_nodash}}/_DONE
            if [ -f "$DONE_FILE" ]; then
                mkdir /home/whitecapella/data/parquet/{{ds_nodash}}
                exit 0
            else
                exit 1
            fi
            """
    )


    task_to_parquet = PythonOperator(
        task_id="to_parquet",  
        python_callable=to_parquet,
        provide_context=True
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            echo "make.done"
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_end = gen_emp('end',trg='all_done')
    task_start = gen_emp('start')

    task_start >> task_check
    task_check >> task_to_parquet >> task_done >> task_end
    task_check >> task_err >> task_end
