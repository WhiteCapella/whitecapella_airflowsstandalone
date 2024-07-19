from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
        'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import_db DAG',
    #schedule_interval=timedelta(days=1),
    schedule="15 4 * * *",
    start_date=datetime(2024, 7, 12),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop'],
) as dag:
    task_start = EmptyOperator (task_id='start')
    task_end = EmptyOperator (task_id='end', trigger_rule = "dummy")
    # task_example = BashOperator (task_id='', bash_command=""" """)
    task_check = BashOperator (
            task_id='check', 
            bash_command="""
            
            if [ -f "$HOME/data/signal/{{ds_nodash}}/_DONE" ]; then
                figlet "Let's move on"
                exit 0
            else
                echo "I'll be back => ~/data/signal/{{ds_nodash}}"
                exit 1
            fi
            """,
            )
    #check exist file "_DONE" in dir.
    task_tocsv = BashOperator (
            task_id='to_csv', 
            bash_command="""
            echo "to.csv"
            U_PATH=$HOME/data/count/{{ds_nodash}}/count.log
            CSV_PATH=$HOME/data/csv/{{ds_nodash}}
            
            mkdir -p $CSV_PATH
            
            cat $U_PATH | awk '{print"{{ds}}, "$2", "$1}' | sort -n -t,  -k3 > $CSV_PATH/csv.csv

            """)
    task_totmp = BashOperator (
            task_id='to_tmp', 
            bash_command=""" 

            """)
    task_tobase = BashOperator (
            task_id='to_base', 
            bash_command=""" 

            """)
    task_mkdone = BashOperator (
            task_id='make_done', 
            bash_command=""" 

            """)
    task_errreport = BashOperator (
            task_id='err_report',
            bash_command="""
            """,
            trigger_rule = "one_failed"
            )
    
    task_start >> task_check
    task_check >> task_tocsv
    task_tocsv >> task_totmp
    task_totmp >> task_tobase
    task_tobase >> task_mkdone
    task_mkdone >> task_end
    task_check >> task_errreport >> task_end
