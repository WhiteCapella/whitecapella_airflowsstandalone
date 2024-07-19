from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    DailyWatcher_start = BashOperator(
            task_id='start',
            bash_command='init and start all of task',
    )
    Report_start = BashOperator(
            task_id='report_start',
            bash_command='start compile data, and make report documents',
    )
    Report_end = BashOperator(
            task_id='report_end',
            bash_command='complete document of data and print',
    )
    Compile_risk = BashOperator(
            task_id='compile_riskdata',
            bash_command='compile all risk data of daily',
    )
    Compile_requestdata1 = BashOperator(
            task_id='compile_requiredata1',
            bash_command='compile daily data of requested by Team A ',
    )
    Compile_requestdata2 = BashOperator(
            task_id='compile_requiredata2',
            bash_command='compile daily data of requested by Team B ',
    )
    Compile_requestdata3 = BashOperator(
            task_id='compile_requiredata3',
            bash_command='compile daily data of requested by Team C ',
    )
    DailyWatcher_risk = BashOperator(
            task_id='watch_risk',
            bash_command='watch data of anormaly system, user, server and collect data',
    )
    DailyWatcher_basicdata1 = BashOperator(
            task_id='watch_basic1',
            bash_command='collect daily data of X',
    )
    DailyWatcher_basicdata2 = BashOperator(
            task_id='watch_basic2',
            bash_command='collect daily data of Y',
    )
    DailyWatcher_basicdata3 = BashOperator(
            task_id='watch_basic3',
            bash_command='collect daily data of Z',
    )
    DailyWatcher_emergencyalert = BashOperator(
            task_id='alert',
            bash_command='catch emergency risk, and alert relevant department',
    )
    DailyWatcher_requestdata1 = BashOperator(
            task_id='watch_request1',
            bash_command='collect daily data requested by Team A',
    )
    DailyWatcher_requestdata2 = BashOperator(
            task_id='watch_request2',
            bash_command='collect daily data requested by Team B',
    )
    DailyWatcher_complexdata1 = BashOperator(
            task_id='watch_complex1',
            bash_command='collect and compile daily data requested by Team C',
    )
    DailyWatcher_report = BashOperator(
            task_id='report_basic',
            bash_command='report document for collected daily basic data',
    )
    DailyWatcher_end = BashOperator(
            task_id='watch_end',
            bash_command='end process',
    )  


    DailyWatcher_start >> Report_start
    Report_start >> [Compile_risk, Compile_requestdata1, Compile_requestdata2, Compile_requestdata3] >> Report_end
    DailyWatcher_start >> [DailyWatcher_basicdata1, DailyWatcher_basicdata2, DailyWatcher_basicdata3, DailyWatcher_requestdata1, DailyWatcher_requestdata2, DailyWatcher_risk] >> DailyWatcher_report
    DailyWatcher_risk >> DailyWatcher_emergencyalert >> DailyWatcher_report
    DailyWatcher_report >> Report_start
    DailyWatcher_report >> DailyWatcher_end
    [DailyWatcher_basicdata1, DailyWatcher_basicdata2, DailyWatcher_requestdata1] >> DailyWatcher_complexdata1 >> DailyWatcher_report
    DailyWatcher_risk >> DailyWatcher_report
    
    
