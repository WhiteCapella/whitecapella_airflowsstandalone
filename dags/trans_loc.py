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
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def db_login_func():
    # TODO
    # 로그인 시도
    try:
        conn = mysql.connector.connect(
            host="13.209.89.75",
            port=32678,
            user="pic",
            password="1234",
            database="picturedb"
        )
        print("MariaDB 연결 성공")
        conn.close() # 연결 종료
    except Exception as e:
        print(f"MariaDB 연결 실패: {e}")
        raise AirflowException("MariaDB 연결 실패")

def db_check_func():
    # TODO
    # 업데이트 해야하는 DB 체크
    conn = mysql.connector.connect(
        host="13.209.89.75",
        port=32678,
        user="pic",
        password="1234",
        database="picturedb"
    )
    with conn.get_conn() as cur:
        cur.execute("SELECT COUNT(*) FROM picture WHERE address = ''")  
        # address 컬럼이 비어있는 행 개수 확인
        empty_count = cur.fetchone()[0]
    if empty_count == 0:
        return 'need_not_update'  
        # 비어있는 행이 없으면 need_not_update 태스크로 이동
    else:
        return 'db_update'

def db_update_func():
    # TODO
    # 업데이트 진행
    conn = mysql.connector.connect(
        host="13.209.89.75",
        port=32678,
        user="pic",
        password="1234",
        database="picturedb"
    )

def task_succ_aleam():
    # TODO
    # 알람 - 작업 완료

def login_fail_aleam():
    # TODO
    # 알람 - 로그인 실패

def nothing_to_update_aleam():
    # TODO
    # 알람 - 작업할 필요 없음

def error_aleam():
    # TODO
    # 알람 - 작업 에러
with DAG(
    'Transfer Location',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        'execution_timeout': timedelta(hours=2),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='Transform location to address using API',
    schedule="3 * * * *",
    start_date=datetime(2024, 10, 1),
    catchup=True,
    tags=['API','geometry_transform'],


) as dag:
    

    # Line notify 로그 적극 사용해보고자 함.
    #         login_fail   nothing_to_update    error
    # start >> db_login >> db_check >>          update >> task_succ >> end
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    db_login = PythonOperator(
        task_id='db_login',
        python_callable=db_login_func,
    )
    db_check = BranchPythonOperator(
        task_id='db.check',
        python_callable=db_check_func,
    )
    db_update = PythonOperator(
        task_id='db.update',
        python_callable=db_update_func,
    )

    task_succ = PythonOperator(
        task_id='task.suc',
        python_callable=task_succ_func,
    )

    login_fail = EmptyOperator(task_id='login.fail')
    need_not_update = EmptyOperator(task_id='nnu')
    error = EmptyOperator(task_id='error')


start >> db_login >> db_check >> db_update >> task_succ >> end
db_login >> login_fail >> end
db_check >> need_not_update >> task_succ >> end
db_update >> error >> end
