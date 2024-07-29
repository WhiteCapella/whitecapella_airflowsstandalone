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
            U_PATH=/home/whitecapella/data/count/{{ds_nodash}}/count.log
            CSV_PATH=/home/whitecapella/data/csv/{{ds_nodash}}
            CSV_FILE=/home/whitecapella/data/csv/{{ds_nodash}}/csv.csv

            mkdir -p $CSV_PATH
            sudo chown mysql:mysql /home/whitecapella/data/csv/{{ds_nodash}}
            cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_FILE}
            chmod 755 /home/whitecapella/data/csv/{{ds_nodash}}
            chmod 644 /home/whitecapella/data/csv/{{ds_nodash}}/csv.csv

            """)
    
    task_totmp = BashOperator (
            task_id='to_tmp', 
            bash_command=""" 
            echo "to.tmp_db"
            user="root"
            password="qwer123"
            database="history_db"
            mysql --local-infile=1 -u"$user" -p"$password" "$database" << EOF

            LOAD DATA LOCAL INFILE '/home/whitecapella/data/csv/{{ds_nodash}}/csv.csv' INTO TABLE history_db.tmp_cmd_usage FIELDS TERMINATED BY ',' ENCLOSED BY ' ' LINES TERMINATED BY '\n';
            """)
    task_tobase = BashOperator(
        task_id='to_base',
        bash_command="""
        echo "to.base_db"
        user="root"
        password="qwer123"
        database="history_db"
        mysql -u"$user" -p"$password" "$database" << EOF
    
        USE history_db;
        INSERT INTO cmd_usage (dt, command, cnt)
        SELECT 
            STR_TO_DATE(REPLACE(REPLACE(dt, ',', ''), '^', ''), '%Y-%m-%d'),
            REPLACE(REPLACE(command, ',', ''), '^',''),
            CAST(REPLACE(REPLACE(cnt, ',', ''), '^', '') AS SIGNED)
        FROM tmp_cmd_usage;
    """
    )
    task_mkdone = BashOperator (
            task_id='make_done', 
            bash_command=""" 
            mkdir -p "/home/whitecapella/data/import_signal/{{ds_nodash}}"
            touch /home/whitecapella/data/import_signal/{{ds_nodash}}/_DONE
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
