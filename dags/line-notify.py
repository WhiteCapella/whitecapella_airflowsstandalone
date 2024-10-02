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
    'line-notify',
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3),
        'execution_timeout': timedelta(hours=2),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='line_test',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 1),
    end_date=datetime(2024, 8, 31),
    catchup=True,
    tags=['movie'],
) as dag:
    
    #Tasks
    #Empty Operators
    

    def predict():
        import os
        import matplotlib.pyplot as plt
        import pandas as pd
        import numpy as np
        from sklearn.neighbors import KNeighborsClassifier

        file_path = '/home/whitecapella/tmp/modeldata.csv'
        df = pd.read_csv(file_path)

        fish_data = []
        fish_target = []
        for index, row in df.iterrows():
            fish_data.append([row['Length'], row['Weight']])
        if row['Label'] == "Bream":
            fish_target.append(1)
        else:
            fish_target.append(0)

        kn1 = KNeighborsClassifier(n_neighbors=1)
        kn2 = KNeighborsClassifier(n_neighbors=4)
        kn3 = KNeighborsClassifier(n_neighbors=9)
        kn4 = KNeighborsClassifier(n_neighbors=16)
        kn5 = KNeighborsClassifier(n_neighbors=25)
        kn6 = KNeighborsClassifier(n_neighbors=36)
        kn7 = KNeighborsClassifier(n_neighbors=49)

        fish_array = np.array(fish_data)
        kn1.fit(fish_array, fish_target)
        kn2.fit(fish_array, fish_target)
        kn3.fit(fish_array, fish_target)
        kn4.fit(fish_array, fish_target)
        kn5.fit(fish_array, fish_target)
        kn6.fit(fish_array, fish_target)
        kn7.fit(fish_array, fish_target)

        mean = np.mean(fish_array, axis=0)
        std = np.std(fish_array, axis=0)

        new_data = pd.DataFrame({
            'Length': df['Length'],
            'Weight': df['Weight'],
            'Label': df['Label'] 
        })

        # 각 KNN 모델에 대해 예측 수행 및 결과 저장

        scaled_data = (new_data[['Length', 'Weight']] - mean) / std 
        new_data['kn1'] = kn1.predict(scaled_data)
        new_data['kn2'] = kn2.predict(scaled_data)
        new_data['kn3'] = kn3.predict(scaled_data)
        new_data['kn4'] = kn4.predict(scaled_data)
        new_data['kn5'] = kn5.predict(scaled_data)
        new_data['kn6'] = kn6.predict(scaled_data)
        new_data['kn7'] = kn7.predict(scaled_data)
    

        # 결과를 Parquet 파일로 저장
        new_data.to_parquet('predictions.parquet')


    start = EmptyOperator(
            task_id='start'
            )
    end = EmptyOperator(
            task_id='end',
            trigger_rule='one_success',
            )

    #Bash Operators
    prediction = PythonOperator(
            task_id='knn_prediction_task',
            python_callable=predict,  # prediction.py 실행
            dag=dag,
            )

    notify_success = BashOperator(
            task_id='notify.success',
            bash_command="""
            echo 'Success!'
            """,
            trigger_rule='one_success',
            )
    notify_failure = BashOperator(
            task_id='notify.failure',
            bash_command="""
            curl -X POST -H 'Authorization: Bearer lFAUGd2l1MgZkHf54FJmZEXgyExhjOiqB2ueZlGQe52' -F 'message=task {{ dag_run.dag_id }} return failure' https://notify-api.line.me/api/notify
            """,
            trigger_rule='one_failed'
            )


start >> prediction >> [ notify_success, notify_failure ] >> end

