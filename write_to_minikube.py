from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def save_to_local_fs():
    # Данные
    data = {'item': ['test_data'], 'timestamp': [datetime.now()]}
    df = pd.DataFrame(data)
    
    # Путь сохранения.
    # Если /opt/airflow/test.csv, то сохранится в ПОДе worker-0, а в minikube не сохранится. 
    target_path = '/opt/airflow/logs/test_minikube.csv' 
    
    # Запись
    df.to_csv(target_path, index=False)
    
    # Вывод списка файлов для проверки в логах
    print(f"Файл записан в: {target_path}")
    print("Содержимое директории:", os.listdir('/opt/airflow/logs'))

with DAG(
    dag_id='write_to_minikube',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    write_file = PythonOperator(
        task_id='save_csv_locally',
        python_callable=save_to_local_fs
    )
