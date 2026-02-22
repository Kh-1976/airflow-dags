from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def write_to_minio():
    # Используем S3Hook с ID вашего нового соединения
    hook = S3Hook(aws_conn_id='minio_id')
    # Создаем простой текст для загрузки
    content = 'Hello from Airflow in Minikube!'
    # Загружаем строку как файл 'hello.txt' в бакет 'airflow-bucket'
    hook.load_string(
        string_data=content,
        key='hello.txt',
        bucket_name='test-bucket-for-airflow',
        replace=True
    )
    print("Файл успешно загружен!")

with DAG(
    dag_id='test_minio_connection',
    start_date=datetime(2026, 2, 22),
    schedule=None,
    catchup=False
) as dag:
    test_task = PythonOperator(
        task_id='write_to_minio',
        python_callable=write_to_minio
    )
