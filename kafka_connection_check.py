from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
import logging

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

def check_kafka_connection():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'airflow-connection-test',
        'session.timeout.ms': 10000,
    })
    
    try:
        metadata = consumer.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        logger.info(f"Kafka доступен. Топиков: {len(topics)}")
        logger.info(f"Список топиков: {topics}")
        return True
    finally:
        consumer.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 7),
    'retries': 1,
}

with DAG(
    'kafka_connection_check',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['kafka'],
) as dag:

    check = PythonOperator(
        task_id='check_connection',
        python_callable=check_kafka_connection,
    )
