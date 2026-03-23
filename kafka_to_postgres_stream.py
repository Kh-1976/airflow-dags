from airflow.sdk import dag, task
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import uuid
import json
import psycopg2

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),}

@dag(
    dag_id='kafka_to_postgres_stream',
    default_args=default_args,
    schedule='*/5 * * * *',
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['kafka', 'postgres'],)
def kafka_dag():
    @task
    def consume_to_db():
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'airflow-persistent-consumer', # Оставим один ID, чтобы не дублировать данные
            'auto.offset.reset': 'earliest',
        }   
        consumer = Consumer(conf)

        try:
            metadata = consumer.list_topics(timeout=10)
            target_topics = [t for t in metadata.topics.keys() if not t.startswith('__')]
            consumer.subscribe(target_topics)
            
            logger.info(f"Начинаем сбор из: {target_topics}")
            data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Данные в data выглядят так: {data}")
        finally:
            consumer.close()

    consume_to_db()

kafka_connection_check_dag = kafka_dag()
