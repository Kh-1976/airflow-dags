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
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='kafka_to_postgres_stream',
    default_args=default_args,
    schedule='*/5 * * * *',
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['kafka', 'postgres'],
)
def kafka_dag():
    @task
    def consume_to_db():
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'airflow-persistent-consumer',
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(conf)

        try:
            metadata = consumer.list_topics(timeout=10)
            target_topics = [t for t in metadata.topics.keys() if not t.startswith('__')]
            consumer.subscribe(target_topics)
            
            logger.info(f"Начинаем сбор из: {target_topics}")

            empty_count = 0
            while True:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    empty_count += 1
                    if empty_count > 30:
                        logger.info("Новых сообщений нет, завершаю цикл.")
                        break
                    continue
                
                empty_count = 0

                if msg.error():
                    logger.error(f"Ошибка Kafka: {msg.error()}")
                    continue

                try:
                    # Парсим JSON
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Преобразуем timestamp из формата "YYYY-MM-DD HH:MM:SS,mmm" в "YYYY-MM-DD HH:MM:SS.mmm"
                    timestamp_str = data['timestamp']
                    # Заменяем запятую на точку в миллисекундах
                    timestamp_str = timestamp_str.replace(',', '.')
                    
                    # Создаем кортеж для вставки
                    values = (
                        timestamp_str,  # Преобразованный timestamp
                        data.get('level'),
                        data.get('service'),
                        data.get('message'),
                        data.get('host'),
                        data.get('pid')
                    )
                    
                    # Вставка в PostgreSQL
                    hook = PostgresHook(postgres_conn_id='postgres_kafka_all_topics')
                    insert_query = """
                    INSERT INTO kafka_all_topics (log_timestamp, log_level, service_name, message, host_name, pid)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    hook.run(insert_query, parameters=values)
                    logger.info(f"Записан лог из {msg.topic()}")

                except Exception as parse_err:
                    logger.error(f"Ошибка парсинга или записи: {parse_err}")

        finally:
            consumer.close()

    consume_to_db()

kafka_connection_check_dag = kafka_dag()
