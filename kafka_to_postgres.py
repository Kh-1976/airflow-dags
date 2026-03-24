from airflow.sdk import dag, task
from datetime import datetime, timedelta
import json
import logging
from confluent_kafka import Consumer
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='kafka_to_postgres',
    default_args=default_args,
    schedule='@hourly',  # Запуск каждый час
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
            'enable.auto.commit': True
        }
        
        consumer = Consumer(conf)
        hook = PostgresHook(postgres_conn_id='postgres_kafka_all_topics')
        
        try:
            # Получаем топики и подписываемся
            metadata = consumer.list_topics(timeout=10)
            target_topics = [t for t in metadata.topics.keys() if not t.startswith('__')]
            consumer.subscribe(target_topics)
            
            # Читаем пачку сообщений (максимум 1000 за раз или ждем 5 секунд)
            messages = consumer.consume(num_messages=1000, timeout=5.0)
            
            if not messages:
                logger.info("Новых сообщений не обнаружено.")
                return

            rows_to_insert = []
            for msg in messages:
                if msg.error():
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    # Форматируем timestamp (замена , на .)
                    ts = data['timestamp'].replace(',', '.')
                    
                    rows_to_insert.append((
                        ts, 
                        data.get('level'), 
                        data.get('service'), 
                        data.get('message'), 
                        data.get('host'), 
                        data.get('pid')
                    ))
                except Exception as e:
                    logger.error(f"Ошибка парсинга: {e}")

            if rows_to_insert:
                fields = ['log_timestamp', 'log_level', 'service_name', 'message', 'host_name', 'pid']
                hook.insert_rows(table='kafka_all_topics', rows=rows_to_insert, target_fields=fields)
                logger.info(f"Успешно записано {len(rows_to_insert)} строк.")

        finally:
            consumer.close()

    consume_to_db()

kafka_connection_check_dag = kafka_dag()
