from airflow.sdk import dag, task
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import logging
import uuid
import json
import psycopg2

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
DB_PARAMS = {
    "dbname": "work_db",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost", # Проверьте доступность изнутри пода!
    "port": "5432"
}

@dag(
    dag_id='kafka_to_postgres_stream',
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
            'group.id': 'airflow-persistent-consumer', # Оставим один ID, чтобы не дублировать данные
            'auto.offset.reset': 'earliest',
        }
        
        consumer = Consumer(conf)
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        try:
            metadata = consumer.list_topics(timeout=10)
            target_topics = [t for t in metadata.topics.keys() if not t.startswith('__')]
            consumer.subscribe(target_topics)
            
            logger.info(f"Начинаем сбор из: {target_topics}")

            # Бесконечный цикл чтения
            empty_count = 0
            while True:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    empty_count += 1
                    # Если 30 секунд нет сообщений, завершаем задачу (Airflow запустит её снова по расписанию)
                    if empty_count > 30: 
                        logger.info("Новых сообщений нет, завершаю цикл.")
                        break
                    continue
                
                empty_count = 0 # Сбрасываем счетчик, если что-то пришло

                if msg.error():
                    logger.error(f"Ошибка Kafka: {msg.error()}")
                    continue

                try:
                    # Парсим JSON
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Вставка в PostgreSQL
                    insert_query = """
                    INSERT INTO app_logs (log_timestamp, log_level, service_name, message, host_name, pid)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        data.get('timestamp'),
                        data.get('level'),
                        data.get('service'),
                        data.get('message'),
                        data.get('host'),
                        data.get('pid')
                    ))
                    conn.commit()
                    logger.info(f"Записан лог из {msg.topic()}")

                except Exception as parse_err:
                    logger.error(f"Ошибка парсинга или записи: {parse_err}")
                    conn.rollback()

        finally:
            cursor.close()
            conn.close()
            consumer.close()

    consume_to_db()

kafka_connection_check_dag = kafka_dag()
