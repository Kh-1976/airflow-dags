from airflow.sdk import dag, task
from datetime import datetime
from confluent_kafka import Consumer
import logging

# Для того чтобы в ходе выполнения программы вставлять в отчет свои комментарии
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'


@dag(
    dag_id='kafka_connection_check_dekorator',
    schedule='*/5 * * * *',
    start_date=datetime(2026, 3, 7),
    catchup=False,
    tags=['kafka'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
)
def kafka_connection_check():
    @task
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

    check = check_kafka_connection()


# Создание экземпляра DAG
kafka_connection_check_dag = kafka_connection_check()
