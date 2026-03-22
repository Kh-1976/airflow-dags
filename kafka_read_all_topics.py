from airflow.sdk import dag, task
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import logging

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
    def create_consumer():
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'airflow-connection-test',
            'session.timeout.ms': 10000,
        })
        return consumer
    
    @task
    def check_kafka_connection(consumer):
        try:
            metadata = consumer.list_topics(timeout=10)
            topics = list(metadata.topics.keys())
            logger.info(f"Kafka доступен. Топиков: {len(topics)}")
            logger.info(f"Список топиков: {topics}")
            return topics
        finally:
            consumer.close()
    
    @task
    def read_all_messages(consumer, topics):
        consumer.subscribe(topics)
        
        all_messages = []
        
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    break
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Ошибка: {msg.error()}")
                        break
                
                message_data = {
                    'topic': msg.topic(),
                    'value': msg.value().decode('utf-8') if msg.value() else None,
                }
                all_messages.append(message_data)
                logger.info(f"Сообщение: {message_data}")
            
            logger.info(f"Всего сообщений: {len(all_messages)}")
            return all_messages
            
        finally:
            consumer.close()
    
    consumer = create_consumer()
    topics = check_kafka_connection(consumer)
    read_all_messages(consumer, topics)

# Создание экземпляра DAG
kafka_connection_check_dag = kafka_connection_check()
