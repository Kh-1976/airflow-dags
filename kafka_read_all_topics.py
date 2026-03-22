from airflow.sdk import dag, task
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import logging
import uuid

logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

@dag(
    dag_id='kafka_read_all_topics',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kafka'],
)
def kafka_dag():

    @task
    def check_and_read():
        # Используем уникальный group.id, чтобы гарантированно прочитать данные с начала
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'airflow-test-{uuid.uuid4().hex[:8]}',
            'auto.offset.reset': 'earliest',
            'session.timeout.ms': 10000,
        }
        
        consumer = Consumer(conf)
        
        try:
            # 1. Получаем список топиков и фильтруем служебные
            metadata = consumer.list_topics(timeout=10)
            all_topics = list(metadata.topics.keys())
            target_topics = [t for t in all_topics if not t.startswith('__')]
            
            logger.info(f"Доступные топики: {all_topics}")
            logger.info(f"Подписываемся на пользовательские топики: {target_topics}")
            
            if not target_topics:
                return "Нет доступных топиков для чтения"

            consumer.subscribe(target_topics)

            messages = []
            # Увеличиваем количество попыток poll, так как подписка занимает время
            max_attempts = 20 
            
            for i in range(max_attempts):
                msg = consumer.poll(timeout=2.0) # Ждем сообщение до 2 секунд
                
                if msg is None:
                    logger.info(f"Попытка {i+1}: сообщений пока нет...")
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Ошибка Kafka: {msg.error()}")
                        break
                
                # Если сообщение получено
                payload = msg.value().decode('utf-8') if msg.value() else ""
                topic = msg.topic()
                messages.append(payload)
                
                logger.info(f"ПОЛУЧЕНО из [{topic}]: {payload[:100]}...")
                
                # Ограничим выборку для теста
                if len(messages) >= 10:
                    break
            
            result_str = f"Прочитано сообщений: {len(messages)}"
            logger.info(result_str)
            return result_str
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise
        finally:
            consumer.close()

    check_and_read()

# Создание экземпляра DAG
kafka_connection_check_dag = kafka_dag()
