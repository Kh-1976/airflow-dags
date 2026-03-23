from airflow.sdk import dag, task
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import json
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='kafka_to_postgres_batch',
    default_args=default_args,
    schedule='*/5 * * * *',
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['kafka', 'postgres'],
    max_active_runs=1,  # Запускаем только одну активную сессию
)
def kafka_dag():
    
    @task
    def consume_and_save():
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'airflow-batch-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Ручной коммит для контроля
            'max.poll.interval.ms': 300000,  # 5 минут
        }
        
        consumer = Consumer(conf)
        hook = PostgresHook(postgres_conn_id='postgres_kafka_all_topics')
        
        try:
            # Получаем все топики, исключая внутренние
            metadata = consumer.list_topics(timeout=10)
            target_topics = [t for t in metadata.topics.keys() if not t.startswith('__')]
            consumer.subscribe(target_topics)
            
            logger.info(f"Подписались на топики: {target_topics}")
            
            batch_size = 100  # Размер пачки для записи
            batch_timeout = 10  # Таймаут в секундах
            messages_batch = []
            total_processed = 0
            
            start_time = datetime.now()
            
            while True:
                # Проверяем, не истекло ли время выполнения задачи
                if (datetime.now() - start_time).seconds > 50:  # 50 секунд на выполнение
                    logger.info(f"Достигнут лимит времени, обработано {total_processed} сообщений")
                    break
                
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    if messages_batch:
                        # Если есть накопленные сообщения, записываем их
                        save_to_postgres(hook, messages_batch)
                        messages_batch = []
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Конец партиции, продолжаем
                        continue
                    else:
                        logger.error(f"Ошибка Kafka: {msg.error()}")
                        break
                
                try:
                    # Парсим JSON
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Преобразуем в формат для вставки
                    record = (
                        data.get('timestamp'),
                        data.get('level'),
                        data.get('service'),
                        data.get('message'),
                        data.get('host'),
                        data.get('pid')
                    )
                    
                    messages_batch.append(record)
                    total_processed += 1
                    
                    # Если набрали достаточно сообщений - записываем
                    if len(messages_batch) >= batch_size:
                        save_to_postgres(hook, messages_batch)
                        messages_batch = []
                        
                        # Коммитим offset после успешной записи
                        consumer.commit(asynchronous=False)
                        logger.info(f"Записано и закоммичено {total_processed} сообщений")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга JSON: {e}, сообщение: {msg.value()}")
                    # Для плохих сообщений все равно коммитим, чтобы не застревать
                    consumer.commit(asynchronous=False)
                    continue
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    continue
            
            # Записываем оставшиеся сообщения
            if messages_batch:
                save_to_postgres(hook, messages_batch)
                consumer.commit(asynchronous=False)
                logger.info(f"Записано последние {len(messages_batch)} сообщений")
            
            logger.info(f"Задача завершена. Всего обработано: {total_processed} сообщений")
            
        except KafkaException as e:
            logger.error(f"Kafka ошибка: {e}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            raise
        finally:
            consumer.close()
    
    consume_and_save()

def save_to_postgres(hook, messages):
    """Сохраняет пачку сообщений в PostgreSQL"""
    if not messages:
        return
    
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO kafka_all_topics 
            (log_timestamp, log_level, service_name, message, host_name, pid)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            execute_values(cursor, insert_query, messages)
            conn.commit()
            logger.info(f"Успешно записано {len(messages)} сообщений в PostgreSQL")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка записи в PostgreSQL: {e}")
        raise
    finally:
        conn.close()

kafka_dag_instance = kafka_dag()
