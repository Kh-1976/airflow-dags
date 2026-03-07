from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import logging
from typing import Dict, List
import time

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = ['my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092']
TOPICS = [
    'logs-debug',
    'logs-info', 
    'logs-error',
    'logs-warning',
    'logs-critical',
    'logs-trace'
]

def consume_kafka_messages(topic_name: str, **context):
    """
    Функция для чтения сообщений из указанного топика Kafka
    """
    consumer = None
    try:
        # Создание Kafka consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',  # Читаем с начала
            enable_auto_commit=True,
            group_id=f'airflow-consumer-{topic_name}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000,  # Таймаут 10 секунд
            max_poll_records=100  # Максимальное количество сообщений за раз
        )
        
        messages = []
        message_count = 0
        
        # Чтение сообщений
        for message in consumer:
            messages.append({
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'value': message.value,
                'timestamp': message.timestamp
            })
            message_count += 1
            
            logger.info(f"Получено сообщение из {topic_name}: {message.value}")
            
            # Лимит на количество сообщений за один запуск
            if message_count >= 100:
                break
        
        # Сохраняем результаты в XCom для использования в других задачах
        context['task_instance'].xcom_push(key=f'messages_{topic_name}', value=messages)
        
        logger.info(f"Всего получено {message_count} сообщений из топика {topic_name}")
        
        # Здесь можно добавить логику сохранения в БД или файлы
        save_messages_to_storage(topic_name, messages)
        
        return message_count
        
    except Exception as e:
        logger.error(f"Ошибка при чтении из топика {topic_name}: {str(e)}")
        raise
    finally:
        if consumer:
            consumer.close()

def save_messages_to_storage(topic_name: str, messages: List[Dict]):
    """
    Сохранение сообщений в файлы или базу данных
    """
    if not messages:
        return
    
    # Пример сохранения в файл
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'/opt/airflow/logs/kafka_messages/{topic_name}_{timestamp}.json'
    
    try:
        with open(filename, 'w') as f:
            json.dump(messages, f, indent=2, default=str)
        logger.info(f"Сохранено {len(messages)} сообщений в файл {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении в файл: {str(e)}")

def aggregate_all_topics(**context):
    """
    Агрегация данных из всех топиков
    """
    all_messages = {}
    total_count = 0
    
    for topic in TOPICS:
        messages = context['task_instance'].xcom_pull(
            key=f'messages_{topic}',
            task_ids=f'consume_{topic.replace("-", "_")}'
        )
        if messages:
            all_messages[topic] = messages
            total_count += len(messages)
    
    logger.info(f"Всего получено {total_count} сообщений из всех топиков")
    
    # Анализ распределения по уровням логирования
    level_stats = {}
    for topic, messages in all_messages.items():
        level_stats[topic] = len(messages)
    
    logger.info(f"Статистика по уровням логирования: {level_stats}")
    
    # Сохраняем статистику
    context['task_instance'].xcom_push(key='level_stats', value=level_stats)
    
    return total_count

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_logs_consumer',
    default_args=default_args,
    description='Consume log messages from Kafka topics',
    schedule_interval='*/5 * * * *',  # Каждые 5 минут
    catchup=False,
    tags=['kafka', 'logs'],
) as dag:

    # Создаем задачи для каждого топика
    consume_tasks = []
    for topic in TOPICS:
        task_id = f'consume_{topic.replace("-", "_")}'
        consume_task = PythonOperator(
            task_id=task_id,
            python_callable=consume_kafka_messages,
            op_kwargs={'topic_name': topic},
            provide_context=True,
        )
        consume_tasks.append(consume_task)
    
    # Задача для агрегации всех сообщений
    aggregate_task = PythonOperator(
        task_id='aggregate_all_topics',
        python_callable=aggregate_all_topics,
        provide_context=True,
    )
    
    # Настройка зависимостей
    consume_tasks >> aggregate_task
