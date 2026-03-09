from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import logging
from typing import Dict, List, Optional
import time
import os

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
    'group.id': 'airflow-confluent-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'max.poll.interval.ms': 300000,
    'session.timeout.ms': 45000,
    'heartbeat.interval.ms': 15000,
    'max.partition.fetch.bytes': 1048576,  # 1MB
}

TOPICS = [
    'logs-debug',
    'logs-info', 
    'logs-error',
    'logs-warning',
    'logs-critical',
    'logs-trace'
]

def create_consumer(topic: str) -> Consumer:
    """
    Создание consumer для конкретного топика
    """
    config = KAFKA_CONFIG.copy()
    config['group.id'] = f'airflow-confluent-{topic}'
    config['client.id'] = f'airflow-consumer-{topic}'
    
    return Consumer(config)

def consume_messages_confluent(topic_name: str, max_messages: int = 100, **context) -> int:
    """
    Чтение сообщений из Kafka топика с использованием confluent_kafka
    """
    consumer = None
    messages_collected = []
    message_count = 0
    
    try:
        # Создание consumer
        consumer = create_consumer(topic_name)
        
        # Подписка на топик
        consumer.subscribe([topic_name])
        
        logger.info(f"Начало чтения из топика: {topic_name}")
        
        # Таймаут для чтения (10 секунд)
        timeout = 10.0
        start_time = time.time()
        
        while time.time() - start_time < timeout and message_count < max_messages:
            # Чтение сообщения с таймаутом 1 секунда
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Достигнут конец партиции
                    logger.debug(f"Достигнут конец партиции для {topic_name}")
                    break
                else:
                    raise KafkaException(msg.error())
            
            # Успешное получение сообщения
            try:
                # Декодирование значения
                value = msg.value().decode('utf-8')
                
                # Попытка парсинга JSON
                try:
                    value_json = json.loads(value)
                except json.JSONDecodeError:
                    # Если не JSON, сохраняем как строку
                    value_json = {"raw_message": value}
                
                message_data = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': value_json,
                    'timestamp': msg.timestamp()[1],  # timestamp in milliseconds
                    'headers': dict(msg.headers()) if msg.headers() else {}
                }
                
                messages_collected.append(message_data)
                message_count += 1
                
                # Логирование первого сообщения для примера
                if message_count <= 3:
                    logger.info(f"Пример сообщения из {topic_name}: {json.dumps(message_data, indent=2)}")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {str(e)}")
                continue
        
        logger.info(f"Получено {message_count} сообщений из топика {topic_name}")
        
        # Сохранение в XCom
        context['task_instance'].xcom_push(
            key=f'messages_{topic_name}',
            value=messages_collected
        )
        
        # Сохранение в файл
        save_messages_to_file(topic_name, messages_collected)
        
        # Сохранение статистики
        context['task_instance'].xcom_push(
            key=f'stats_{topic_name}',
            value={'count': message_count, 'last_offset': msg.offset() if message_count > 0 else -1}
        )
        
        return message_count
        
    except Exception as e:
        logger.error(f"Критическая ошибка при чтении из топика {topic_name}: {str(e)}")
        raise
    finally:
        if consumer:
            consumer.close()
            logger.debug(f"Consumer для {topic_name} закрыт")

def save_messages_to_file(topic_name: str, messages: List[Dict]):
    """
    Сохранение сообщений в файл с ротацией
    """
    if not messages:
        return
    
    # Создание директории если не существует
    log_dir = '/opt/airflow/logs/kafka_messages'
    os.makedirs(log_dir, exist_ok=True)
    
    # Формирование имени файла с датой
    date_str = datetime.now().strftime('%Y%m%d')
    filename = f'{log_dir}/{topic_name}_{date_str}.json'
    
    try:
        # Загрузка существующих сообщений если файл есть
        existing_messages = []
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                try:
                    existing_messages = json.load(f)
                except:
                    existing_messages = []
        
        # Добавление новых сообщений
        all_messages = existing_messages + messages
        
        # Сохранение в файл
        with open(filename, 'w') as f:
            json.dump(all_messages, f, indent=2, default=str)
        
        logger.info(f"Сохранено {len(messages)} сообщений в {filename}. Всего: {len(all_messages)}")
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении в файл: {str(e)}")

def process_all_topics(**context):
    """
    Обработка и агрегация данных из всех топиков
    """
    all_stats = {}
    total_messages = 0
    
    for topic in TOPICS:
        # Получение статистики из XCom
        stats = context['task_instance'].xcom_pull(
            key=f'stats_{topic}',
            task_ids=f'consume_{topic.replace("-", "_")}'
        )
        
        if stats:
            all_stats[topic] = stats
            total_messages += stats.get('count', 0)
            
        # Получение сообщений для дополнительной обработки
        messages = context['task_instance'].xcom_pull(
            key=f'messages_{topic}',
            task_ids=f'consume_{topic.replace("-", "_")}'
        )
        
        if messages:
            # Пример анализа сообщений по уровню
            if topic == 'logs-error':
                analyze_error_patterns(messages)
            elif topic == 'logs-critical':
                trigger_alerts_for_critical(messages)
    
    # Создание отчета
    report = {
        'timestamp': datetime.now().isoformat(),
        'total_messages': total_messages,
        'statistics': all_stats
    }
    
    # Сохранение отчета
    save_report(report)
    
    logger.info(f"Агрегация завершена. Всего сообщений: {total_messages}")
    logger.info(f"Статистика: {json.dumps(all_stats, indent=2)}")
    
    context['task_instance'].xcom_push(key='final_report', value=report)
    
    return total_messages

def analyze_error_patterns(messages: List[Dict]):
    """
    Анализ паттернов ошибок
    """
    error_patterns = {}
    
    for msg in messages:
        value = msg.get('value', {})
        if isinstance(value, dict):
            service = value.get('service', 'unknown')
            error_patterns[service] = error_patterns.get(service, 0) + 1
    
    if error_patterns:
        logger.info(f"Распределение ошибок по сервисам: {error_patterns}")
    
    return error_patterns

def trigger_alerts_for_critical(messages: List[Dict]):
    """
    Отправка алертов для критических сообщений
    """
    critical_count = len(messages)
    if critical_count > 0:
        logger.warning(f"Обнаружено {critical_count} критических сообщений!")
        # Здесь можно добавить отправку email или другого уведомления

def save_report(report: Dict):
    """
    Сохранение отчета
    """
    report_dir = '/opt/airflow/logs/kafka_reports'
    os.makedirs(report_dir, exist_ok=True)
    
    filename = f'{report_dir}/report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Отчет сохранен в {filename}")

def check_kafka_connection(**context):
    """
    Проверка подключения к Kafka перед основными задачами
    """
    try:
        consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
            'group.id': 'airflow-connection-check',
            'session.timeout.ms': 6000,
        })
        
        # Получение списка топиков
        topics_metadata = consumer.list_topics(timeout=10)
        available_topics = topics_metadata.topics.keys()
        
        logger.info(f"Доступные топики: {list(available_topics)}")
        
        # Проверка наличия нужных топиков
        missing_topics = [t for t in TOPICS if t not in available_topics]
        if missing_topics:
            logger.warning(f"Отсутствуют топики: {missing_topics}")
        else:
            logger.info("Все необходимые топики доступны")
        
        consumer.close()
        
        context['task_instance'].xcom_push(key='available_topics', value=list(available_topics))
        
        return len(available_topics)
        
    except Exception as e:
        logger.error(f"Ошибка подключения к Kafka: {str(e)}")
        raise

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 7),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],  # Замените на ваш email
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'kafka_logs_consumer_confluent',
    default_args=default_args,
    description='Consume log messages from Kafka topics using confluent_kafka',
    schedule='*/5 * * * *',  # Каждые 5 минут
    catchup=False,
    tags=['kafka', 'logs', 'confluent'],
    max_active_runs=1,
) as dag:

    # Задача проверки подключения
    check_connection = PythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection,
        provide_context=True,
    )
    
    # Создаем задачи для каждого топика
    consume_tasks = []
    for topic in TOPICS:
        task_id = f'consume_{topic.replace("-", "_")}'
        consume_task = PythonOperator(
            task_id=task_id,
            python_callable=consume_messages_confluent,
            op_kwargs={
                'topic_name': topic,
                'max_messages': 200  # Максимум сообщений за запуск
            },
            provide_context=True,
            retries=1,
            retry_delay=timedelta(minutes=1),
        )
        consume_tasks.append(consume_task)
    
    # Задача для агрегации и обработки
    process_all = PythonOperator(
        task_id='process_all_topics',
        python_callable=process_all_topics,
        provide_context=True,
        trigger_rule='all_done',  # Запускаем даже если некоторые задачи завершились с ошибкой
    )
    
    # Настройка зависимостей
    check_connection >> consume_tasks >> process_all
