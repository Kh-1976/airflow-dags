from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
import json
import logging
import os

logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    'bootstrap.servers': 'my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
    'group.id': 'airflow-consumer',
    'auto.offset.reset': 'earliest',
}

TOPICS = ['logs-debug', 'logs-info', 'logs-error', 'logs-warning', 'logs-critical', 'logs-trace']

def consume_messages(topic_name, max_messages=100, **context):
    consumer = Consumer({**KAFKA_CONFIG, 'group.id': f'airflow-{topic_name}'})
    consumer.subscribe([topic_name])
    messages = []
    
    try:
        for _ in range(max_messages):
            msg = consumer.poll(1.0)
            if not msg or msg.error():
                break
            messages.append(json.loads(msg.value().decode('utf-8')))
        
        if messages:
            os.makedirs('/opt/airflow/logs/kafka', exist_ok=True)
            with open(f'/opt/airflow/logs/kafka/{topic_name}.json', 'a') as f:
                json.dump(messages, f)
                f.write('\n')
        
        return len(messages)
    finally:
        consumer.close()

with DAG(
    'kafka_consumer',
    default_args={'owner': 'airflow', 'start_date': datetime(2026, 3, 7), 'retries': 2},
    schedule='*/5 * * * *',
    catchup=False,
) as dag:
    
    check = PythonOperator(
        task_id='check',
        python_callable=lambda: Consumer(KAFKA_CONFIG).list_topics(10)
    )
    
    consume_tasks = [
        PythonOperator(
            task_id=f'consume_{t.replace("-", "_")}',
            python_callable=consume_messages,
            op_kwargs={'topic_name': t}
        ) for t in TOPICS
    ]
    
    process = PythonOperator(
        task_id='process',
        python_callable=lambda ti: logger.info(f"Processed {len(ti.xcom_pull(task_ids='consume_*'))} topics"),
        trigger_rule='all_done'
    )
    
    check >> consume_tasks >> process
