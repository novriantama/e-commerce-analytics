import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from minio import Minio
# pyrefly: ignore [missing-import]
from kafka import KafkaConsumer, KafkaProducer

# Configurations from environment variables
MINIO_HOST = "minio:" + os.getenv("MINIO_PORT", "9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio_admin_pass")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "ecommerce-raw")

KAFKA_BROKERS_ENV = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_BROKERS_LIST = [b.strip() for b in KAFKA_BROKERS_ENV.split(",")]

TOPICS = ["orders", "order_items", "order_payments", "order_reviews"]

def consume_and_upload_to_minio(topic_name):
    print(f"Connecting to Kafka cluster '{KAFKA_BROKERS_LIST}' to consume topic '{topic_name}'...")
    
    # Initialize Kafka consumer with manual offset commits (without deserializer so we handle errors)
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BROKERS_LIST,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'airflow_minio_{topic_name}_consumers',
        # Poll timeout: stop if no new messages are received for 4 seconds
        consumer_timeout_ms=4000
    )
    
    records = []
    dlq_producer = None
    print("Polling messages...")
    try:
        for message in consumer:
            raw_val = message.value
            try:
                # Decode and parse manually
                val = json.loads(raw_val.decode('utf-8'))
                if not isinstance(val, dict):
                    raise ValueError("Payload is not a valid JSON object")
                records.append(val)
            except Exception as e:
                print(f"Malformed payload found in topic {topic_name}: {e}. Routing to DLQ...")
                if dlq_producer is None:
                    dlq_producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKERS_LIST,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
                
                dlq_topic = f"{topic_name}_dlq"
                dlq_envelope = {
                    "original_topic": topic_name,
                    "raw_payload": raw_val.decode('utf-8', errors='replace'),
                    "error_msg": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                dlq_producer.send(dlq_topic, dlq_envelope)
                
            # Batch limit to prevent memory issues in Airflow
            if len(records) >= 20000:
                print(f"Reached batch limit of {len(records)} records. Ingesting batch...")
                break
    except StopIteration:
        pass
    finally:
        if dlq_producer is not None:
            dlq_producer.flush()
            dlq_producer.close()
        
    print(f"Polled {len(records)} valid messages from topic '{topic_name}'.")
    
    if len(records) == 0:
        consumer.close()
        print(f"No new events to write for '{topic_name}'.")
        return
        
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Standard cleanup for Pandas to keep nulls in Parquet
    df = df.replace({np.nan: None})
    
    # Create local temporary Parquet file
    local_parquet_path = f"/tmp/streaming_{topic_name}.parquet"
    print(f"Writing {len(df)} records to temporary Parquet file: {local_parquet_path}")
    df.to_parquet(local_parquet_path, index=False, engine="pyarrow")
    
    # Connect to MinIO
    print("Connecting to MinIO...")
    minio_client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )
    
    # Verify bucket exists
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        
    # Define MinIO partition path: bronze/topic/year=YYYY/month=MM/day=DD/file.parquet
    now = datetime.utcnow()
    partition_path = f"bronze/{topic_name}/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/{topic_name}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    
    print(f"Uploading Parquet file to MinIO bucket '{MINIO_BUCKET}' at '{partition_path}'...")
    minio_client.fput_object(MINIO_BUCKET, partition_path, local_parquet_path)
    
    # Cleanup temp file
    if os.path.exists(local_parquet_path):
        os.remove(local_parquet_path)
        
    # Commit Kafka offsets after successful write to data lake
    print("Committing Kafka offsets...")
    consumer.commit()
    consumer.close()
    
    print(f"Successfully processed and committed {len(records)} messages for topic '{topic_name}'!")

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "streaming_consumer_dag",
    default_args=default_args,
    description="Periodically consume streaming transactional events from Kafka to MinIO Data Lake",
    schedule_interval="*/5 * * * *",  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "streaming"],
) as dag:

    for topic in TOPICS:
        PythonOperator(
            task_id=f"consume_{topic}_to_minio",
            python_callable=consume_and_upload_to_minio,
            op_kwargs={"topic_name": topic},
        )
