import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from minio import Minio
# pyrefly: ignore [missing-import]
import clickhouse_connect

# Configurations from environment variables
MINIO_HOST = "minio:"+os.getenv("MINIO_PORT", "9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio_admin_pass")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "ecommerce-raw")

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse_admin")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_admin_pass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "ecommerce")

DATA_DIR = "/opt/airflow/data"

# Mapping dataset key to CSV file name and target clickhouse table
DATASETS = {
    "customers": {
        "file": "olist_customers_dataset.csv",
        "table": "customers"
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "table": "geolocation"
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "table": "products"
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "table": "sellers"
    },
    "category": {
        "file": "product_category_name_translation.csv",
        "table": "product_category_name_translation"
    }
}

def clean_and_upload_to_minio(dataset_name):
    config = DATASETS[dataset_name]
    csv_path = os.path.join(DATA_DIR, config["file"])
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
        
    print(f"Reading dataset: {dataset_name} from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Cleaning steps per dataset
    if dataset_name == "customers":
        df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].astype(str).str.zfill(5)
    elif dataset_name == "sellers":
        df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].astype(str).str.zfill(5)
    elif dataset_name == "geolocation":
        df["geolocation_zip_code_prefix"] = df["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    elif dataset_name == "products":
        # Handle Nullable Integer columns
        int_cols = ["product_name_lenght", "product_description_lenght", "product_photos_qty"]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
        # Handle Nullable Float columns
        float_cols = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        # Handle Nullable String
        df["product_category_name"] = df["product_category_name"].replace({np.nan: None})
    
    # General cleanup: replace nan with None to preserve Null in Parquet
    df = df.replace({np.nan: None})
    
    local_parquet_path = f"/tmp/{dataset_name}.parquet"
    print(f"Saving cleaned dataset to temporary parquet: {local_parquet_path}...")
    df.to_parquet(local_parquet_path, index=False, engine="pyarrow")
    
    # Upload to MinIO
    print("Connecting to MinIO...")
    minio_client = Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False
    )
    
    # Verify bucket exists (should be created by minio-init)
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        
    minio_path = f"bronze/{config['table']}/{config['table']}.parquet"
    print(f"Uploading to MinIO bucket '{MINIO_BUCKET}' at '{minio_path}'...")
    minio_client.fput_object(MINIO_BUCKET, minio_path, local_parquet_path)
    
    # Cleanup local temp file
    if os.path.exists(local_parquet_path):
        os.remove(local_parquet_path)
        
    print(f"Dataset {dataset_name} uploaded successfully to MinIO!")

def load_from_minio_to_clickhouse(dataset_name):
    config = DATASETS[dataset_name]
    table_name = config["table"]
    
    print("Connecting to ClickHouse...")
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB
    )
    
    print(f"Truncating table {CLICKHOUSE_DB}.{table_name}...")
    ch_client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{table_name}")
    
    # Load using ClickHouse s3 function
    s3_url = f"http://{MINIO_HOST}/{MINIO_BUCKET}/bronze/{table_name}/*.parquet"
    print(f"Loading data into ClickHouse from S3 URL: {s3_url}...")
    
    load_query = f"""
    INSERT INTO {CLICKHOUSE_DB}.{table_name}
    SELECT * FROM s3('{s3_url}', '{MINIO_USER}', '{MINIO_PASS}', 'Parquet')
    """
    
    ch_client.command(load_query)
    
    # Verify row count
    result = ch_client.command(f"SELECT count() FROM {CLICKHOUSE_DB}.{table_name}")
    print(f"ClickHouse Table '{table_name}' loaded successfully! Total rows: {result}")

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "batch_ingestion_dag",
    default_args=default_args,
    description="Batch process e-commerce CSV datasets (excl. orders) to MinIO and ClickHouse",
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["ecommerce", "batch"],
) as dag:

    for dataset_key in DATASETS.keys():
        t1 = PythonOperator(
            task_id=f"process_{dataset_key}_to_minio",
            python_callable=clean_and_upload_to_minio,
            op_kwargs={"dataset_name": dataset_key},
        )
        
        t2 = PythonOperator(
            task_id=f"load_{dataset_key}_to_clickhouse",
            python_callable=load_from_minio_to_clickhouse,
            op_kwargs={"dataset_name": dataset_key},
        )
        
        t1 >> t2
