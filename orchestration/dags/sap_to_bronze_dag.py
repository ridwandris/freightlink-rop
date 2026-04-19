from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import io
import boto3
import pandas as pd
from hdbcli import dbapi
from dotenv import load_dotenv
from pathlib import Path

# --- CREDENTIALS CONFIGURATION ---
env_path = Path(__file__).resolve().parents[2] / '.env'
load_dotenv(dotenv_path=env_path)

SAP_ADDRESS = os.getenv("SAP_HANA_ADDRESS")
SAP_USER = os.getenv("SAP_HANA_USER")
SAP_PASSWORD = os.getenv("SAP_HANA_PASSWORD")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")

# check so the DAG fails immediately if it can't find the root .env
if not SAP_PASSWORD:
    raise ValueError(f"CRITICAL: Could not load SAP credentials from {env_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_table_to_s3(table_name, s3_folder, chunk_size=250000):
    """Connects to SAP, pulls data in chunks, and uploads to S3."""
    print(f"[1] Connecting to SAP HANA to extract {table_name}...")
    
    # Validation to ensure env vars are loaded
    if not SAP_PASSWORD:
        raise ValueError("Missing SAP Credentials in .env file!")
        
    conn = dbapi.connect(
        address=SAP_ADDRESS, port=443, user=SAP_USER, password=SAP_PASSWORD,
        encrypt="true", sslValidateCertificate="false"
    )
    cursor = conn.cursor()
    s3_client = boto3.client('s3', region_name="eu-central-1")
    
    timestamp = datetime.now().strftime("%Y%m%d")
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [col[0].lower() for col in cursor.description]
    
    part_number = 1
    total_rows = 0
    
    while True:
        results = cursor.fetchmany(chunk_size)
        if not results:
            break
            
        df = pd.DataFrame(results, columns=columns)
        total_rows += len(df)
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3_key = f"bronze_raw/instacart/{s3_folder}/run_{timestamp}/part_{part_number:04d}.csv"
        print(f"[2] Uploading Part {part_number} ({len(df)} rows) to S3...")
        
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
        part_number += 1

    cursor.close()
    conn.close()
    print(f"{table_name} Complete: {total_rows} rows extracted.")

# --- DEFINE THE DAG ---
with DAG(
    'sap_to_s3_bronze_ingestion',
    default_args=default_args,
    description='Extracts Instacart Big Data from SAP HANA to AWS S3 securely',
    schedule=None, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'bronze', 'sap'],
) as dag:

    extract_departments = PythonOperator(
        task_id='extract_departments',
        python_callable=extract_table_to_s3,
        op_kwargs={'table_name': 'INSTACART_DEPARTMENTS', 's3_folder': 'departments'}
    )

    extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=extract_table_to_s3,
        op_kwargs={'table_name': 'INSTACART_PRODUCTS', 's3_folder': 'products'}
    )

    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_table_to_s3,
        op_kwargs={'table_name': 'INSTACART_ORDERS', 's3_folder': 'orders'}
    )

    extract_order_products = PythonOperator(
        task_id='extract_order_products',
        python_callable=extract_table_to_s3,
        op_kwargs={'table_name': 'INSTACART_ORDER_PRODUCTS', 's3_folder': 'order_products', 'chunk_size': 500000}
    )

    transform_bronze_to_silver = BashOperator(
        task_id='transform_bronze_to_silver_pyspark',
        bash_command='cd $AIRFLOW_HOME/.. && uv run python scripts/bronze_to_silver.py',
    )

    # Dependency Map
    [extract_departments, extract_products, extract_orders, extract_order_products] >> transform_bronze_to_silver