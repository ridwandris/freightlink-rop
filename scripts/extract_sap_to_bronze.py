import os
import io
import boto3
import pandas as pd
from hdbcli import dbapi
from dotenv import load_dotenv
from datetime import datetime

def extract_to_datalake():
    print("🚀 STARTING BIG DATA EXTRACTION: SAP HANA -> AWS S3 BRONZE\n" + "="*50)
    load_dotenv()

    # 1. Connect to Source (SAP HANA)
    print("🔌 Connecting to SAP HANA Production Database...")
    hana_conn = dbapi.connect(
        address=os.getenv("SAP_HANA_ADDRESS"),
        port=443,
        user=os.getenv("SAP_HANA_USER"),
        password=os.getenv("SAP_HANA_PASSWORD"),
        encrypt="true",
        sslValidateCertificate="false"
    )
    cursor = hana_conn.cursor()
    print("✅ SAP Connection Established.")

    # 2. Connect to Destination (AWS S3)
    print("☁️  Connecting to AWS S3 using local AWS profile...")
    s3_client = boto3.client('s3', region_name="eu-central-1") 
    s3_bucket = os.getenv("AWS_S3_BUCKET")
    print("✅ AWS Connection Established.\n")

    # Tables to extract. We map the SAP table name to the folder name we want in S3.
    tables_to_extract = {
        'INSTACART_DEPARTMENTS': 'departments',
        'INSTACART_PRODUCTS': 'products',
        'INSTACART_ORDERS': 'orders',
        'INSTACART_ORDER_PRODUCTS': 'order_products'
    }
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chunk_size = 250000  # Extract and upload 250k rows at a time

    for table_name, s3_folder in tables_to_extract.items():
        print(f"📦 Extracting {table_name} from SAP...")
        
        # Execute the query
        cursor.execute(f"SELECT * FROM {table_name}")
        
        # Get column names for the CSV header
        columns = [col[0].lower() for col in cursor.description]
        
        part_number = 1
        total_rows_extracted = 0
        
        while True:
            # Fetch a chunk of rows from the SAP server
            results = cursor.fetchmany(chunk_size)
            if not results:
                break # We've reached the end of the table!
                
            df = pd.DataFrame(results, columns=columns)
            total_rows_extracted += len(df)
            
            # Create an in-memory string buffer for this chunk
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Define the S3 Path (e.g., bronze_raw/instacart/orders/run_20231025_120000/part_0001.csv)
            s3_key = f"bronze_raw/instacart/{s3_folder}/run_{timestamp}/part_{part_number:04d}.csv"
            
            print(f"   📤 Uploading Part {part_number} ({len(df):,} rows) to s3://{s3_bucket}/{s3_key}...")
            
            # Upload the chunk directly to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            
            part_number += 1

        print(f"✅ {table_name} Complete: Extracted {total_rows_extracted:,} total rows.\n")

    # Cleanup
    cursor.close()
    hana_conn.close()
    print("="*50 + "\n🏁 PIPELINE COMPLETE: All data landed in Bronze Layer.")

if __name__ == "__main__":
    extract_to_datalake()