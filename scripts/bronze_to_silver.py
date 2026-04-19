import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from dotenv import load_dotenv
from pathlib import Path

# 1. Load ONLY the Bucket Name from our root .env
env_path = Path(__file__).resolve().parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

S3_BUCKET = os.getenv("AWS_S3_BUCKET")

if not S3_BUCKET:
    raise ValueError("CRITICAL: Missing AWS_S3_BUCKET in your root .env file!")

# 2. INITIALIZE PYSPARK USING LOCAL AWS PROFILE
print("[1] Booting PySpark Cluster using local AWS Profile...")
spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Transformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-central-1.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def process_table(table_name, schema, partition_col=None):
    """Generic function to read bronze CSVs and write silver Parquet."""
    print(f"[2] Processing {table_name}...")
    
    bronze_path = f"s3a://{S3_BUCKET}/bronze_raw/instacart/{table_name}/run_*/part_*.csv"
    df = spark.read.csv(bronze_path, header=True, schema=schema)
    
    silver_path = f"s3a://{S3_BUCKET}/silver_cleaned/instacart/{table_name}/"
    print(f"[3] Writing Parquet files to {silver_path}...")
    
    # Write to Parquet (partition if specified)
    if partition_col:
        df.write.mode("overwrite").partitionBy(partition_col).parquet(silver_path)
    else:
        df.write.mode("overwrite").parquet(silver_path)
        
    print(f"SUCCESS: {table_name} Transformation Complete!\n")

if __name__ == "__main__":
    
    # --- 1. DEPARTMENTS ---
    dept_schema = StructType([
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True)
    ])
    process_table("departments", dept_schema)

    # --- 2. PRODUCTS ---
    prod_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True)
    ])
    process_table("products", prod_schema)

    # --- 3. ORDERS ---
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("eval_set", StringType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_dow", IntegerType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", StringType(), True) 
    ])
    process_table("orders", orders_schema)

    # --- 4. ORDER PRODUCTS ---
    op_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True)
    ])
    process_table("order_products", op_schema)

    spark.stop()
    print("ALL TASKS COMPLETE: Silver Transformations Finished.")