import os
import boto3
from dotenv import load_dotenv

def wipe_instacart_bronze():
    print("CLEANING S3 BRONZE LAYER...")
    load_dotenv()
    
    s3 = boto3.resource('s3', region_name="eu-central-1")
    bucket = s3.Bucket(os.getenv("AWS_S3_BUCKET"))
    
    # We only delete the instacart folder to leave other projects alone
    prefix = "bronze_raw/instacart/"
    
    response = bucket.objects.filter(Prefix=prefix).delete()
    
    if response and 'Deleted' in response[0]:
        print(f"Successfully deleted {len(response[0]['Deleted'])} files.")
    else:
        print("No files found. Bucket is already clean.")
        
if __name__ == "__main__":
    wipe_instacart_bronze()