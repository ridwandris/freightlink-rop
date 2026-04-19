import os
import pandas as pd
from hdbcli import dbapi
import kagglehub
from dotenv import load_dotenv

def download_kaggle_data():
    load_dotenv()
    print("[1] Authenticating and downloading Instacart dataset via KaggleHub...")
    dataset_path = kagglehub.dataset_download('yasserh/instacart-online-grocery-basket-analysis-dataset')
    print(f"[2] Download complete! Files cached at: {dataset_path}\n")
    return dataset_path

def push_to_sap_hana(dataset_path):
    load_dotenv()
    
    print("[3] Connecting to SAP HANA Cloud...")
    conn = dbapi.connect(
        address=os.getenv("SAP_HANA_ADDRESS"),
        port=443,
        user=os.getenv("SAP_HANA_USER"),
        password=os.getenv("SAP_HANA_PASSWORD"),
        encrypt="true",
        sslValidateCertificate="false"
    )
    cursor = conn.cursor()

    # The tables we need to seed (We include smaller dimension tables and the massive fact tables)
    tables_to_seed = [
        ('departments.csv', 'INSTACART_DEPARTMENTS'),
        ('products.csv', 'INSTACART_PRODUCTS'),
        ('orders.csv', 'INSTACART_ORDERS'),                     # 3.4M Rows
        ('order_products__prior.csv', 'INSTACART_ORDER_PRODUCTS') # 32M Rows
    ]

    for csv_file, table_name in tables_to_seed:
        csv_path = os.path.join(dataset_path, csv_file)
        print(f"\n[4 -> 1/2] Processing {table_name} from {csv_file}...")
        
        # We set chunksize to 100,000. This creates an Iterator instead of a DataFrame.
        chunk_size = 100000
        chunk_iterator = pd.read_csv(csv_path, chunksize=chunk_size)
        
        # Grab the very first chunk so we can dynamically build the SQL table schema
        first_chunk = next(chunk_iterator)
        
        # --- HANA SQL TEST ---
        try:
            cursor.execute(f"DROP TABLE {table_name}")
        except Exception:
            pass 
        
        # Create table using NVARCHAR(500) for staging (PySpark will enforce strict types later!)
        cols = ", ".join([f"{col.upper()} NVARCHAR(500)" for col in first_chunk.columns])
        cursor.execute(f"CREATE TABLE {table_name} ({cols})")
        print(f"🏗️ Table {table_name} created.")

        # Prepare the INSERT statement dynamically
        placeholders = ", ".join(["?" for _ in first_chunk.columns])
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        # --- Helper function to process and push a single chunk ---
        def process_and_insert(df_chunk, current_total):
            # The Pandas object cast fix for proper SQL NULLs
            df_chunk = df_chunk.astype(object).where(pd.notnull(df_chunk), None)
            data_tuples = [tuple(x) for x in df_chunk.values]
            
            cursor.executemany(insert_sql, data_tuples)
            return current_total + len(df_chunk)

        # 1. Insert that first chunk we grabbed for the schema
        total_inserted = process_and_insert(first_chunk, 0)
        print(f"   Inserted {total_inserted:,} rows so far...")

        # 2. Loop through the remaining chunks until the file is empty
        for chunk in chunk_iterator:
            total_inserted = process_and_insert(chunk, total_inserted)
            print(f"   Inserted {total_inserted:,} rows so far...")

        print(f"[4 -> 2/2] {table_name} Fully Seeded with {total_inserted:,} rows.")

    conn.commit()
    cursor.close()
    conn.close()
    print("\n[5] SUCCESS: ALL BIG DATA SEEDED TO SAP HANA CLOUD!")

if __name__ == "__main__":
    path = download_kaggle_data()
    push_to_sap_hana(path)