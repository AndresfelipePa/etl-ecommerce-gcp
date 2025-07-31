from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSUploadObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import requests
import json
import tempfile
from google.cloud import storage

default_args = {
    'start_date': datetime(2025, 7, 31),
    'retries': 1
}

BUCKET_NAME = 'gcp-prue-data-engineer'
PROJECT_ID = 'prueba-data-engineer'
DATASET = 'data'

def fetch_and_process_data(endpoint, gcs_path, is_purchases=False):
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()['data']
    
    if is_purchases:
        flat_data = []
        purchase_products = []
        for item in data:
            flat_item = {
                'id': item['id'],
                'status': item['status'],
                'creditCardNumber': item['creditCardNumber'],
                'creditCardType': item['creditCardType'],
                'purchaseDate': item['purchaseDate']
            }
            flat_data.append(flat_item)
            for p in item['products']:
                purchase_products.append({
                    'purchase_id': item['id'],
                    'product_id': p['id'],
                    'discount': p['discount']
                })
        
        # Guardar ambos
        save_to_gcs('purchases.json', flat_data, gcs_path)
        save_to_gcs('purchase_products.json', purchase_products, gcs_path)
    else:
        save_to_gcs('products.json', data, gcs_path)

def save_to_gcs(filename, content, gcs_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{gcs_path}/{filename}")
    blob.upload_from_string(data=json.dumps(content), content_type='application/json')

with DAG('etl_api_to_bigquery',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    download_products = PythonOperator(
        task_id='download_products',
        python_callable=fetch_and_process_data,
        op_kwargs={
            'endpoint': 'https://mnpwhdbcsk.us-east-2.awsapprunner.com/api/products',
            'gcs_path': 'etl',
            'is_purchases': False
        }
    )

    download_purchases = PythonOperator(
        task_id='download_purchases',
        python_callable=fetch_and_process_data,
        op_kwargs={
            'endpoint': 'https://mnpwhdbcsk.us-east-2.awsapprunner.com/api/purchases',
            'gcs_path': 'etl',
            'is_purchases': True
        }
    )

    load_products_bq = GCSToBigQueryOperator(
        task_id='load_products_bq',
        bucket=BUCKET_NAME,
        source_objects=['etl/products.json'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.products",
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    load_purchases_bq = GCSToBigQueryOperator(
        task_id='load_purchases_bq',
        bucket=BUCKET_NAME,
        source_objects=['etl/purchases.json'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.purchases",
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    load_purchase_products_bq = GCSToBigQueryOperator(
        task_id='load_purchase_products_bq',
        bucket=BUCKET_NAME,
        source_objects=['etl/purchase_products.json'],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.purchase_products",
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    [download_products, download_purchases] >> [load_products_bq, load_purchases_bq, load_purchase_products_bq]
