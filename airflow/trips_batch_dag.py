import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from google.cloud import storage

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
PROJECT_ID= 'zoom-camp-me'
BUCKET = 'yellow_data_lake_zoom-camp-me'

# Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='yellow_taxi_data_v1',
    schedule_interval='0 6 2 * *', #cron: “At 06:00 on day-of-month 2.”
    start_date=datetime(2021, 3, 1),
    end_date=datetime(2021, 4, 1),
    max_active_runs= 3,
    default_args=default_args,
) as dag:

    URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    YELLOW_FILE_TEMPLATE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
    URL_TEMPLATE = URL_PREFIX + YELLOW_FILE_TEMPLATE
    GCS_PATH_TEMPLATE = 'yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/' + YELLOW_FILE_TEMPLATE
    ZONES_FILE= 'taxi_zone_lookup.csv'

    download_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'curl -sSL {URL_TEMPLATE}  > {AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}'
    )
    
    gcs_task = PythonOperator(
        task_id='upload_local_to_gcs_task',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'object_name': GCS_PATH_TEMPLATE,
            'local_file': f'{AIRFLOW_HOME}/{YELLOW_FILE_TEMPLATE}',
        },
    )
    
    PYSPARK_REGION='us-central1'
    PYSPARK_CLUSTER_NAME= 'cluster-bf11'
    PYSPARK_FILE= f'gs://{BUCKET}/dataproc.py'
    PYSPARK_UTILS_FILE= f'gs://{BUCKET}/dataproc_utils.py'
    PYSPARK_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': PYSPARK_CLUSTER_NAME},
    'pyspark_job': {
    'main_python_file_uri': PYSPARK_FILE,
    "python_file_uris": [PYSPARK_UTILS_FILE],
    'args': [
    '--yellow=' + GCS_PATH_TEMPLATE,
    '--zones=' + ZONES_FILE,
    '--out=new1_yellow_results/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%m\') }}',
    '--bucket=' + 'gs://'+ BUCKET
    ]}}

    pyspark_task = DataprocSubmitJobOperator(
    task_id='pyspark_task',
    job=PYSPARK_JOB,
    region=PYSPARK_REGION,
    project_id=PROJECT_ID
    )
    
    bq_task= GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=BUCKET,
        source_objects=['new1_yellow_results/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%m\') }}/*.parquet'],
        source_format= 'PARQUET',
        destination_project_dataset_table= PROJECT_ID +'.yellow_taxi.new_yellow_trips',
        write_disposition='WRITE_APPEND'
    )
    
    
    download_task >> gcs_task >> pyspark_task >> bq_task
