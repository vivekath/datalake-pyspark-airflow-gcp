from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator,    
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="quantum-episode-345713"
BUCKET_NAME = 'udacity_songs_22112025'
CLUSTER_NAME = 'my-demo-cluster2'
REGION = 'us-central1'
PYSPARK_URI = f'gs://{BUCKET_NAME}/spark-job/udacity_etl.py'

def read_sql_from_gcs(bucket, file_path):
    hook = GCSHook()

    # ALWAYS AVAILABLE IN CLOUD COMPOSER
    if hasattr(hook, "read_as_text"):
        return hook.read_as_text(
            bucket_name=bucket,
            object_name=file_path,
            encoding="utf-8"
        )

    # fallback - convert bytes to string
    data = hook.download(bucket_name=bucket, object_name=file_path)
    if isinstance(data, bytes):
        return data.decode("utf-8")
    return data


BRONZE_QUERY = read_sql_from_gcs(BUCKET_NAME, "bq-job/bronze.sql")
SILVER_QUERY = read_sql_from_gcs(BUCKET_NAME, "bq-job/silver.sql")
GOLD_QUERY   = read_sql_from_gcs(BUCKET_NAME, "bq-job/gold.sql")

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    master_disk_size=50,
    worker_disk_size=50,
    image_version="2.0-debian10",
    optional_components=["JUPYTER"],
    enable_component_gateway=True,
    initialization_actions=[
        f"gs://goog-dataproc-initialization-actions-us-east1/connectors/connectors.sh"
    ],
    metadata={
        "bigquery-connector-version": "1.2.0",
        "spark-bigquery-connector-version": "0.21.0",
    }
).make()

default_args = {
    'owner': 'Vivek Athilkar',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}


with DAG('SparkETL', schedule_interval='@once', default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
    )
     # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )
    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )
    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
    )
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
    )

start_pipeline >> create_cluster >> pyspark_task >> bronze_tables >> silver_tables >> gold_tables >> delete_cluster >> finish_pipeline
