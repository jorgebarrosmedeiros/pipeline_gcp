import os
from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "datapipeline-366715")
BUCKET_1 = os.environ.get("GCP_GCS_BUCKET_1", "data-pipeline-stack-combustiveis-brasil-curated_08102022")
GCS_ACL_ENTITY = os.environ.get("GCS_ACL_ENTITY", "allUsers")
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"


with models.DAG(
    "example_gcs",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:

    list_buckets = GCSListObjectsOperator(task_id="list_buckets", bucket=BUCKET_1)

    list_buckets
