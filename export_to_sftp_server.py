import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
import time
import timeout_decorator
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.contrib.operators.ssh_operator import SSHOperator


# Set variables
logger = logging.getLogger()
client = bigquery.Client(project=Variable.get("project_id")) 
bucket_name = Variable.get("bucket_name")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(dag_id='export_to_sftp_server',
          default_args=default_args,
          schedule_interval='0 21 * * 0',
          #schedule_interval='*/60 * * * *',
          catchup=False,
          max_active_runs=1,
          concurrency=1,
          )





origen_bucket = "gs://{}".format(bucket_name)

taks_copy_bucket_to_sftp_server_questions = SSHOperator(
    task_id="taks_copy_bucket_to_sftp_server_questions",
    ssh_conn_id="ssh_sftp_server",
    command="cd /home/test_user && gsutil cp -r {} . && pwd".format(origen_bucket),
    dag=dag)

wait_data_processing = ExternalTaskSensor(
        task_id='wait_data_processing',
        external_dag_id='external-save_scoring',
        external_task_id= 'export_from_bq_to_gcs',
        timeout = 400, dag=dag)

wait_data_processing >> taks_copy_bucket_to_sftp_server_questions

#taks_copy_bucket_to_sftp_server_questions.set_upstream(wait_data_processing)