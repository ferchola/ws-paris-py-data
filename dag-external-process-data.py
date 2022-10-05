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
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators import bash_operator

logger = logging.getLogger()
client = bigquery.Client(project=Variable.get("project_id")) 
bucket_name = Variable.get("bucket_name")

def export_from_bq_to_gcs(**kwargs):
    try:
        client = bigquery.Client(project=Variable.get("project_id")) 
        
        project = Variable.get("project_id")
        dataset_id = Variable.get("dataset_result")
        table_id = Variable.get("table_result")

        destination_uri = "gs://{}/{}.csv".format(bucket_name, table_id)
        dataset_ref = client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location="US",
        )  # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
        )
    except Exception as e:
        logger.error(e)
        raise ValueError('Error exporintg table')


DAG_DEFAULT_ARGS = {
    'owner': 'Airflow',
	'depend_on_past': True,
    "start_date": days_ago(1),
}
dag = DAG(
    dag_id="external-save_scoring",
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval='0 21 * * 0',
    #schedule_interval='*/60 * * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

query = Variable.get("query")
project_id = Variable.get("project_id")
dataset_result = Variable.get("dataset_result")
table_result = Variable.get("table_result")
table_id = "{}:{}.{}".format(project_id,dataset_result,table_result) 
sql = """{}""".format(query)
templated_command = """
bq query --destination_table {} --replace --use_legacy_sql=false '{}'
""".format(table_id,sql)

export_from_bq_to_gcs = PythonOperator(
    task_id='export_from_bq_to_gcs',
    provide_context=True,
    python_callable=export_from_bq_to_gcs,
    dag=dag)

save_scoring = bash_operator.BashOperator(
        task_id='save_scoring',
        bash_command=templated_command,
         dag=dag)


wait_dag_file_upload = ExternalTaskSensor(
        task_id='wait_dag_file_upload',
        external_dag_id='load_cloud_storage_to_bigquery_v2',
        external_task_id= 'change-completed',
       # execution_delta = timedelta(minutes=5),
        timeout = 200, dag=dag)

wait_dag_file_upload >> save_scoring >> export_from_bq_to_gcs

#save_scoring >> export_from_bq_to_gcs