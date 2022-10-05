import logging
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
import time
import timeout_decorator
from airflow.utils.dates import days_ago

# Set variables
LOCAL_TZ = pytz.timezone('Etc/GMT+4')
logger = logging.getLogger()
client = bigquery.Client(project=Variable.get("project_id")) 


@timeout_decorator.timeout(300, use_signals=False)
def load_bucket_to_bigquery_answers(**kwargs):
    try:
        # get variables 
        dataset_answers = Variable.get("dataset_answers")
        bucket_answars = Variable.get("bucket_answers")
        table_answers = Variable.get("table_answers")

        # 
        table_ref = client.dataset(dataset_answers).table(table_answers)
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        
        job_config.source_format = bigquery.SourceFormat.CSV
       
        load_job = client.load_table_from_uri(
            bucket_answars, table_ref, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        print("Loaded {} rows.".format(destination_table.num_rows))
       
    except Exception as e:
        logger.error('Error loading bucket answars')
        logger.error(e)
        raise ValueError('Error loading bucket answars')

@timeout_decorator.timeout(300, use_signals=False)
def load_bucket_to_bigquery_questions(**kwargs):
    try:
        # get variables 
        dataset_questions = Variable.get("dataset_questions")
        bucket_questions = Variable.get("bucket_questions")
        table_questions = Variable.get("table_questions")

        # 
        table_ref = client.dataset("{}".format(dataset_questions)).table("{}".format(table_questions))
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        
        job_config.source_format = bigquery.SourceFormat.CSV
       
        load_job = client.load_table_from_uri(
            bucket_questions, table_ref, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        print("Loaded {} rows.".format(destination_table.num_rows))
       
    except Exception as e:
        logger.error('Error loading bucket questions')
        logger.error(e)
        raise ValueError('Error loading bucket questions')

@timeout_decorator.timeout(300, use_signals=False)
def load_bucket_to_bigquery_tags(**kwargs):
    try:
        # get variables 
        dataset_tags = Variable.get("dataset_tags")
        bucket_tags = Variable.get("bucket_tags")
        table_tags = Variable.get("table_tags")
        print("dataset_tags {}".format(dataset_tags))
        # 
        table_ref = client.dataset(dataset_tags).table(table_tags)
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.skip_leading_rows = 1
        # The source format defaults to CSV, so the line below is optional.
        
        job_config.source_format = bigquery.SourceFormat.CSV
       
        load_job = client.load_table_from_uri(
            bucket_tags, table_ref, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table_ref)
        print("Loaded {} rows.".format(destination_table.num_rows))
       
    except Exception as e:
        logger.error('Error loading bucket tags')
        logger.error(e)
        raise ValueError('Error loading bucket tags')
    

        

###################################
# Define DAG
###################################
DAG_DEFAULT_ARGS = {
	'owner': 'airflow',
	'depend_on_past': True,
	'start_date': days_ago(2),
	'retries': 0,
	'retry_delay': timedelta(seconds=4),
    'dagrun_timeout':timedelta(seconds=60)
}

dag = DAG('load_cloud_storage_to_bigquery_v2',
          default_args=DAG_DEFAULT_ARGS,
          catchup=False,
          max_active_runs=1,
          concurrency=1,
          #schedule_interval='*/1 * * * *'
          schedule_interval='@once'
          )

###################################
# Define Tasks
###################################

end = DummyOperator(task_id="load Complete",
                    trigger_rule="all_success",
                    dag=dag)

start = DummyOperator(task_id="Start",
                      dag=dag)

load_bucket_to_bigquery_questions = PythonOperator(
    task_id='load_bucket_to_bigquery_questions',
    provide_context=True,
    python_callable=load_bucket_to_bigquery_questions,
    dag=dag)

load_bucket_to_bigquery_answers = PythonOperator(
    task_id='load_bucket_to_bigquery_answers',
    provide_context=True,
    python_callable=load_bucket_to_bigquery_answers,
    dag=dag)

load_bucket_to_bigquery_tags = PythonOperator(
    task_id='load_bucket_to_bigquery_tags',
    provide_context=True,
    python_callable=load_bucket_to_bigquery_tags,
    dag=dag)

start >> [load_bucket_to_bigquery_questions, load_bucket_to_bigquery_answers, load_bucket_to_bigquery_tags] >> end