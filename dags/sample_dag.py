# Imports related to airflow
import os
import base64
import airflow
from airflow import DAG
from airflow import models
import datetime
from airflow.models import Variable
from airflow.operators import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators import gcs_to_bq
from google.cloud import storage
from google.cloud import bigquery


################################### Dag initialization End ##############################

with models.DAG(
    "loader_dag",
    schedule_interval=None, 
    start_date=airflow.utils.dates.days_ago(1)) as dag:

	# start task
	task_start = DummyOperator(task_id='start', do_xcom_push=False, dag=dag)


	# Below task will read the file from GCS and load it into BQ raw table.
	gcs_to_bq_load = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
	    task_id='gcs_to_bq_load',
	    bucket='sample_data_1234',  # bucket name
	    source_objects=['input/weather_data_2021_*.csv'],  # source file location
	    destination_project_dataset_table='sample-project-337412.sample_dataset.sample_table',  # destination table details
	    skip_leading_rows=1,  # Skip header
	    field_delimiter=',',  # delimiter
	    source_format='CSV',  # source file format
	    write_disposition='WRITE_APPEND',dag=dag)  # truncate and insert data)


	# Move file to processed location
	move_file = GCSToGCSOperator(
	    task_id="move_file",
	    source_bucket='sample_data_1234',
	    source_object='input/weather_data_2021_*.csv',
	    destination_bucket='sample_data_1234',
	    destination_object='processed/weather_data_2021_',
	    move_object=True,
	    dag=dag
	)

	# #Modified move
	# move_file = GCSToGCSOperator(
	#     task_id="move_file_mod",
	#     source_bucket='sample_data_1234',
	#     source_object=['input/weather_data_2021_*.csv'],
	#     destination_bucket='sample_data_1234',
	#     destination_object='verifychange/weather_data_2021_',
	#     move_object=True,
	#     dag=dag
	# )

	# End Task
	task_end = DummyOperator(task_id='end', trigger_rule="none_failed", do_xcom_push=False, dag=dag)

	##########################################################################
	# Task Scheduling
	task_start >> gcs_to_bq_load >> move_file >> task_end