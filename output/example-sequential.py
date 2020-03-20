# datatime lib
from datetime import datetime, timedelta
from time import time
# logging lib
import logging
# to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
#from airflow.utils import trigger_rule
# to get DAG operators
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# import google dataproc operators
from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator,
                                                        DataProcSparkOperator, 
                                                        DataprocClusterDeleteOperator
                                                        )
# use airflow sensor to check if the file exists or not
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
# 
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# specifiy default args
default_args = {
    'project_id': Variable.get('project_id', default_var=None),
    'depends_on_past': False,
    'email': ['test@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# specify global variable cleaned_dag_id
cleaned_dag_id = 'example-sequential'.strip()

# include xcoms to allow tasks exchange messages & share states through xcom_push & xcom_pull
def push_unique_cluster_name(**kwargs):
    """Pushes an XCOM without return value
    develop a unique cluster name to avoid name collision
    """
    unique_cluster_name = cleaned_dag_id + '-' + str(int(time() * 10000 % 10000))
    ti = kwargs['ti']
    ti.xcom_push(key='unique_cluster_name', value=unique_cluster_name)

# ==========
# == DAG ===
# ==========
# instantiate a dag
dag = DAG(
    dag_id=cleaned_dag_id,
    description='run DAGs sequentially',
    start_date= datetime(2020, 3, 15),
    schedule_interval='@daily',
    concurrency=10,
    max_active_runs=1,
    default_args=default_args
)

# ============
# == Tasks ===
# ============

# unique tasks
push_unique_cluster_name = PythonOperator(
    task_id='generate_unique_cluster_name',
    provide_context=True,
    python_callable=push_unique_cluster_name,
    dag=dag
)

# replicate tasks (dataproc_list)

create_cluster_1 = DataprocClusterCreateOperator(
    task_id='create_cluster_1',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '1',
    project_id=Variable.get('project_id', default_var=None),
    region='us-west1',
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    num_workers=2,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

delete_cluster_1 = DataprocClusterDeleteOperator(
    task_id='delete_cluster_1',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '1',
    region='us-west1',
    project_id=Variable.get('project_id', default_var=None),
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

create_cluster_2 = DataprocClusterCreateOperator(
    task_id='create_cluster_2',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '2',
    project_id=Variable.get('project_id', default_var=None),
    region='us-west1',
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    num_workers=2,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

delete_cluster_2 = DataprocClusterDeleteOperator(
    task_id='delete_cluster_2',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '2',
    region='us-west1',
    project_id=Variable.get('project_id', default_var=None),
    execution_timeout=timedelta(minutes=20),
    dag=dag
)



# =================
# == Spark Jobs ===
# =================


# define arguments
args = ["--args.for.jar", "ThisIsArgs"]

calc_users = DataProcSparkOperator(
    task_id='calc_users',
    dataproc_spark_jars=['gs://exampleBucket/jar/yourProject-assembly-0.1.jar'],
    main_class='yourProject.com.ActionProcess',
    region='us-west1',
    job_name=cleaned_dag_id + 'calc_users',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '1',
    execution_timeout=timedelta(hours=2),
    arguments=args,
    dag=dag
)

# define arguments
args = ["--args.for.jar", "ThisIsArgs"]

calc_agg = DataProcSparkOperator(
    task_id='calc_agg',
    dataproc_spark_jars=['gs://exampleBucket/jar/yourProject-assembly-0.1.jar'],
    main_class='yourProject.com.ActionProcess',
    region='us-west1',
    job_name=cleaned_dag_id + 'calc_agg',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '1',
    execution_timeout=timedelta(hours=2),
    arguments=args,
    dag=dag
)

# define arguments
args = ["--args.for.jar", "ThisIsArgs"]

calc_retention_day1 = DataProcSparkOperator(
    task_id='calc_retention_day1',
    dataproc_spark_jars=['gs://exampleBucket/jar/yourProject-assembly-0.1.jar'],
    main_class='yourProject.com.ActionProcess',
    region='us-west1',
    job_name=cleaned_dag_id + 'calc_retention_day1',
    cluster_name='{{ ti.xcom_pull(key=unique_cluster_name, task_ids="generate_unique_cluster_name") }}' + '2',
    execution_timeout=timedelta(hours=2),
    arguments=args,
    dag=dag
)



# ==================
# == gcs sensors ===
# ==================



sensor_task = GoogleCloudStorageObjectSensor(
    task_id='sensor_task',
    bucket='exampleBucket',
    object='output/user/_SUCCESS',
    poke_interval=30,
    timeout=2700,
    dag=dag
)




# =======================
# == load to bigquery ===
# =======================



bq_load_user = GoogleCloudStorageToBigQueryOperator(
    task_id='bq_load_user',
    bucket='exampleBucket',
    source_objects=["obj_folder/*"]
,
    source_format='NEWLINE_DELIMITED_JSON',
    destination_project_dataset_table='exampleSchema.exampleTable0',
    schema_fields=[
  {'name':'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'first_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
]
,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

bq_load_agg = GoogleCloudStorageToBigQueryOperator(
    task_id='bq_load_agg',
    bucket='exampleBucket',
    source_objects=["obj_folder/*"]
,
    source_format='NEWLINE_DELIMITED_JSON',
    destination_project_dataset_table='exampleSchema.exampleTable1',
    schema_fields=[
  {'name':'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'start_station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'end_station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'zip_code', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'avg_ec', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
]
,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

bq_load_retention = GoogleCloudStorageToBigQueryOperator(
    task_id='bq_load_retention',
    bucket='exampleBucket',
    source_objects=["obj_folder/*"]
,
    source_format='NEWLINE_DELIMITED_JSON',
    destination_project_dataset_table='exampleSchema.exampleTable2',
    schema_fields=[
  {'name':'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'start_station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'end_station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'zip_code', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name':'retention', 'type': 'INTEGER', 'mode': 'NULLABLE'},
]
,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    dag=dag
)



# =================
# == tasks flow ===
# =================

# dataproc upstream & downstream for both create and delete dataproc
create_cluster_1.set_upstream(push_unique_cluster_name)
create_cluster_1.set_upstream(sensor_task)
create_cluster_2.set_upstream(push_unique_cluster_name)
create_cluster_2.set_upstream(delete_cluster_1)
# create job upstream & downstream
calc_users.set_upstream(create_cluster_1)
calc_users.set_downstream(bq_load_user)
calc_users.set_downstream(calc_agg)
calc_agg.set_downstream(bq_load_agg)
calc_retention_day1.set_upstream(create_cluster_2)
calc_retention_day1.set_downstream(bq_load_retention)
# create cfs_to_bq upstream & downstream
bq_load_agg.set_downstream(delete_cluster_1)
bq_load_retention.set_downstream(delete_cluster_2)
