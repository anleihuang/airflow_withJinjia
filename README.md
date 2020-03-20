# Apache Airflow DAG - Template with Jinja2

## Introduction
An ETL pipeline is designed in Google Cloud Platform to read files from Google Cloud Storage, process the data in Apache Spark and load the processed results to Google Big Query. To schedule this ETL pipeline, a workflow management platform - Apache Airflow - is chosen.

However, the DAG tasks developing process can sometime be tedious and repeated. Therefore, Jinja2 (a template engine for Python) is introduced to automate the Airflow DAGs generation process.

The repo provides the DAGs to be generated in both serial and parallel format, depending on the parameters specified in `dag_config.yaml` file.

## DAG Workflow
- Provision a cluster
- Run Spark jobs
- Load results to Google Big Query
- Destroy the cluster

## Implementation
- High level workflow logic and the default parameters are defined in `dataproc-airflow.py.j2` under the airflow_template folder
- User and project specific inputs are defined in `dag_config.yaml` under the config folder
- After finishing the above steps, run the `generate_dag.py` file through the make command `make generate-dag`
- The final DAG files can be found in the output folder

## Testing
- Before uploading the DAG file to GCP's `dags/` folder, be sure to test it locally
    - if the DAGs is in sequential order, run `make load-dag-test-seq`
    - if the DAGs is in parallel order, run `make load-dag-test-par`

## Requirements
- Apache Airflow 1.10
- python 3.6
- Jinja2 2.10.3
