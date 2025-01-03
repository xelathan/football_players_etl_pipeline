from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator, EmrServerlessStartJobOperator, EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from io import BytesIO

import json
import logging
import pandas as pd

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alex.t.tran@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# TODO: For testing purposes, please use the request library to extract data from the API
file_path = "/opt/airflow/mock/mock_football_players"

with open("/opt/airflow/config/football_players_data_config.json", 'r') as f:
    config = json.load(f)


def extract_from_football_api(**kwargs):
    logging.info('Extracting data from football API...')

    players_df = pd.DataFrame()

    # TODO: For testing purposes, please use the request library to extract data from the API
    for league_id in config["LEAGUE_IDS"]:
        pagination = 1
        while True:
            try:
                # obtain data
                with open(f"{file_path}_{pagination}.json", 'r') as f:
                    data = json.load(f)
                
                # handle error
                if 'errors' in data and len(data['errors']) > 0:
                    raise Exception(data['errors'])
                
                # handle pagination
                if 'paging' in data and 'current' in data['paging']:
                    pagination = data['paging']['current']
                else:
                    raise Exception('No pagination data found')
                    
                # handle empty data
                if 'response' in data and len(data) == 0:
                    raise Exception('No data found')
                
                # add data to data-frame
                players_df = pd.concat([players_df, pd.json_normalize(data['response'])], ignore_index=True)

                # check if we have reached the last page
                if 'total' in data['paging'] and data['paging']['total'] == pagination:
                    break
                else:
                    pagination += 1
            except KeyError as e:
                logging.error(f"Key error: {e}")
                raise e
            except FileNotFoundError as e:
                logging.error(f"File not found: {file_path}. Please check the file path.")
                raise e
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON from the file: {file_path}. {e}")
                raise e
            except Exception as e:
                logging.error(f"Error: {data['errors']}")
                raise e
    logging.info(f"Extracted {players_df.shape[0]} records from football API")

    return players_df




def store_to_unprocessed_data_s3(**kwargs):
    ti = kwargs['ti']
    current_timestamp = kwargs['time_started']

    players_df = pd.DataFrame(ti.xcom_pull(task_ids='extract_from_football_api'))

    logging.info(f"Storing {players_df.shape[0]} records to unprocessed data S3...")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    folders = [
        f'football_players_data/biweekly_builds/{current_timestamp}/input/',
        f'football_players_data/biweekly_builds/{current_timestamp}/output/',
        f'football_players_data/biweekly_builds/{current_timestamp}/logs/',
    ]

    for folder in folders:
        if 'input' in folder:
            parquet_buffer = BytesIO()
            players_df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)

            s3_key = f'{folder}football_data.parquet'
            
            s3_hook.load_bytes(
                bytes_data=parquet_buffer.getvalue(),
                key=s3_key,
                bucket_name=config["FOOTBALL_DATA_BUCKET"],
                replace=True
            )
        else:
            s3_hook.load_bytes(
                bytes_data=b'',
                key=folder,
                bucket_name=config["FOOTBALL_DATA_BUCKET"],
                replace=True
            )


    logging.info('Data stored to unprocessed data S3')

with DAG(
    'football_players_dag',
    default_args=DEFAULT_ARGS,
    description='DAG for football players stats ETL pipeline',
    schedule='0 0 * * 1 4',
    start_date=datetime(2025, 1, 1),
) as dag:
    time_started = datetime.now().strftime('%Y-%m-%d')
    
    extract_football_api = PythonOperator(
        task_id='extract_from_football_api',
        python_callable=extract_from_football_api,
    )

    store_unprocessed_data_s3 = PythonOperator(
        task_id='store_to_unprocessed_data_s3',
        op_kwargs={"time_started": time_started},
        python_callable=store_to_unprocessed_data_s3,
    )

    create_emr_serverless_app = EmrServerlessCreateApplicationOperator(
        release_label=config["EMR_VERSION"],
        task_id="create_emr_serverless_app",
        aws_conn_id="aws_default",
        job_type="SPARK",
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=25,
        config={
            "name": "football-data",
            "applicationConfiguration": [
                 {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.hadoop.fs.s3.s3AccessGrants.enabled": "true",
                        "spark.hadoop.fs.s3.s3AccessGrants.fallbackToIAM": "false",
                    }
                }
            ]
        }
    )

    input_path = f"s3://{config["FOOTBALL_DATA_BUCKET"]}/football_players_data/biweekly_builds/{time_started}/input/"
    output_path = f"s3://{config["FOOTBALL_DATA_BUCKET"]}/football_players_data/biweekly_builds/{time_started}/output/"
    logs_path = f"s3://{config["FOOTBALL_DATA_BUCKET"]}/football_players_data/biweekly_builds/{time_started}/logs/"
    job_path = f"s3://{config["EMR_JOB_BUCKET"]}/football/football_players_job.py"

    start_emr_job = EmrServerlessStartJobOperator(
        task_id="start_emr_football_players_job",
        aws_conn_id='aws_default',
        application_id=create_emr_serverless_app.output,
        execution_role_arn=config["EMR_JOB_ROLE_ARN"],
        job_driver={
            "sparkSubmit": {
                "entryPoint": job_path,
                "entryPointArguments": [
                    "--input_path", input_path,
                    "--output_path", output_path,
                    "--file_format", "parquet"
                ],
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": logs_path,
                }
            }
        },
    )

    delete_emr_serverless_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_emr_serverless_app",
        application_id=create_emr_serverless_app.output,
        aws_conn_id="aws_default",
    )

    extract_football_api >> store_unprocessed_data_s3 >> create_emr_serverless_app \
    >> start_emr_job >> delete_emr_serverless_app
