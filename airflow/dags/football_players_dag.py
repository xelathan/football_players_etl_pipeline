from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from io import BytesIO

import json
import logging
import pandas as pd

file_path = "/opt/airflow/mock/mock_football_players"

league_ids = [
    39, # Premier League
    140, # La Liga
    78, # Serie A
    61, # Ligue 1
    135, # Bundesliga
]


def extract_from_football_api(**kwargs):
    logging.info('Extracting data from football API...')

    players_df = pd.DataFrame()

    for league_id in league_ids:
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
    players_df = pd.DataFrame(ti.xcom_pull(task_ids='extract_from_football_api'))

    logging.info(f"Storing {players_df.shape[0]} records to unprocessed data S3...")

    bucket_name = 'api-football'
    s3_hook = S3Hook(aws_conn_id='aws_default')

    current_timestamp = datetime.now().strftime('%Y-%m-%d')
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
                bucket_name=bucket_name,
                replace=True
            )
        else:
            s3_hook.load_bytes(
                bytes_data=b'',
                key=folder,
                bucket_name=bucket_name,
                replace=True
            )


    logging.info('Data stored to unprocessed data S3')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alex.t.tran@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'football_players_dag',
    default_args=default_args,
    description='DAG for football players stats ETL pipeline',
    schedule='0 0 * * 1 4',
    start_date=datetime(2025, 1, 1),
)

start = PythonOperator(
    task_id='extract_from_football_api',
    dag=dag,
    python_callable=extract_from_football_api,
)

end = PythonOperator(
    task_id='store_to_unprocessed_data_s3',
    dag=dag,
    python_callable=store_to_unprocessed_data_s3,
)

start >> end
