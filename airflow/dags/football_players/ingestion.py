from airflow.providers.amazon.aws.transfers.s3_to_sql import S3Hook
from io import BytesIO

import json
import logging
import pandas as pd
import ssl
import http
import time

# TODO: For testing purposes, please use the request library to extract data from the API
file_path = "/opt/airflow/mock/mock_football_players"

with open("/opt/airflow/dags/football_players/football_players_data_config.json", 'r') as f:
    config = json.load(f)

def extract_from_football_api(**kwargs):
    """
    Extracts football player data from a set of league ids
    Dev: Mock file of json response opened and parsed for player data
    Production: A request is sent to API-Football to get all the players of a league with pagination
    """
    logging.info('Extracting data from football API...')

    players_df = pd.DataFrame()

    context = ssl._create_unverified_context()
    conn = http.client.HTTPSConnection("v3.football.api-sports.io", context=context)
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': config["API_FOOTBALL_KEY"]
    }

    def load_data_from_mock_file(pagination):
        with open(f"{file_path}_{pagination}.json", 'r') as f:
            return json.load(f)
        
    def load_data_from_api(league_id: int, pagination: int):
        conn.request("GET", f"/players?league={league_id}&season={config['SEASON']}&page={pagination}", headers=headers)
        res = conn.getresponse()
        json_data = res.read()
        return json.loads(json_data.decode("utf-8"))

    # TODO: For testing purposes, please use the request library to extract data from the API
    for league_id in config["LEAGUE_IDS"]:
        pagination = 1
        while True:
            try:
                # obtain data
                data = (
                    load_data_from_mock_file(pagination)
                    if config["DEV"]
                    else load_data_from_api(league_id=league_id, pagination=pagination)
                )
                
                # handle error
                if 'errors' in data and len(data['errors']) > 0:
                    logging.info(data['errors'])
                    break
                
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
                
                # time sleep to avoid rate limiting
                time.sleep(1)
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

    num_records = players_df.shape[0]
    if num_records == 0:
        raise Exception("No records found")
    
    logging.info(f"Extracted {num_records} records from football API")
    kwargs['ti'].xcom_push(key='num_records', value=num_records)

    return players_df


def store_to_unprocessed_records_s3(**kwargs):
    """
    Stores unprocessed records to S3.
    Creates folders input, output, logs under a timestamped build
    Writes parquet file of player data to input file
    """

    ti = kwargs['ti']
    current_date = kwargs['ds']

    players_df = pd.DataFrame(ti.xcom_pull(task_ids='extract_from_football_api'))

    logging.info(f"Storing {players_df.shape[0]} records to unprocessed data S3...")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    folders = [
        f'football_players_data/biweekly_builds/{current_date}/input/',
        f'football_players_data/biweekly_builds/{current_date}/output/',
        f'football_players_data/biweekly_builds/{current_date}/logs/',
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