from airflow.providers.amazon.aws.transfers.s3_to_sql import S3Hook

import json
import pandas as pd

with open("/opt/airflow/dags/football_players/football_players_data_config.json", 'r') as f:
    config = json.load(f)

def get_processed_file_path(**kwargs):
    """
    
    """
    dated_started = kwargs['ds']
    s3_hook = S3Hook(aws_conn_id='aws_default')

    parquet_files = s3_hook.list_keys(
        bucket_name=config["FOOTBALL_DATA_BUCKET"],
        prefix=f"football_players_data/biweekly_builds/{dated_started}/output/*.parquet",
        delimiter="/",
        apply_wildcard=True,
    )

    if not parquet_files:
        raise Exception("No matching parquet file found")

    return parquet_files[0]

def parquet_parser(filepath):
    """
    Parser function for S3ToSqlOperator to handle parquet files.
    Args:
        filepath: Path to the parquet file in S3 (downloaded to local temp storage by operator)
    Returns:
        Iterator of rows from the parquet file, where each row corresponds to a single database record.
    """
    # Load the parquet file into a DataFrame
    df = pd.read_parquet(filepath)
    
    # Convert each row of the DataFrame into a list and yield
    for _, row in df.iterrows():
        yield row.tolist()