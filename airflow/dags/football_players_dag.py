from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator, EmrServerlessStartJobOperator, EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator, S3Hook
from airflow.utils.dates import datetime, timedelta
from airflow.utils.state import State
from io import BytesIO

import json
import logging
import pandas as pd
import requests
import ssl
import http

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# TODO: For testing purposes, please use the request library to extract data from the API
file_path = "/opt/airflow/mock/mock_football_players"

with open("/opt/airflow/config/football_players_data_config.json", 'r') as f:
    config = json.load(f)

def extract_from_football_api(**kwargs):
    logging.info('Extracting data from football API...')

    players_df = pd.DataFrame()

    context = ssl._create_unverified_context()
    conn = http.client.HTTPSConnection("v3.football.api-sports.io", context=context)
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': config["API_FOOTBALL_KEY"]
    }

    # TODO: For testing purposes, please use the request library to extract data from the API
    for league_id in config["LEAGUE_IDS"]:
        pagination = 1
        while True:
            try:
                # obtain data
                # with open(f"{file_path}_{pagination}.json", 'r') as f:
                #     data = json.load(f)

                conn.request("GET", f"/players?league={league_id}&season={config['SEASON']}&page={pagination}", headers=headers)
                res = conn.getresponse()
                json_data = res.read()
                data = json.loads(json_data.decode("utf-8"))
                
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

    num_records = players_df.shape[0]
    logging.info(f"Extracted {num_records} records from football API")
    kwargs['ti'].xcom_push(key='num_records', value=num_records)

    return players_df

def store_to_unprocessed_records_s3(**kwargs):
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

def get_processed_file_path(**kwargs):
    current_timestamp = kwargs['time_started']
    s3_hook = S3Hook(aws_conn_id='aws_default')

    parquet_files = s3_hook.list_keys(
        bucket_name=config["FOOTBALL_DATA_BUCKET"],
        prefix=f"football_players_data/biweekly_builds/{current_timestamp}/output/*.parquet",
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
        Iterator of rows from the parquet file
    """
    df = pd.read_parquet(filepath)
        
    for row in df.values:
        yield row.tolist()

def get_root_failed_task(context):
    """
    Find the root failed task by checking upstream tasks.
    Returns tuple of (task_id, dag_id) of the root failed task.
    """
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()
    
    # Find failed tasks
    failed_tasks = [
        ti for ti in task_instances 
        if ti.state == State.FAILED
    ]
    
    if not failed_tasks:
        # If no failed tasks found, return the current task
        return context['task_instance'].task_id, context['dag'].dag_id
    
    # Sort by start_date to get the first failed task
    failed_tasks.sort(key=lambda x: x.start_date or datetime.max)
    root_task = failed_tasks[0]
    
    return root_task.task_id, root_task.dag_id

def get_task_status_timeline(context):
    """Get status of all tasks in order of execution."""
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()
    
    # Sort tasks by start_date
    task_instances.sort(key=lambda x: x.start_date or datetime.max)
    
    timeline = []
    for ti in task_instances:
        status = "✅" if ti.state == State.SUCCESS else "❌" if ti.state == State.FAILED else "⏳"
        timeline.append(f"{status} {ti.task_id}")
    
    return timeline

def format_elapsed_time(elapsed_timedelta):
    """Format timedelta into HH:MM:SS string."""
    if not elapsed_timedelta:
        return "Duration unknown"
        
    # Get total seconds
    total_seconds = int(elapsed_timedelta.total_seconds())
    
    # Calculate hours, minutes, and seconds
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def on_notify_failure(context):
    """
    Notifies if pipeline failed through slack
    """

    logging.info('Sending failure notification to Slack...')

    failed_task_id, failed_dag_id = get_root_failed_task(context)

    execution_date = context['execution_date']
    exception = context.get('exception')
    
    # Get task timeline
    task_timeline = get_task_status_timeline(context)
    timeline_text = "\n".join(task_timeline)

    data = {
        #Fallback
        "text": "🚨 Airflow Task Failure Alert 🚨",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *Airflow Task Failure Alert* 🚨"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed Task:*\n{failed_task_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed DAG:*\n{failed_dag_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Execution Date:*\n{execution_date.strftime('%Y-%m-%d %H:%M:%S')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Build logs:*\n*<https://console.aws.amazon.com/mwaa|Apache Airflow>*"
                    },
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Task Timeline:*\n{timeline_text}"
                }
            }
        ]
    }

    if exception:
        data["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Details:*\n```{str(exception)}```"
            }
        })

    try:
        response = requests.post(
            config["SLACK_WEBHOOK_URL"],
            headers={'Content-Type': 'application/json'},
            data=json.dumps(data)
        )

        if response.status_code != 200:
            logging.error(f"Failed to send Slack notification: {response.text}")
        else:
            logging.info("Slack notification sent successfully.")
    except Exception as e:
        logging.error(f"Exception while sending Slack notification: {str(e)}")

def on_notify_success(context):
    logging.info('Sending success notification to Slack...')

    dag_id = context['dag'].dag_id
    task_instance = context['task_instance']
    num_records = task_instance.xcom_pull(task_ids='extract_from_football_api', key='num_records')

    start_date = task_instance.start_date
    end_date = task_instance.end_date

    if start_date and end_date:
        elapsed_time = end_date - start_date
        elapsed_time_str = format_elapsed_time(elapsed_time)
    else:
        elapsed_time_str = "Duration unknown"

    data = {
        # Fallback
        "text": ":white_check_mark: Football Player Stats ETL Pipeline DAG {{ ti.dag_id }}" +  
        f"successfully completed for {start_date}! :white_check_mark:",
        "blocks" : [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Football Player Stats ETL Pipeline Completed! ✅",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"DAG: *{dag_id}* has successfully completed:\n\n"
                        f"• *Records Processed:* {num_records}\n"
                        f"• *Elapsed Time:* {elapsed_time_str}\n"
                        "• *Data Source:* *<https://www.api-football.com/|Football API>*\n"
                        "• *Transformed Data Location:* *<https://s3.console.aws.amazon.com/|S3 Bucket>*\n"
                        "• *Loaded into:* *<https://console.aws.amazon.com/redshift/|Amazon Redshift>*\n\n"
                        "For details, visit the *<https://console.aws.amazon.com/mwaa|Apache Airflow>* dashboard on MWAA."
                    ),
                },
                "accessory": {
                    "type": "image",
                    "image_url": "https://cdn-icons-png.flaticon.com/512/5438/5438899.png",
                    "alt_text": "Football Icon",
                },
            },
            {
                "type": "divider",
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Triggered by *Apache Airflow*. Data updated on *{end_date}*.",
                    }
                ],
            },
        ],
    }

    try:
        response = requests.post(
            config["SLACK_WEBHOOK_URL"],
            headers={'Content-Type': 'application/json'},
            data=json.dumps(data)
        )

        if response.status_code != 200:
            logging.error(f"Failed to send Slack notification: {response.text}")
        else:
            logging.info("Slack notification sent successfully.")
    except Exception as e:
        logging.error(f"Exception while sending Slack notification: {str(e)}")

with DAG(
    'football_players_dag',
    default_args=DEFAULT_ARGS,
    description='DAG for football players stats ETL pipeline',
    schedule='0 0 * * 1 4',
    start_date=datetime(2025, 1, 1),
    on_failure_callback=on_notify_failure,
    on_success_callback=on_notify_success,
) as dag:
    time_started = datetime.now().strftime('%Y-%m-%d')
    
    extract_football_api = PythonOperator(
        task_id='extract_from_football_api',
        python_callable=extract_from_football_api,
    )

    store_unprocessed_records_s3 = PythonOperator(
        task_id='store_to_unprocessed_records_s3',
        op_kwargs={"time_started": time_started},
        python_callable=store_to_unprocessed_records_s3,
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
            "runtimeConfiguration": [
                 {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.hadoop.fs.s3a.s3AccessGrants.enabled": "true",
                        "spark.hadoop.fs.s3a.s3AccessGrants.fallbackToIAM": "false",
                    }
                }
            ],
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
                    "--input-path", input_path,
                    "--output-path", output_path,
                    "--format", "parquet"
                ]
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

    get_processed_file_path_name = PythonOperator(
        task_id="get_processed_file_path_name",
        op_kwargs={"time_started": time_started},
        python_callable=get_processed_file_path,
    )

    create_table_if_needed = SQLExecuteQueryOperator(
        task_id='create_table_if_needed',
        sql="""
            CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
                player_id SERIAL PRIMARY KEY,
                player_name VARCHAR(255),
                player_firstname VARCHAR(255),
                player_lastname VARCHAR(255),
                player_age INT,
                player_birth_date VARCHAR(255),
                player_birth_place VARCHAR(255),
                player_birth_country VARCHAR(255),
                player_nationality VARCHAR(255),
                player_height VARCHAR(255),
                player_weight VARCHAR(255),
                player_injured BOOLEAN,
                player_photo VARCHAR(255),
                cards_red INT,
                cards_yellow INT,
                cards_yellowred INT,
                dribbles_attempts FLOAT,
                dribbles_past INT,
                dribbles_success FLOAT,
                duels_total FLOAT,
                duels_won FLOAT,
                fouls_committed FLOAT,
                fouls_drawn FLOAT,
                games_appearences INT,
                games_captain BOOLEAN,
                games_lineups INT,
                games_minutes INT,
                games_number FLOAT,
                games_position VARCHAR(255),
                games_rating VARCHAR(255),
                goals_assists FLOAT,
                goals_conceded INT,
                goals_saves INT,
                goals_total INT,
                league_country VARCHAR(255),
                league_flag VARCHAR(255),
                league_id INT,
                league_logo VARCHAR(255),
                league_name VARCHAR(255),
                league_season INT,
                passes_accuracy FLOAT,
                passes_key FLOAT,
                passes_total FLOAT,
                penalty_commited FLOAT,
                penalty_missed FLOAT,
                penalty_saved FLOAT,
                penalty_scored FLOAT,
                penalty_won FLOAT,
                shots_on FLOAT,
                shots_total FLOAT,
                substitutes_bench INT,
                substitutes_in INT,
                substitutes_out INT,
                tackles_blocks FLOAT,
                tackles_interceptions FLOAT,
                tackles_total FLOAT,
                team_id INT,
                team_logo VARCHAR(255),
                team_name VARCHAR(255),
                CONSTRAINT unique_player_team_league_season UNIQUE (player_id, team_id, league_id, league_season)
            );
          """,
          params={"table_name": f"football_players_data_{config["SEASON"]}"},
          conn_id="aws_rds_postgres_conn",
          show_return_value_in_logs=True,
    )

    load_processed_records_to_postgres = S3ToSqlOperator(
        task_id="load_to_rds",
        s3_bucket=config["FOOTBALL_DATA_BUCKET"],
        s3_key="{{ ti.xcom_pull(task_ids='get_processed_file_path_name') }}",
        schema='public',
        aws_conn_id="aws_default",
        table=f"football_players_data_{config['SEASON']}",
        sql_conn_id="aws_rds_postgres_conn",
        column_list=[],
        parser=parquet_parser,
    )
    
    extract_football_api >> store_unprocessed_records_s3 >> create_emr_serverless_app \
    >> start_emr_job >> delete_emr_serverless_app >> get_processed_file_path_name \
    >> create_table_if_needed >> load_processed_records_to_postgres
