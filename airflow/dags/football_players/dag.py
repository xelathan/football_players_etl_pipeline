from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator, EmrServerlessStartJobOperator, EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.utils.dates import datetime, timedelta

from football_players.ingestion import extract_from_football_api, store_to_unprocessed_records_s3
from football_players.loading import get_processed_file_path, parquet_parser
from football_players.monitoring import on_notify_failure, on_notify_success

from football_players.sql import CREATE_PRODUCTION_TABLE_IF_NEEDED, CREATE_STAGING_TABLE, MERGE_STAGING_WITH_PRODUCTION, DROP_STAGING_TABLE

import json

with open("/opt/airflow/dags/football_players/football_players_data_config.json", 'r') as f:
    config = json.load(f)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'football_players_dag',
    default_args=DEFAULT_ARGS,
    description='DAG for football players stats ETL pipeline',
    schedule='0 0 * * 1 4',
    start_date=datetime(2025, 1, 1),
    on_failure_callback=on_notify_failure,
    on_success_callback=on_notify_success,
) as dag:   
    extract_football_api = PythonOperator(
        task_id='extract_from_football_api',
        python_callable=extract_from_football_api,
    )

    store_unprocessed_records_s3 = PythonOperator(
        task_id='store_to_unprocessed_records_s3',
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
        config=config["EMR_SERVERLESS_APP_CONFIG"]
    )

    dated_started = "{{ ds }}" 
    start_emr_job = EmrServerlessStartJobOperator(
        task_id="start_emr_football_players_job",
        aws_conn_id='aws_default',
        application_id=create_emr_serverless_app.output,
        execution_role_arn=config["EMR_JOB_ROLE_ARN"],
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{config['EMR_JOB_BUCKET']}/football/football_players_job.py",
                "entryPointArguments": [
                    "--input-path", f"s3://{config['FOOTBALL_DATA_BUCKET']}/football_players_data/biweekly_builds/{dated_started}/input/",
                    "--output-path", f"s3://{config['FOOTBALL_DATA_BUCKET']}/football_players_data/biweekly_builds/{dated_started}/output/",
                    "--format", "parquet"
                ]
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{config['FOOTBALL_DATA_BUCKET']}/football_players_data/biweekly_builds/{dated_started}/logs/",
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
        python_callable=get_processed_file_path,
    )

    create_table_if_needed = SQLExecuteQueryOperator(
        task_id='create_table_if_needed',
        sql=CREATE_PRODUCTION_TABLE_IF_NEEDED,
          params={"table_name": f"football_players_data_{config["SEASON"]}"},
          conn_id="aws_rds_postgres_conn",
          show_return_value_in_logs=True,
    )

    create_staging_table = SQLExecuteQueryOperator(
        task_id='create_staging_table',
        sql=CREATE_STAGING_TABLE,
        conn_id="aws_rds_postgres_conn",
        show_return_value_in_logs=True,
    )

    load_processed_records_to_staging_table = S3ToSqlOperator(
        task_id="load_to_rds",
        s3_bucket=config["FOOTBALL_DATA_BUCKET"],
        s3_key="{{ ti.xcom_pull(task_ids='get_processed_file_path_name') }}",
        schema='public',
        aws_conn_id="aws_default",
        table="staging_table",
        sql_conn_id="aws_rds_postgres_conn",
        column_list=[],
        parser=parquet_parser,
    )

    # merge tables
    merge_staging_with_production_table = SQLExecuteQueryOperator(
        task_id="merge_staging_with_production_table",
        sql=MERGE_STAGING_WITH_PRODUCTION,
        conn_id="aws_rds_postgres_conn",
        params={"production_table": f"football_players_data_{config["SEASON"]}"},
        show_return_value_in_logs=True,
    )


    delete_staging_table = SQLExecuteQueryOperator(
        task_id='delete_staging_table',
        sql=DROP_STAGING_TABLE,
        conn_id="aws_rds_postgres_conn",
        show_return_value_in_logs=True,
    )
    
    extract_football_api >> store_unprocessed_records_s3 >> create_emr_serverless_app \
    >> start_emr_job >> delete_emr_serverless_app >> get_processed_file_path_name \
    >> create_table_if_needed >> create_staging_table >> load_processed_records_to_staging_table \
    >> merge_staging_with_production_table >> delete_staging_table
    
