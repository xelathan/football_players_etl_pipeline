from airflow.utils.state import State
from airflow.utils.dates import datetime

import logging
import requests
import json

with open("/opt/airflow/dags/football_players/football_players_data_config.json", 'r') as f:
    config = json.load(f)

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