###############################################################################
# Name: weather_analysis_pipeline.py
# Description: This DAG script orchestrates data processes on Airflow. The flow
#              is designed to send an alert email upon completion or failure.
# Author: Travis Hong
# Repository: https://github.com/TravisH0301/weather_analysis
###############################################################################
from datetime import datetime
from airflow import DAG

from utils.airflow_email import AirflowEmailSender
from airflow.operators.bash import BashOperator


# Instantiate Airflow email sender
email_sender = AirflowEmailSender(
    send_from="airflow-bot@test.com",
    send_to="local-email@test.com"
)


with DAG(
    dag_id="Weather_Analysis",
    default_args={
        "owner": "Travis Hong",
        "start_date": datetime(2023, 11, 12),
        "schedule_interval": "@monthly",
        "retries": 0,
        "on_success_callback": email_sender.dag_complete_alert,
        "on_failure_callback": email_sender.dag_failure_alert
    }
) as dag:
    
    run_this = BashOperator(
        task_id="first_job",
        bash_command="echo 1",
        dag=dag
    )
