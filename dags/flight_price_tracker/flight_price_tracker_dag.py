import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import data_pipeline
import email_utils
import elasticsearch_utils
from config import EMAIL_RECIPIENT

with DAG(
    "flight_price_tracker",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    description="Ingestion task to fetch data",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2025, 3, 1),
    catchup=False,
    max_active_runs=3,
) as flight_price_dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=data_pipeline.fetch_data,
    )
    prepare_price_alerts_task = PythonOperator(
        task_id="prepare_price_alerts",
        python_callable=data_pipeline.check_for_price_alerts,
    )
    index_data_task = PythonOperator(
        task_id="index_data",
        python_callable=elasticsearch_utils.ElasticsearchConnection.index_data,
    )
    should_continue_task = ShortCircuitOperator(
        task_id="should_continue",
        python_callable=data_pipeline.should_send_email,
    )
    generate_email_content = PythonOperator(
        task_id="generate_email_content", python_callable=email_utils.prepare_email_body
    )
    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_RECIPIENT,
        subject="Price Alert for Cheap Tickets",
        html_content="{{ ti.xcom_pull(task_ids='generate_email_content') }}",
    )
    (
        fetch_data_task
        >> prepare_price_alerts_task
        >> index_data_task
        >> should_continue_task
        >> generate_email_content
        >> send_email
    )
