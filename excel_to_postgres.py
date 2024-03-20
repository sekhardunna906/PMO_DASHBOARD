import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from pytz import timezone
from datetime import datetime, timedelta

# Set the IST timezone
ist_timezone = timezone('Asia/Kolkata')

default_args = {
    'owner': 'Sekhar Dunna',
    'start_date': ist_timezone.localize(datetime(2023, 1, 1, 16, 0, 0)),  # Start date and time in IST
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="excel_to_postgres", 
    default_args=default_args,
    schedule_interval='0 16 * * *',
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    excel_to_postgres = AirbyteTriggerSyncOperator( 
        task_id='excel_to_postgres',
        airbyte_conn_id='airbyte_connection_excel',
        connection_id='0dad5067-499d-42ba-b829-f5128bf44a42',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    end = DummyOperator(task_id='end')


    start >> excel_to_postgres >> end

