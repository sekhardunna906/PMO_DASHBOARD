import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from pytz import timezone
from datetime import datetime, timedelta

# Set the IST timezone
ist_timezone = timezone('Asia/Kolkata')

default_args = {
    'owner': 'Airflow',
    'start_date': ist_timezone.localize(datetime(2023, 1, 1, 17, 0, 0)),  # Start date and time in IST
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="PMO_Dashboard_Resource_Utilization", 
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    PMO_Resource_Utilization_API_to_Bronze = AirbyteTriggerSyncOperator( 
        task_id='PMO_Resource_Utilization_API_to_Bronze',
        airbyte_conn_id='airbyte_connection',
        connection_id='8e1ff4fb-f694-4cd3-b44d-b0c40d6c28ea',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )


    PMO_Resource_Utilization_Silver_to_Gold = AirbyteTriggerSyncOperator(
        task_id='PMO_Resource_Utilization_Silver_to_Gold',
        airbyte_conn_id='airbyte_connection',
        connection_id='be35b8b1-cd89-473b-a035-b552dbf32401',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )


    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    dbt_project_path = f'{AIRFLOW_HOME}/plugins/dbt/pmo_dashboard/'

    bash_cmd = f'cd {dbt_project_path} && dbt run'


    PMO_Resource_Utilization_Bronze_to_silver = BashOperator(
    task_id='PMO_Resource_Utilization_Bronze_to_silver',
    bash_command=bash_cmd,
    dag=dag,)

    end = DummyOperator(task_id='end')


    start  >> PMO_Resource_Utilization_API_to_Bronze >> \
    PMO_Resource_Utilization_Bronze_to_silver >> \
    PMO_Resource_Utilization_Silver_to_Gold >> end

 
