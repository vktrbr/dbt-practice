from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from connector__mongo_postgres_logic import move_data_to_postgres

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 29),
    'retries': 0,
}

dag = DAG(
    'connector__mongo_postgres',
    default_args=default_args,
    description='Utilise weather API',
    schedule_interval='@daily',
)

with dag:
    connector_task = PythonOperator(
        task_id='connector_task',
        python_callable=move_data_to_postgres,
    )

    _ = connector_task
