from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from loguru import logger

# Barcelona coordinates
LAT = 41.3896
LONG = 2.1591
HOST = "host.docker.internal"


def task_trigger_forecast():
    """
    Функция для триггера получения прогноза погоды на ближайшие 3 дня
    json_data = {
        'lat': 41.3896,
        'long': 2.1591,
        'date_time': '2025-02-11T11:58:47.384Z',
    }

    response = requests.post('http://{HOST}:8081/forecast', ...)

    :return:
    """
    for i in range(1, 4):
        date = datetime.now() + timedelta(days=i)
        logger.info(f"Getting forecast for day {date}")

        json_data = {
            'lat': LAT,
            'long': LONG,
            'date_time': date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        response = requests.post(f"http://{HOST}:8081/forecast", json=json_data)

        if response.status_code != 200:
            logger.error(f"Failed to get forecast for day {date}")
            return False

        logger.info(f"Forecast for day {date}: {response.json()}")

    return True


def task_trigger_current_conditions():
    """
    Функция для триггера получения текущих погодных условий
    json_data = {
        'lat': 41.3896,
        'long': 2.1591,
    }

    response = requests.post('http://{HOST}:8081/current', ...)
    """

    json_data = {
        'lat': LAT,
        'long': LONG,
    }

    response = requests.post(f"http://{HOST}:8081/current", json=json_data)

    if response.status_code != 200:
        logger.error(f"Failed to get current conditions")
        return False

    logger.info(f"Current conditions: {response.json()}")

    return True


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 29),
    'retries': 0,
}

dag = DAG(
    'weather_api',
    default_args=default_args,
    description='Utilise weather API',
    schedule_interval='0 */6 * * *'  # every 6 hours = 4 times a day
)

with dag:
    task_forecast = PythonOperator(
        task_id='task_trigger_forecast',
        python_callable=task_trigger_forecast,
    )

    task_current_conditions = PythonOperator(
        task_id='task_trigger_current_conditions',
        python_callable=task_trigger_current_conditions,
    )

    # Параллельное выполнение задач
    _ = [
        task_forecast,
        task_current_conditions,
    ]

    # Последовательное выполнение задач
    # task_forecast >> task_current_conditions
