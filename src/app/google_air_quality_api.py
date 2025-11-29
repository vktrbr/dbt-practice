import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from fastapi import HTTPException
from loguru import logger
from pydantic import BaseModel
from pymongo import MongoClient

load_dotenv()

# Конфигурация MongoDB
MONGO_CONFIG = {
    "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
    "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
    # "host": os.getenv("MONGO_HOST"),
    "host": "0.0.0.0",
    "port": int(os.getenv("MONGO_PORT")),
    "authSource": "admin",
}
logger.info(f"Connecting to MongoDB: {MONGO_CONFIG}")

client = MongoClient(**MONGO_CONFIG)

db = client["air_quality_db"]
collection = db["air_quality_data"]


def save_request_to_mongo(
        url: str,
        request: dict,
        response: dict,
        status_code: int,
        created_at: datetime,
        update_at: datetime
):
    """
    Сохранение запроса и ответа в MongoDB

    :param url:
    :param request:
    :param response:
    :param status_code:
    :param created_at:
    :param update_at:
    :return:
    """

    url_masked = url.split("?")[0]

    collection.insert_one({
        "url": url_masked,
        "request": request,
        "response": response,
        "status_code": status_code,
        "created_at": created_at,
        "update_at": update_at
    })


# Модель для запроса прогноза
class ForecastRequest(BaseModel):
    lat: float
    long: float
    date_time: datetime


# Модель для запроса текущего состояния
class CurrentRequest(BaseModel):
    lat: float
    long: float


def get_forecast_air_quality_data(forecast_request: ForecastRequest) -> dict:
    """
    Get air quality forecast data

    :param forecast_request:
    :return:
    """

    data = get_air_quality_data(
        forecast_request.lat,
        forecast_request.long,
        forecast_request.date_time
    )

    return data


def get_current_air_quality_data(current_request: CurrentRequest) -> dict:
    """
    Get air quality current data

    :param current_request:
    :return:
    """

    data = get_air_quality_data(
        current_request.lat,
        current_request.long
    )

    return data


def get_air_quality_data(lat: float, long: float, date_time: datetime = None):
    """
    Функция для получения данных от Google API

    :param lat:
    :param long:
    :param date_time:
    :return:
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    date_time = date_time.strftime("%Y-%m-%dT%H:%M:%SZ") if date_time else None

    if date_time:
        url = f"https://airquality.googleapis.com/v1/forecast:lookup?key={api_key}"
        params = {
            "location": {
                "latitude": lat,
                "longitude": long
            },
            "dateTime": date_time
        }
    else:
        url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={api_key}"
        params = {
            "location": {
                "latitude": lat,
                "longitude": long
            }
        }

    logger.info("Sending request to Google API")
    created_at = datetime.now()
    response = requests.post(url, json=params, timeout=60)  # Запрос к Google API
    updated_at = datetime.now()

    logger.info("Saving request to MongoDB")
    save_request_to_mongo(
        url,
        params,
        response.json() if response.status_code == 200 else response.text,
        response.status_code,
        created_at,
        updated_at
    )

    if response.status_code != 200:
        logger.error(f"Error fetching data from Google API: {response.text}")
        raise HTTPException(status_code=response.status_code,
                            detail="Error fetching data from Google API")

    return response.json()


if __name__ == "__main__":
    # Пример использования функции сохранения
    # save_request_to_mongo(
    #     url="/example?param=value",
    #     request={"param": "value"},
    #     response={"result": "success"},
    #     status_code=200,
    #     created_at=datetime.now(),
    #     update_at=datetime.now()
    # )

    import json

    # res = collection.find(
    #     {"status_code_2": 200}
    # ).to_list()
    # print(json.dumps(res, indent=4, default=str))

    # Пример использования функции получения данных
    data = get_air_quality_data(37.7749, -122.4194)
    print(json.dumps(data, indent=4))
