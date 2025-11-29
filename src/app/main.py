from fastapi import FastAPI

from google_air_quality_api import (
    ForecastRequest,
    get_forecast_air_quality_data,
    CurrentRequest,
    get_current_air_quality_data
)

app = FastAPI()


@app.post("/forecast")
async def get_forecast(request: ForecastRequest):
    """
    Ручка для получения прогноза качества воздуха
    """
    data = get_forecast_air_quality_data(request)
    return data


@app.post("/current")
async def get_current(request: CurrentRequest):
    """
    Ручка для получения текущего состояния качества воздуха
    """
    data = get_current_air_quality_data(request)
    return data


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8081, reload=True, workers=1)
