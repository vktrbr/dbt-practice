import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# Конфигурация MongoDB
MONGO_CONFIG = {
    "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
    "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
    "host": "0.0.0.0",
    "port": int(os.getenv("MONGO_PORT")),
    "authSource": "admin",
}

client = MongoClient(**MONGO_CONFIG)

db = client["air_quality_db"]
collection = db["air_quality_data"]

if __name__ == "__main__":
    import json

    result = collection.find(
        {
            "status_code": 200
            # order by created_at descending
        },
        sort=[("created_at", 1)],
        limit=5
    )

    print(json.dumps(list(result), indent=4, default=str))
