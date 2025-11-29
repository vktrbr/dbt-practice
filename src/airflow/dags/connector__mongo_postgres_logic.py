import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

load_dotenv()


def get_config():
    """
    Получение конфигурации
    """

    return {
        "mongo": {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
            "host": os.getenv("MONGO_HOST"),
            "port": int(os.getenv("MONGO_PORT")),
            "authSource": "admin",
        },
        "postgres": {
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "url": f"postgresql://"
                   f"{os.getenv("POSTGRES_USER")}:"
                   f"{os.getenv("POSTGRES_PASSWORD")}@"
                   f"{os.getenv("POSTGRES_HOST")}:"
                   f"{os.getenv("POSTGRES_PORT")}"
                   f"/{os.getenv("POSTGRES_DB")}"
        }
    }


def get_data_from_mongo(update_at: datetime = None):
    """
    Получение данных из MongoDB
    """

    mongo_config = get_config()["mongo"]

    client = MongoClient(**mongo_config)

    db = client["air_quality_db"]
    collection = db["air_quality_data"]

    # Если update_at не передан, то берем все данные
    # Если передан, то берем данные, которые были обновлены после update_at
    # Таким образом, мы можем обновлять только новые данные – делать инкрементальную
    # загрузку
    logger.info(f"Getting data from MongoDB with update_at: {update_at}")
    filter_condition = {} if update_at is None else {"update_at": {"$gt": update_at}}
    data = collection.find(filter_condition)

    for item in data:
        yield item


Base = declarative_base()


class AirQuality(Base):
    __tablename__ = "air_quality"
    # RAW – raw, схема для сырых таблиц после EL процесса
    __table_args__ = {"schema": "raw"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    air_quality_id = Column(String, primary_key=True)
    created_at = Column(DateTime)
    request = Column(JSONB)
    response = Column(JSONB)
    status_code = Column(Integer)
    update_at = Column(DateTime)
    url = Column(String)

    valid_from_dttm = Column(DateTime, server_default=func.now())
    # 5999-12-31 – дата-заглушка, которая означает, что запись актуальна
    valid_to_dttm = Column(DateTime, nullable=False, default="5999-01-01")


def declare_database_in_postgres():
    """
    Создание таблицы в PostgreSQL
    """
    try:
        # Конфигурация PostgreSQL
        postgres_config = get_config()["postgres"]

        # Создание таблицы в PostgreSQL
        engine = create_engine(postgres_config["url"])

        with engine.connect() as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            Base.metadata.create_all(engine, checkfirst=True)
            logger.info("Schema and table created successfully.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise


def get_last_update_at(session, shift_days: int = 1):
    """
    Получение времени последнего обновления

    :param session: Сессия SQLAlchemy
    :param shift_days: Сдвиг в днях – нужен для того, чтобы записи, которые были
    обновлены задним числом, попали в выборку

    """
    last_update_at = session.query(func.max(AirQuality.update_at)).scalar()

    if last_update_at is None:
        last_update_at = datetime(1970, 1, 1)

    last_update_at = last_update_at + timedelta(days=-shift_days)
    return last_update_at


def upsert_air_quality(session, data):
    """
    Загрузка данных в PostgreSQL с обновлением по _id:
    - Если запись существует, обновляем её и ставим valid_to_dttm = NOW()
    - Если запись новая, вставляем её с valid_to_dttm = None
    """
    logger.info("Upserting data to PostgreSQL")
    try:
        for item in data:
            logger.info(f"Upserting item with _id: {item['_id']}")

            # Update existing record
            session.query(AirQuality).filter(
                AirQuality.air_quality_id == str(item["_id"])
            ).update({"valid_to_dttm": func.now()})

            air_quality = AirQuality(
                air_quality_id=str(item["_id"]),
                created_at=item["created_at"],
                request=item["request"],
                response=item["response"],
                status_code=item.get("status_code"),
                update_at=item["update_at"],
                url=item["url"],
            )

            session.add(air_quality)
            session.commit()

        session.commit()
        logger.info("Upsert completed successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred during upsert: {e}")
        raise


def move_data_to_postgres():
    """
    Перемещение данных из MongoDB в PostgreSQL
    """

    engine = create_engine(get_config()["postgres"]["url"])
    Session = sessionmaker(bind=engine)
    session = Session()

    declare_database_in_postgres()

    try:
        # Получение данных из MongoDB
        data = get_data_from_mongo(update_at=get_last_update_at(session))
        upsert_air_quality(session, data)
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        session.close()

    return True
