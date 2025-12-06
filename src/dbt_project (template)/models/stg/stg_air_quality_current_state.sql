{{
    config(
        materialized = 'table',
        unique_key = 'id'
    )
}}

select *
from {{ source("raw_data", "air_quality") }}
where valid_to_dttm = '5999-01-01 00:00:00.000000'
  and url = 'https://airquality.googleapis.com/v1/currentConditions:lookup'
