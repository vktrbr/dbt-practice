{{
    config(
        materialized = 'table',
        unique_key = 'id'
    )
}}


select id
     , air_quality_id
     , (request ->> 'dateTime')::timestamp            as request__forecast_datetime
     , (request -> 'location' ->> 'latitude')::float  as request__location_latitude
     , (request -> 'location' ->> 'longitude')::float as request__location_longitude
     , (response ->> 'regionCode')::text              as response__region_code
     , (response ->> 'hourlyForecasts')::jsonb        as response__hourly_forecasts

from {{ ref("stg_air_quality_forecast") }}

