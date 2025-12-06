{{
    config(
        unique_key = 'air_quality_id'
    )
}}


select id
     , air_quality_id
     , (request ->> 'dateTime')::timestamp            as request__forecast_datetime
     , (request -> 'location' ->> 'latitude')::float  as request__location_latitude
     , (request -> 'location' ->> 'longitude')::float as request__location_longitude
     , (response ->> 'regionCode')::text              as response__region_code
     , (response ->> 'hourlyForecasts')::jsonb        as response__hourly_forecasts

     , current_timestamp as processed_dttm
     , processed_dttm as processed_src_dttm

from {{ ref("stg_air_quality_forecast") }}

where true
    {% if is_incremental() %}

        and processed_dttm >=
        (
            select coalesce(max(processed_src_dttm), '1900-01-01'::timestamp)
            from {{ this }}
        )

    {%- endif %}

