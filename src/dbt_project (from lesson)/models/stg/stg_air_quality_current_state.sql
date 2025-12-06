{{
    config(
        unique_key = 'air_quality_id'
    )
}}

select *
      , current_timestamp as processed_dttm
from {{ source("raw_data", "air_quality") }}
where valid_to_dttm = '5999-01-01 00:00:00.000000'
  and url = 'https://airquality.googleapis.com/v1/currentConditions:lookup'
    {% if is_incremental() %}

    and valid_from_dttm >= (
          select max(valid_from_dttm)
          from air_quality.stg.stg_airquality
      )

    {% endif %}