{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key="air_quality_id"
    )
}}


select  id
      , air_quality_id
      , created_at
      , request
      , response
      , status_code
      , update_at
      , url
      , valid_from_dttm
      , valid_to_dttm

      , current_timestamp as processed_dttm

from {{ source("raw_data", "air_quality") }}
where valid_to_dttm = '5999-01-01 00:00:00.000000'

    {% if is_incremental() %}

    and valid_from_dttm >= (
          select max(valid_from_dttm)
          from air_quality.stg.stg_airquality
      )

    {% endif %}
