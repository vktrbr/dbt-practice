{{
    config(
        materialized = 'table',
        unique_key = 'air_quality_id'
    )
}}


with first_unnest as (
    select *
         , jsonb_array_elements(response__hourly_forecasts) as forecasts
    from {{ ref("ods_air_quality_forecast") }}
)
   , second_unnest as (
    select *
         , forecasts ->> 'dateTime'                     as forecast__datetime
         , jsonb_array_elements(forecasts -> 'indexes') as forecast__indexes
    from first_unnest
)
   , row_numbered as (
    select id
         , air_quality_id

         , (forecast__indexes ->> 'aqi')::int                as forecast__indexes_aqi
         , (forecast__indexes ->> 'code')::text              as forecast__indexes_code
         , (forecast__indexes ->> 'category')::text          as forecast__indexes_category
         , (forecast__indexes ->> 'aqiDisplay')::text        as forecast__indexes_aqi_display
         , (forecast__indexes ->> 'displayName')::text       as forecast__indexes_display_name
         , (forecast__indexes ->> 'dominantPollutant')::text as forecast__indexes_dominant_pollutant

         , (forecast__indexes -> 'color' ->> 'red')::float   as forecast__indexes_color_red
         , (forecast__indexes -> 'color' ->> 'blue')::float  as forecast__indexes_color_blue
         , (forecast__indexes -> 'color' ->> 'green')::float as forecast__indexes_color_green
         , row_number() over (partition by id)               as forecast__hourly_forecasts_row_number

    from second_unnest
)
select id
     , air_quality_id
     , forecast__indexes_aqi
     , forecast__indexes_code
     , forecast__indexes_category
     , forecast__indexes_aqi_display
     , forecast__indexes_display_name
     , forecast__indexes_dominant_pollutant
     , forecast__indexes_color_red
     , forecast__indexes_color_blue
     , forecast__indexes_color_green
     , forecast__hourly_forecasts_row_number

from row_numbered
where forecast__hourly_forecasts_row_number = 1 -- Простая логика, чтобы получить только первый прогноз

