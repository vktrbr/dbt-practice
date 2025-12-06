{{
    config(
        materialized = 'table',
        unique_key = 'air_quality_id'
    )
}}


select air_quality_id
     , main_table.request__forecast_datetime                  as request_datetime
     , main_table.request__location_latitude                  as location_latitude
     , main_table.request__location_longitude                 as location_longitude
     , main_table.response__region_code                       as region_code

     , hourly_forecasts.forecast__indexes_aqi                 as aqi
     , hourly_forecasts.forecast__indexes_code                as code
     , hourly_forecasts.forecast__indexes_category            as category
     , hourly_forecasts.forecast__indexes_aqi_display         as aqi_display
     , hourly_forecasts.forecast__indexes_display_name        as display_name
     , hourly_forecasts.forecast__indexes_dominant_pollutant  as dominant_pollutant
     , hourly_forecasts.forecast__indexes_color_red           as color_red
     , hourly_forecasts.forecast__indexes_color_blue          as color_blue
     , hourly_forecasts.forecast__indexes_color_green         as color_green
     , hourly_forecasts.forecast__hourly_forecasts_row_number as hourly_forecasts_row_number

from {{ ref("ods_air_quality_forecast") }} main_table
         left join {{ ref("ods_air_quality_forecast_hourly_forecasts") }} hourly_forecasts using (air_quality_id)


