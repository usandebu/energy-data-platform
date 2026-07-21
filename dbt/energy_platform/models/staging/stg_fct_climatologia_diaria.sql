with source as (

    select * from {{ source('silver', 'fct_climatologia_diaria') }}

),

renamed as (

    select
        fecha,
        estacion_id,
        temperatura_media_c,
        temperatura_minima_c,
        temperatura_maxima_c,
        precipitacion_mm,
        velocidad_media_viento,
        racha_maxima_viento,
        direccion_viento,
        humedad_media_pct,
        humedad_minima_pct,
        humedad_maxima_pct,
        horas_sol,
        source_file,
        loaded_at
    from source

)

select * from renamed