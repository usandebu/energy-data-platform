with clima as (

    select * from {{ ref('stg_fct_climatologia_diaria') }}

),

aggregated as (

    select
        fecha,
        avg(temperatura_media_c) as temperatura_media_espana_c,
        avg(temperatura_minima_c) as temperatura_minima_media_espana_c,
        avg(temperatura_maxima_c) as temperatura_maxima_media_espana_c,
        avg(precipitacion_mm) as precipitacion_media_mm,
        avg(velocidad_media_viento) as velocidad_media_viento,
        avg(racha_maxima_viento) as racha_maxima_media_viento,
        avg(horas_sol) as horas_sol_media,
        count(distinct estacion_id) as num_estaciones
    from clima
    group by fecha

),

final as (

    select
        fecha,
        temperatura_media_espana_c,
        temperatura_minima_media_espana_c,
        temperatura_maxima_media_espana_c,
        precipitacion_media_mm,
        velocidad_media_viento,
        racha_maxima_media_viento,
        horas_sol_media,
        num_estaciones
    from aggregated

)

select * from final