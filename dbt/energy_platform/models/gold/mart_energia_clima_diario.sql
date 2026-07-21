with energia as (

    select * from {{ ref('int_energia_diaria') }}

),

clima as (

    select * from {{ ref('int_clima_diario') }}

),

joined as (

    select
        energia.fecha,

        energia.generacion_pura_mwh,
        energia.generacion_renovable_mwh,
        energia.generacion_no_renovable_mwh,
        energia.saldo_almacenamiento_mwh,
        energia.saldo_intercambios_mwh,
        energia.generacion_eolica_mwh,
        energia.generacion_solar_fotovoltaica_mwh,
        energia.porcentaje_renovable,

        clima.temperatura_media_espana_c,
        clima.temperatura_minima_media_espana_c,
        clima.temperatura_maxima_media_espana_c,
        clima.precipitacion_media_mm,
        clima.velocidad_media_viento,
        clima.racha_maxima_media_viento,
        clima.horas_sol_media,
        clima.num_estaciones

    from energia
    inner join clima
        on energia.fecha = clima.fecha

)

select * from joined