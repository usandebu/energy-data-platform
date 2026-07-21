with generacion as (

    select * from {{ ref('stg_fct_generacion_energia_diaria') }}

),

tecnologia as (

    select * from {{ ref('stg_dim_tecnologia_energia') }}

),

joined as (

    select
        generacion.fecha,
        generacion.tecnologia_id,
        generacion.generacion_mwh,
        tecnologia.tecnologia_nombre,
        tecnologia.grupo_energia_id,
        tecnologia.grupo_energia_nombre,
        tecnologia.es_renovable,
        tecnologia.es_almacenamiento
    from generacion
    left join tecnologia
        on generacion.tecnologia_id = tecnologia.tecnologia_id

),

aggregated as (

    select
        fecha,

        sum(
            case
                when grupo_energia_id in ('Renovable', 'No-Renovable')
                    then generacion_mwh
                else 0
            end
        ) as generacion_pura_mwh,

        sum(
            case
                when grupo_energia_id = 'Renovable'
                    then generacion_mwh
                else 0
            end
        ) as generacion_renovable_mwh,

        sum(
            case
                when grupo_energia_id = 'No-Renovable'
                    then generacion_mwh
                else 0
            end
        ) as generacion_no_renovable_mwh,

        sum(
            case
                when grupo_energia_id = 'Almacenamiento'
                    then generacion_mwh
                else 0
            end
        ) as saldo_almacenamiento_mwh,

        sum(
            case
                when grupo_energia_id = 'Demanda'
                    then generacion_mwh
                else 0
            end
        ) as saldo_intercambios_mwh,

        sum(
            case
                when tecnologia_nombre = 'Eólica'
                    then generacion_mwh
                else 0
            end
        ) as generacion_eolica_mwh,

        sum(
            case
                when tecnologia_nombre = 'Solar fotovoltaica'
                    then generacion_mwh
                else 0
            end
        ) as generacion_solar_fotovoltaica_mwh

    from joined
    group by fecha

),

final as (

    select
        fecha,
        generacion_pura_mwh,
        generacion_renovable_mwh,
        generacion_no_renovable_mwh,
        saldo_almacenamiento_mwh,
        saldo_intercambios_mwh,
        generacion_eolica_mwh,
        generacion_solar_fotovoltaica_mwh,
        generacion_renovable_mwh / nullif(generacion_pura_mwh, 0) as porcentaje_renovable
    from aggregated

)

select * from final