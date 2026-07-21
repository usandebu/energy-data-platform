with source as (

    select * from {{ source('silver', 'fct_generacion_energia_diaria') }}

),

renamed as (

    select
        fecha,
        tecnologia_id,
        generacion_mwh,
        porcentaje_grupo,
        source_file,
        loaded_at
    from source

)

select * from renamed