with source as (

    select * from {{ source('silver', 'dim_tecnologia_energia') }}

),

renamed as (

    select
        tecnologia_id,
        tecnologia_nombre,
        grupo_energia_id,
        grupo_energia_nombre,
        es_renovable,
        es_almacenamiento
    from source

)

select * from renamed