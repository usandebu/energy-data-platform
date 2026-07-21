with source as (

    select * from {{ source('silver', 'dim_estacion_meteorologica') }}

),

renamed as (

    select
        estacion_id,
        estacion_nombre,
        provincia,
        altitud_metros
    from source

)

select * from renamed