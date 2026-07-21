select
    fecha,
    porcentaje_renovable
from {{ ref('mart_energia_clima_diario') }}
where porcentaje_renovable < 0
   or porcentaje_renovable > 1
   or porcentaje_renovable is null