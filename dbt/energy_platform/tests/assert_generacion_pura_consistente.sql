select
    fecha,
    generacion_pura_mwh,
    generacion_renovable_mwh,
    generacion_no_renovable_mwh
from {{ ref('mart_energia_clima_diario') }}
where abs(
    generacion_pura_mwh
    - generacion_renovable_mwh
    - generacion_no_renovable_mwh
) > 0.001