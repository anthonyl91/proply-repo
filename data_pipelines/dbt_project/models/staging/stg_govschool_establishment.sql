
{{ config(materialized="view")}}

select *
from {{ source("staging","govschools_establishmentdata") }}
