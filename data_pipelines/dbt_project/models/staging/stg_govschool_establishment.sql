
{{ config(materialized="table")}}

select *
from {{ source("staging","govschools_establishmentdata") }}
