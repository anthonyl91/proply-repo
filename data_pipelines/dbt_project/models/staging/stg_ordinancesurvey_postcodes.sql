
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

-- {{ config(materialized="incremental", unique_key="transaction_id",incremental_strategy = "merge") }}
{{ config(materialized="table")}}


select *
from {{ source("staging","ordinancesurvey_postcodes") }}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
