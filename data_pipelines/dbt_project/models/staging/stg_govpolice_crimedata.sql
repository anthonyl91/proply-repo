
-- {{ config(materialized="incremental", unique_key="crime_id",incremental_strategy = "merge") }}
{{ config(materialized="view") }}

select 
crime_id,
(ARRAY_AGG(month) FILTER (WHERE month IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(month) FILTER (WHERE month IS NOT NULL),1))] AS month,
(ARRAY_AGG(reported_by) FILTER (WHERE reported_by IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(reported_by) FILTER (WHERE reported_by IS NOT NULL),1))] AS reported_by,
(ARRAY_AGG(falls_within) FILTER (WHERE falls_within IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(falls_within) FILTER (WHERE falls_within IS NOT NULL),1))] AS falls_within,
(ARRAY_AGG(longitude) FILTER (WHERE longitude IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(longitude) FILTER (WHERE longitude IS NOT NULL),1))] AS longitude,
(ARRAY_AGG(latitude) FILTER (WHERE latitude IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(latitude) FILTER (WHERE latitude IS NOT NULL),1))] AS latitude,
(ARRAY_AGG(location) FILTER (WHERE location IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(location) FILTER (WHERE location IS NOT NULL),1))] AS location,
(ARRAY_AGG(lsoa_code) FILTER (WHERE lsoa_code IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(lsoa_code) FILTER (WHERE lsoa_code IS NOT NULL),1))] AS lsoa_code,
(ARRAY_AGG(lsoa_name) FILTER (WHERE lsoa_name IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(lsoa_name) FILTER (WHERE lsoa_name IS NOT NULL),1))] AS lsoa_name,
(ARRAY_AGG(crime_type) FILTER (WHERE crime_type IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(crime_type) FILTER (WHERE crime_type IS NOT NULL),1))] AS crime_type,
(ARRAY_AGG(last_outcome_category) FILTER (WHERE last_outcome_category IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(last_outcome_category) FILTER (WHERE last_outcome_category IS NOT NULL),1))] AS last_outcome_category,
(ARRAY_AGG(context) FILTER (WHERE context IS NOT NULL))[(ARRAY_UPPER(ARRAY_AGG(context) FILTER (WHERE context IS NOT NULL),1))] AS context
from {{ source("staging","govpolice_crimedata") }}
where crime_id IS NOT NULL
group by crime_id
