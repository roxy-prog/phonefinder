    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'pf_mapping') }}

