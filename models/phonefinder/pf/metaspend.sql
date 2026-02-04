    
    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'meta_spend') }}

