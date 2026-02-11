    
    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'google_spend') }}

