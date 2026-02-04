    
    {{ config(materialized='table') }}

    select * 
    from {{ source('phonefinder', 'leadbyte_master') }}

