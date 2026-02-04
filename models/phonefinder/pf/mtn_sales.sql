    
    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'mtn_sales_master') }}

