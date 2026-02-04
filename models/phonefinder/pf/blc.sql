    
    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'blc_sales_master') }}

