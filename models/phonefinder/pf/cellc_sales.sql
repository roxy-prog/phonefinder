    
    {{ config(materialized='table') }}

    select * 
    from {{ source('Phonefinder', 'cellc_sales_master') }}

