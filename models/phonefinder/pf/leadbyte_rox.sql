    
    {{ config(materialized='table') }}


    select *


from {{ source('Phonefinder', 'leadbyte_rox') }}


