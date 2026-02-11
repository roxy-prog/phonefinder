    
    {{ config(materialized='table') }}


    select
    Lead_ID,
    SID,
    SSID,
    created_date,
    id_number,
    blds_description,
    blds_mtn,
    blds_cellc,
    blds_blc,
    blds_mtn_BureauStatus,
    blds_mtn_BureauPass,
    LEFT(fb_ad_id, 18) as fb_ad_id,
    fb_adset_id,
    fb_campaign_id,
    fb_clid,
    gclid,
    Id_number_valid,
    fb_campaign_name,
    fb_adset_name,
    fb_ad_name


from {{ source('Phonefinder', 'leadbyte_rox') }}


