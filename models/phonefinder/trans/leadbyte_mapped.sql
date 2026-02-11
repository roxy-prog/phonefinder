{{ config(materialized='table') }}

with


mapped as (
select
    Lead_ID,
    COALESCE(fb_clid, gclid) as clid,
    fb_campaign_id as campaign_id,
    fb_ad_id as ad_id,
    blds_check,
    fb_ad_name,
    fb_adset_id,
    fb_adset_name,
    fb_campaign_name,
    Original_Received,
    time_string,
    SID,
    SSID,
    id_number,
    blds_description,
    blds_mtn,
    blds_cellc,
    blds_blc,
    blds_mtn_BureauStatus,
    blds_mtn_BureauPass,
    fb_clid,
    gclid,
    Id_number_valid

from {{ ref('leadbyte_mapping') }})
,

final as (
select
case when ssid like '%GOOGLE%' then 'Google'
when ssid like 'FACEBOOKWHATSAPP' then 'Meta WhatsApp' 
when ssid like '%FACEBOOK%' then 'Meta'
when ssid like 'ORGANIC' then 'Meta' else ssid end as ssid,
CASE 
    WHEN ssid LIKE 'FACEBOOKWHATSAPP' THEN '120235589621790327' 
    ELSE CAST(campaign_id AS STRING) 
END AS campaign_id,
    Lead_ID,
    clid,
    ad_id,
    blds_check,
    fb_ad_name,
    fb_adset_id,
    fb_adset_name,
    fb_campaign_name,
    Original_Received,
    time_string,
    SID,
    id_number,
    blds_description,
    blds_mtn,
    blds_cellc,
    blds_blc,
    blds_mtn_BureauStatus,
    blds_mtn_BureauPass,
    fb_clid,
    gclid,
    Id_number_valid
from mapped
)

select *
from final


