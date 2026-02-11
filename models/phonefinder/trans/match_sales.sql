{{ config(materialized='table') }}


with joined_mapped as (
    select
t1.ssid,
t1.campaign_id,
t1.Lead_ID,
t1.clid,
t1.ad_id,
t1.fb_ad_name,
t1.fb_adset_id,
t1.fb_adset_name,
t1.fb_campaign_name,
t1.Original_Received,
t1.time_string,
t1.SID,
t1.id_number,
t1.blds_description,
t1.blds_mtn,
t1.blds_cellc,
t1.blds_blc,
t1.blds_mtn_BureauStatus,
t1.blds_mtn_BureauPass,
t1.Id_number_valid,
        t2.mtn_sale,
        t2.cellc_sale,
        ROW_NUMBER() OVER (PARTITION BY t1.Lead_ID ORDER BY t1.Original_Received DESC) as rn
    from {{ ref('leadbyte_mapped') }} as t1
    left join {{ ref('mtn_cellc_sales') }} as t2
        -- Cast both to string to resolve INT64 vs STRING conflict
        on SAFE_CAST(t1.Lead_ID AS STRING) = SAFE_CAST(t2.pf_leadid AS STRING)
)

select
case when ssid like 'Meta WhatsApp' and campaign_id is null then '120235589621790327' else campaign_id end as campaign_id,
ssid,
Lead_ID,
clid,
ad_id,
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
Id_number_valid,
mtn_sale,
cellc_sale
from joined_mapped