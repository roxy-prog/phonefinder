{{ config(materialized='table') }}

with joined_mapped as (
    select
t1.ssid,
t1.campaign_id,
t1.Lead_ID,
t1.clid,
t1.ad_id,
t1.blds_check,
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
),

map as (
    select
        Lead_ID,
        Original_Received,
        time_string,
        blds_check,
        ssid,
        clid,
        campaign_id,
        ad_id,
        fb_adset_id,
        fb_campaign_name,
        fb_adset_name,
        fb_ad_name,
        SID,
        id_number,
        Id_number_valid,
        (case when blds_mtn like 'Pass' then 1 else 0 end) as blds_mtn_pass,
        (case when blds_mtn like 'Fail' then 1 else 0 end) as blds_mtn_fail,
        (case when blds_cellc like 'Pass' then 1 else 0 end) as blds_cellc_pass,
        (case when blds_cellc like 'Fail' then 1 else 0 end) as blds_cellc_fail,
        (case when blds_blc like 'Pass' then 1 else 0 end) as blds_blc_pass,
        (case when blds_blc like 'Fail' then 1 else 0 end) as blds_blc_fail,
        (case when blds_mtn_BureauStatus like 'Approved' then 1 else 0 end) as blds_mtn_Bureau_approved,
        (case when blds_mtn_BureauStatus like 'Failed' then 1 else 0 end) as blds_mtn_Bureau_failed,
        (cast(blds_mtn_BureauPass as numeric)) as blds_mtn_BureauPass,
        max(case when rn = 1 then mtn_sale else 0 end) as mtn_sales,
        max(case when rn = 1 then cellc_sale else 0 end) as cellc_sales
    from joined_mapped
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
)

select
    Original_Received,
    ssid,
    clid,
    campaign_id,
    ad_id,
    fb_adset_id,
    fb_campaign_name,
    fb_adset_name,
    fb_ad_name,
    Id_number_valid,
    SID,
    time_string,
    blds_check,
    count(distinct Lead_ID) as leads,
    count(distinct id_number) as ids,
    
    -- FIX: Replaced standard division '/' with SAFE_DIVIDE(numerator, denominator)
    -- If count(id_number) is 0, this returns NULL instead of throwing an error.
    -- We also wrap the result in COALESCE(..., 0) to return 0 instead of NULL if preferred.
    
    COALESCE(SAFE_DIVIDE(sum(blds_mtn_pass), count(id_number)), 0) * count(distinct id_number) as blds_mtn_pass,
    COALESCE(SAFE_DIVIDE(sum(blds_mtn_fail), count(id_number)), 0) * count(distinct id_number) as blds_mtn_fail,
    COALESCE(SAFE_DIVIDE(sum(blds_cellc_pass), count(id_number)), 0) * count(distinct id_number) as blds_cellc_pass,
    COALESCE(SAFE_DIVIDE(sum(blds_cellc_fail), count(id_number)), 0) * count(distinct id_number) as blds_cellc_fail,
    COALESCE(SAFE_DIVIDE(sum(blds_blc_pass), count(id_number)), 0) * count(distinct id_number) as blds_blc_pass,
    COALESCE(SAFE_DIVIDE(sum(blds_blc_fail), count(id_number)), 0) * count(distinct id_number) as blds_blc_fail,
    COALESCE(SAFE_DIVIDE(sum(blds_mtn_Bureau_approved), count(id_number)), 0) * count(distinct id_number) as blds_mtn_Bureau_approved,
    COALESCE(SAFE_DIVIDE(sum(blds_mtn_Bureau_failed), count(id_number)), 0) * count(distinct id_number) as blds_mtn_Bureau_failed,
    COALESCE(SAFE_DIVIDE(sum(blds_mtn_BureauPass), count(id_number)), 0) * count(distinct id_number) as blds_mtn_BureauPass,
    COALESCE(SAFE_DIVIDE(sum(mtn_sales), count(id_number)), 0) * count(distinct id_number) as mtn_sales,
    COALESCE(SAFE_DIVIDE(sum(cellc_sales), count(id_number)), 0) * count(distinct id_number) as cellc_sales

from map
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13