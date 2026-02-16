{{ config(materialized='table') }}
WITH 

original as (
    select
    DATE(CAST(left(created_date, 10) AS TIMESTAMP)) AS Original_Received,
    SPLIT(SPLIT(created_date, 'T')[OFFSET(1)], '+')[OFFSET(0)] as time_string,
    Lead_ID,
    case when SSID like '%GOOGLE%' then 'Google'
    when SSID like '%FACEBOOK%' then 'Meta' else SSID end as SSID,
    case when SSID like '%GOOGLE%' then '278'    
    when SSID like '%META%' then '278'    
    when SSID like '%FACEBOOK%' then '278' else SID end as SID,
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
    case when SSID like '%FACEBOOKWHATSAPP%' and fb_campaign_name is null then 'whatsapp_campaign' else fb_campaign_name end as fb_campaign_name,
    fb_adset_name,
    fb_ad_name
    from {{ ref('leadbyte_rox') }}
),

leadbyte_data AS (
    SELECT
    Lead_ID,
    SID,
    SSID,
    Original_Received,
    time_string,
    id_number,
    blds_description,
    blds_mtn,
    blds_cellc,
    blds_blc,
    blds_mtn_BureauStatus,
    blds_mtn_BureauPass,
    fb_ad_id,
    fb_adset_id,
    fb_campaign_id,
    concat((case when fb_campaign_name like '%campaign.name%' then '' else fb_campaign_name end), (case when fb_adset_name like '%adset.name%' then '' else fb_adset_name end)) as concat_campaign_adset,
    concat((case when fb_campaign_name like '%campaign.name%' then '' else fb_campaign_name end), (case when fb_adset_name like '%adset.name%' then '' else fb_adset_name end), (case when fb_ad_name like '%ad.name%' then '' else fb_ad_name end)) as concat_campaign_adset_ad_names,
    concat((case when fb_campaign_id like '%campaign.id%' then '' else fb_campaign_id end), (case when fb_adset_id like '%adset.id%' then '' else fb_adset_id end), (case when fb_ad_id like '%ad.id%' then '' else fb_ad_id end)) as concat_ids,
    fb_clid,
    gclid,
    Id_number_valid,
    case when fb_campaign_name like '%campaign.name%' then null else fb_campaign_name end as fb_campaign_name,
    fb_adset_name,
    fb_ad_name
    FROM original
),

-- Create a consolidated mapping with all three lookups combined
mapping_consolidated AS (
    SELECT
        l.Lead_ID,
        MAX(CASE WHEN m.concat_campaign_adset_ad = l.concat_campaign_adset_ad_names 
                 THEN CAST(m.Actual_Ad_ID AS STRING) END) as lookup_ad_id,
        MAX(CASE WHEN m.concat_campaign_adset = l.concat_campaign_adset 
                 THEN CAST(m.Actual_Adset_ID AS STRING) END) as lookup_adset_id,
        MAX(CASE WHEN m.fb_campaign_name = l.fb_campaign_name 
                 THEN CAST(m.Actual_Campaign_ID AS STRING) END) as lookup_campaign_id
    FROM leadbyte_data l
    LEFT JOIN {{ ref('mapping') }} m
        ON (l.concat_campaign_adset_ad_names = m.concat_campaign_adset_ad AND m.concat_campaign_adset_ad IS NOT NULL AND m.concat_campaign_adset_ad <> '')
        OR (l.concat_campaign_adset = m.concat_campaign_adset AND m.concat_campaign_adset IS NOT NULL AND m.concat_campaign_adset <> '')
        OR (l.fb_campaign_name = m.fb_campaign_name AND m.fb_campaign_name IS NOT NULL AND m.fb_campaign_name <> '')
    GROUP BY l.Lead_ID
)

SELECT
    l.Lead_ID,
    l.SID,
    l.SSID,
    l.Original_Received,
    l.time_string,
    l.id_number,
    l.blds_description,
    l.blds_mtn,
    l.blds_cellc,
    l.blds_blc,
    l.blds_mtn_BureauStatus,
    l.blds_mtn_BureauPass,
    -- Replace fb_ad_id when conditions are met
    CASE 
        WHEN (l.concat_ids IS NULL OR l.concat_ids = '') 
             AND (l.concat_campaign_adset_ad_names IS NOT NULL AND l.concat_campaign_adset_ad_names <> '')
             AND m.lookup_ad_id IS NOT NULL
        THEN m.lookup_ad_id
        ELSE l.fb_ad_id
    END AS fb_ad_id,
    -- Replace fb_adset_id when conditions are met
    CASE 
        WHEN (l.concat_ids IS NULL OR l.concat_ids = '') 
             AND (l.concat_campaign_adset_ad_names IS NOT NULL AND l.concat_campaign_adset_ad_names <> '')
             AND m.lookup_adset_id IS NOT NULL
        THEN m.lookup_adset_id
        ELSE l.fb_adset_id
    END AS fb_adset_id,
    -- Replace fb_campaign_id when conditions are met
    CASE 
        WHEN (l.concat_ids IS NULL OR l.concat_ids = '') 
             AND (l.fb_campaign_name IS NOT NULL AND l.fb_campaign_name <> '')
             AND m.lookup_campaign_id IS NOT NULL
        THEN m.lookup_campaign_id
        ELSE l.fb_campaign_id
    END AS fb_campaign_id,
    l.concat_campaign_adset,
    l.concat_campaign_adset_ad_names,
    l.concat_ids,
    l.fb_clid,
    l.gclid,
    l.Id_number_valid,
    l.fb_campaign_name,
    l.fb_adset_name,
    l.fb_ad_name
FROM leadbyte_data l
LEFT JOIN mapping_consolidated m
    ON l.Lead_ID = m.Lead_ID