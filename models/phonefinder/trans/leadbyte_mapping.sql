{{ config(materialized='table') }}

WITH leadbyte_data AS (
    SELECT
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
    fb_ad_id,
    fb_adset_id,
    fb_campaign_id,
    fb_clid,
    gclid,
    Id_number_valid,
    fb_campaign_name,
    fb_adset_name,
    fb_ad_name
    FROM {{ ref('leadbyte_rox') }}
),

metaspend_campaigns AS (
    SELECT DISTINCT 
        Campaign_name, 
        Campaign_ID
    FROM {{ ref('metaspend') }}
    WHERE Campaign_ID IS NOT NULL
),

metaspend_adsets AS (
    SELECT DISTINCT 
        Ad_set_name, 
        Ad_set_ID
    FROM {{ ref('metaspend') }}
    WHERE Ad_set_ID IS NOT NULL
),

metaspend_ads AS (
    SELECT DISTINCT 
        Ad_name, 
        Ad_ID
    FROM {{ ref('metaspend') }}
    WHERE Ad_ID IS NOT NULL
),

pf_mapping_data AS (
    SELECT 
        fb_campaign_name,
        fb_adset_name,
        fb_ad_name,
        Actual_Campaign_ID,
        Actual_Adset_ID,
        Actual_Ad_ID
    FROM {{ source('Phonefinder', 'pf_mapping') }}
),

joined_data AS (
    SELECT 
        lb.*, 
        
        -- 1. Campaign Name Lookup
        COALESCE(
            ms_camp.Campaign_name,
            pf_camp.fb_campaign_name
        ) AS _calc_fb_campaign_name,

        -- 2. Adset Name Lookup
        COALESCE(
            ms_adset.Ad_set_name,
            pf_adset.fb_adset_name
        ) AS _calc_fb_adset_name,

        -- 3. Ad Name Lookup
        COALESCE(
            ms_ad.Ad_name,
            pf_ad.fb_ad_name
        ) AS _calc_fb_ad_name

    FROM leadbyte_data lb

    -- Join for Campaign Name using ID
    LEFT JOIN metaspend_campaigns ms_camp 
        ON SAFE_CAST(lb.fb_campaign_id AS INT64) = ms_camp.Campaign_ID
    LEFT JOIN (SELECT DISTINCT fb_campaign_name, Actual_Campaign_ID FROM pf_mapping_data) pf_camp 
        ON SAFE_CAST(lb.fb_campaign_id AS INT64) = pf_camp.Actual_Campaign_ID

    -- Join for Adset Name using ID
    LEFT JOIN metaspend_adsets ms_adset 
        ON SAFE_CAST(lb.fb_adset_id AS INT64) = ms_adset.Ad_set_ID
    LEFT JOIN (SELECT DISTINCT fb_adset_name, Actual_Adset_ID FROM pf_mapping_data) pf_adset
        ON SAFE_CAST(lb.fb_adset_id AS INT64) = pf_adset.Actual_Adset_ID

    -- Join for Ad Name using ID
    LEFT JOIN metaspend_ads ms_ad 
        ON SAFE_CAST(lb.fb_ad_id AS INT64) = ms_ad.Ad_ID
    LEFT JOIN (SELECT DISTINCT fb_ad_name, Actual_Ad_ID FROM pf_mapping_data) pf_ad
        ON SAFE_CAST(lb.fb_ad_id AS INT64) = pf_ad.Actual_Ad_ID
)

SELECT
    case when blds_mtn is null then 'not_checked' else 'checked' end as blds_check,
    fb_ad_id,
    _calc_fb_ad_name AS fb_ad_name,
    fb_adset_id,
    _calc_fb_adset_name AS fb_adset_name,
    fb_campaign_id,
    _calc_fb_campaign_name AS fb_campaign_name,
    DATE(CAST(created_date AS TIMESTAMP)) AS Original_Received,
    SPLIT(SPLIT(created_date, 'T')[OFFSET(1)], '+')[OFFSET(0)] as time_string,
    SID,
    SSID,
    Lead_ID,
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
FROM joined_data