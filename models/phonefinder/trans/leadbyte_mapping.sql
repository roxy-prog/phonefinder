WITH leadbyte_data AS (
    SELECT * FROM {{ ref('leadbyte') }}
),

-- Create unique mapping tables from metaspend to avoid row duplication (fan-out)
-- since metaspend likely has multiple rows per campaign (daily data).
metaspend_campaigns AS (
    SELECT DISTINCT 
        Campaign_name, 
        Campaign_ID
    FROM {{ ref('metaspend') }}
    WHERE Campaign_ID IS NOT NULL
),

metaspend_adsets AS (
    SELECT DISTINCT 
        Campaign_name, 
        Ad_set_name, 
        Ad_set_ID
    FROM {{ ref('metaspend') }}
    WHERE Ad_set_ID IS NOT NULL
),

metaspend_ads AS (
    SELECT DISTINCT 
        Campaign_name, 
        Ad_set_name, 
        Ad_name, 
        Ad_ID,
        Ad_set_ID -- Included for the fallback lookup logic
    FROM {{ ref('metaspend') }}
    WHERE Ad_ID IS NOT NULL
),

pf_mapping_data AS (
    SELECT * FROM {{ source('Phonefinder', 'pf_mapping') }}
)

SELECT
    lb.* EXCEPT (fb_campaign_id, fb_adset_id, fb_ad_id),

    -- 1. CAMPAIGN ID LOGIC
    COALESCE(
        -- First: Try keeping the original if it is valid (numerical)
        SAFE_CAST(lb.fb_campaign_id AS INT64),
        -- Second: Match with metaspend on Campaign Name
        ms_camp.Campaign_ID,
        -- Third: Match with pf_mapping on Campaign Name
        pf_camp.Actual_Campaign_ID
    ) AS fb_campaign_id,

    -- 2. ADSET ID LOGIC
    COALESCE(
        -- First: Try keeping the original if it is valid
        SAFE_CAST(lb.fb_adset_id AS INT64),
        -- Second: Match with metaspend (Campaign Name AND (Adset Name OR Ad Name))
        ms_adset.Ad_set_ID,
        -- Third: Match with metaspend using just Ad ID (if Ad ID exists in Leadbyte)
        ms_ad_lookup.Ad_set_ID,
        -- Fourth: Match with pf_mapping (Campaign Name AND Adset Name)
        pf_adset.Actual_Adset_ID
    ) AS fb_adset_id,

    -- 3. AD ID LOGIC
    COALESCE(
        -- First: Try keeping the original if it is valid
        SAFE_CAST(lb.fb_ad_id AS INT64),
        -- Second: Match with metaspend (Campaign + Adset + Ad Names)
        ms_ad.Ad_ID,
        -- Third: Match with pf_mapping (Campaign + Adset + Ad Names)
        pf_ad.Actual_Ad_ID
    ) AS fb_ad_id

FROM leadbyte_data lb

-- Join for Campaign ID (Logic 1)
LEFT JOIN metaspend_campaigns ms_camp 
    ON lb.fb_campaign_name = ms_camp.Campaign_name
LEFT JOIN pf_mapping_data pf_camp 
    ON lb.fb_campaign_name = pf_camp.fb_campaign_name

-- Join for Adset ID (Logic 2)
-- Complex join: Campaign Match AND (Adset Match OR Ad Name matches Adset Name)
LEFT JOIN metaspend_adsets ms_adset 
    ON lb.fb_campaign_name = ms_adset.Campaign_name 
    AND (lb.fb_adset_name = ms_adset.Ad_set_name OR lb.fb_ad_name = ms_adset.Ad_set_name)
-- Secondary lookup: If we have an Ad ID in leadbyte, find its parent Ad Set in metaspend
LEFT JOIN metaspend_ads ms_ad_lookup
    ON SAFE_CAST(lb.fb_ad_id AS INT64) = ms_ad_lookup.Ad_ID
LEFT JOIN pf_mapping_data pf_adset
    ON lb.fb_campaign_name = pf_adset.fb_campaign_name 
    AND lb.fb_adset_name = pf_adset.fb_adset_name

-- Join for Ad ID (Logic 3)
LEFT JOIN metaspend_ads ms_ad 
    ON lb.fb_campaign_name = ms_ad.Campaign_name 
    AND lb.fb_adset_name = ms_ad.Ad_set_name 
    AND lb.fb_ad_name = ms_ad.Ad_name
LEFT JOIN pf_mapping_data pf_ad
    ON lb.fb_campaign_name = pf_ad.fb_campaign_name 
    AND lb.fb_adset_name = pf_ad.fb_adset_name
    AND lb.fb_ad_name = pf_ad.fb_ad_name