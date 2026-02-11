WITH metaspend_deduped AS (
    SELECT
        Ad_name,
        Campaign_name,
        Campaign_ID,
        Ad_set_name,
        Ad_set_ID,
        Ad_ID,
        ROW_NUMBER() OVER (PARTITION BY Ad_ID ORDER BY Ad_ID) as rn
    FROM {{ ref('metaspend') }}
),
metaspend_unique AS (
    SELECT
        Ad_name,
        Campaign_name,
        CAST(Campaign_ID AS STRING) AS Campaign_ID,
        Ad_set_name,
        CAST(Ad_set_ID AS STRING) AS Ad_set_ID,
        CAST(Ad_ID AS STRING) AS Ad_ID
    FROM metaspend_deduped
    WHERE rn = 1
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
    l.fb_ad_id,
    -- Replace fb_adset_id if there's a match
    CASE 
        WHEN l.fb_ad_id IS NOT NULL 
             AND l.fb_ad_id <> '' 
             AND m.Ad_set_ID IS NOT NULL
        THEN m.Ad_set_ID
        ELSE l.fb_adset_id
    END AS fb_adset_id,
    -- Replace fb_campaign_id if there's a match
    CASE 
        WHEN l.fb_ad_id IS NOT NULL 
             AND l.fb_ad_id <> '' 
             AND m.Campaign_ID IS NOT NULL
        THEN m.Campaign_ID
        ELSE l.fb_campaign_id
    END AS fb_campaign_id,
    l.fb_clid,
    l.gclid,
    l.Id_number_valid,
    -- Replace fb_campaign_name if there's a match
    CASE 
        WHEN l.fb_ad_id IS NOT NULL 
             AND l.fb_ad_id <> '' 
             AND m.Campaign_name IS NOT NULL
        THEN m.Campaign_name
        ELSE l.fb_campaign_name
    END AS fb_campaign_name,
    -- Replace fb_adset_name if there's a match
    CASE 
        WHEN l.fb_ad_id IS NOT NULL 
             AND l.fb_ad_id <> '' 
             AND m.Ad_set_name IS NOT NULL
        THEN m.Ad_set_name
        ELSE l.fb_adset_name
    END AS fb_adset_name,
    -- Replace fb_ad_name if there's a match
    CASE 
        WHEN l.fb_ad_id IS NOT NULL 
             AND l.fb_ad_id <> '' 
             AND m.Ad_name IS NOT NULL
        THEN m.Ad_name
        ELSE l.fb_ad_name
    END AS fb_ad_name
FROM {{ ref('leadbyte_mapping') }} l
LEFT JOIN metaspend_unique m
    ON l.fb_ad_id = m.Ad_ID