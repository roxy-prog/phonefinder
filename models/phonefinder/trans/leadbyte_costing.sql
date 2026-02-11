WITH 
-- Base data from match_sales
base_data AS (
  SELECT *
  FROM {{ ref('match_sales') }}
),

-- Cost data
cost_data AS (
  SELECT
    date,
    Campaign_id,
    Ad_set_ID,
    Ad_ID,
    cost,
    platform
  FROM {{ ref('google_meta_join') }}
),

-- Step 1: Ad-level matches (most granular)
ad_level_allocations AS (
  SELECT
    c.date,
    c.Campaign_id,
    c.Ad_set_ID,
    c.Ad_ID,
    c.platform,
    c.cost,
    COUNT(b.Lead_ID) AS match_count
  FROM cost_data c
  LEFT JOIN base_data b
    ON CAST(c.Ad_ID AS STRING) = CAST(b.ad_id AS STRING)
    AND CAST(c.date AS DATE) = CAST(b.Original_Received AS DATE)
    AND b.ad_id IS NOT NULL 
    AND CAST(b.ad_id AS STRING) != ''
  WHERE c.Ad_ID IS NOT NULL AND CAST(c.Ad_ID AS STRING) != ''
  GROUP BY c.date, c.Campaign_id, c.Ad_set_ID, c.Ad_ID, c.platform, c.cost
),

-- Step 2: Adset-level allocations (subtract ad-level costs)
adset_level_allocations AS (
  SELECT
    c.date,
    c.Campaign_id,
    c.Ad_set_ID,
    c.platform,
    c.cost - COALESCE(SUM(ad_alloc.cost), 0) AS remaining_cost,
    COUNT(b.Lead_ID) AS match_count
  FROM cost_data c
  LEFT JOIN ad_level_allocations ad_alloc
    ON CAST(c.Ad_set_ID AS STRING) = CAST(ad_alloc.Ad_set_ID AS STRING)
    AND CAST(c.date AS DATE) = CAST(ad_alloc.date AS DATE)
  LEFT JOIN base_data b
    ON CAST(c.Ad_set_ID AS STRING) = CAST(b.fb_adset_id AS STRING)
    AND CAST(c.date AS DATE) = CAST(b.Original_Received AS DATE)
    AND (b.ad_id IS NULL OR CAST(b.ad_id AS STRING) = '')
    AND b.fb_adset_id IS NOT NULL 
    AND CAST(b.fb_adset_id AS STRING) != ''
  WHERE c.Ad_set_ID IS NOT NULL AND CAST(c.Ad_set_ID AS STRING) != ''
  GROUP BY c.date, c.Campaign_id, c.Ad_set_ID, c.platform, c.cost
),

-- Step 3: Campaign-level allocations (subtract ad and adset costs)
campaign_level_allocations AS (
  SELECT
    c.date,
    c.Campaign_id,
    c.platform,
    c.cost 
      - COALESCE(SUM(ad_alloc.cost), 0)
      - COALESCE(SUM(CASE WHEN adset_alloc.match_count > 0 THEN adset_alloc.remaining_cost ELSE 0 END), 0) AS remaining_cost,
    COUNT(b.Lead_ID) AS match_count
  FROM cost_data c
  LEFT JOIN ad_level_allocations ad_alloc
    ON CAST(c.Campaign_id AS STRING) = CAST(ad_alloc.Campaign_id AS STRING)
    AND CAST(c.date AS DATE) = CAST(ad_alloc.date AS DATE)
  LEFT JOIN adset_level_allocations adset_alloc
    ON CAST(c.Campaign_id AS STRING) = CAST(adset_alloc.Campaign_id AS STRING)
    AND CAST(c.date AS DATE) = CAST(adset_alloc.date AS DATE)
  LEFT JOIN base_data b
    ON CAST(c.Campaign_id AS STRING) = CAST(b.campaign_id AS STRING)
    AND CAST(c.date AS DATE) = CAST(b.Original_Received AS DATE)
    AND (b.ad_id IS NULL OR CAST(b.ad_id AS STRING) = '')
    AND (b.fb_adset_id IS NULL OR CAST(b.fb_adset_id AS STRING) = '')
    AND b.campaign_id IS NOT NULL 
    AND CAST(b.campaign_id AS STRING) != ''
  WHERE c.Campaign_id IS NOT NULL AND CAST(c.Campaign_id AS STRING) != ''
  GROUP BY c.date, c.Campaign_id, c.platform, c.cost
),

-- Step 4: Platform-level allocations (subtract all lower-level costs)
platform_level_allocations AS (
  SELECT
    c.date,
    c.platform,
    c.cost 
      - COALESCE(SUM(ad_alloc.cost), 0)
      - COALESCE(SUM(CASE WHEN adset_alloc.match_count > 0 THEN adset_alloc.remaining_cost ELSE 0 END), 0)
      - COALESCE(SUM(CASE WHEN campaign_alloc.match_count > 0 THEN campaign_alloc.remaining_cost ELSE 0 END), 0) AS remaining_cost,
    COUNT(b.Lead_ID) AS match_count
  FROM cost_data c
  LEFT JOIN ad_level_allocations ad_alloc
    ON CAST(c.platform AS STRING) = CAST(ad_alloc.platform AS STRING)
    AND CAST(c.date AS DATE) = CAST(ad_alloc.date AS DATE)
  LEFT JOIN adset_level_allocations adset_alloc
    ON CAST(c.platform AS STRING) = CAST(adset_alloc.platform AS STRING)
    AND CAST(c.date AS DATE) = CAST(adset_alloc.date AS DATE)
  LEFT JOIN campaign_level_allocations campaign_alloc
    ON CAST(c.platform AS STRING) = CAST(campaign_alloc.platform AS STRING)
    AND CAST(c.date AS DATE) = CAST(campaign_alloc.date AS DATE)
  LEFT JOIN base_data b
    ON CAST(c.platform AS STRING) = CAST(b.ssid AS STRING)
    AND CAST(c.date AS DATE) = CAST(b.Original_Received AS DATE)
    AND (b.ad_id IS NULL OR CAST(b.ad_id AS STRING) = '')
    AND (b.fb_adset_id IS NULL OR CAST(b.fb_adset_id AS STRING) = '')
    AND (b.campaign_id IS NULL OR CAST(b.campaign_id AS STRING) = '')
    AND b.ssid IS NOT NULL 
    AND CAST(b.ssid AS STRING) != ''
  WHERE c.platform IS NOT NULL AND CAST(c.platform AS STRING) != ''
  GROUP BY c.date, c.platform, c.cost
)

-- Final SELECT with cascading cost allocation
SELECT
  b.ssid,
  b.campaign_id,
  b.Lead_ID,
  b.clid,
  b.ad_id,
  b.fb_ad_name,
  b.fb_adset_id,
  b.fb_adset_name,
  b.fb_campaign_name,
  b.Original_Received,
  b.time_string,
  b.SID,
  b.id_number,
  b.blds_description,
  b.blds_mtn,
  b.blds_cellc,
  b.blds_blc,
  b.blds_mtn_BureauStatus,
  b.blds_mtn_BureauPass,
  b.Id_number_valid,
  b.mtn_sale,
  b.cellc_sale,
  -- Waterfall cost allocation logic
  CASE
    -- Level 1: Direct ad match
    WHEN b.ad_id IS NOT NULL AND CAST(b.ad_id AS STRING) != '' THEN
      COALESCE(SAFE_DIVIDE(ad_alloc.cost, ad_alloc.match_count), 0)
    -- Level 2: Adset match (no ad_id)
    WHEN b.fb_adset_id IS NOT NULL AND CAST(b.fb_adset_id AS STRING) != '' THEN
      COALESCE(SAFE_DIVIDE(adset_alloc.remaining_cost, adset_alloc.match_count), 0)
    -- Level 3: Campaign match (no ad_id or fb_adset_id)
    WHEN b.campaign_id IS NOT NULL AND CAST(b.campaign_id AS STRING) != '' THEN
      COALESCE(SAFE_DIVIDE(campaign_alloc.remaining_cost, campaign_alloc.match_count), 0)
    -- Level 4: Platform match (no ad_id, fb_adset_id, or campaign_id)
    WHEN b.ssid IS NOT NULL AND CAST(b.ssid AS STRING) != '' THEN
      COALESCE(SAFE_DIVIDE(platform_alloc.remaining_cost, platform_alloc.match_count), 0)
    ELSE 0
  END AS cost
FROM base_data b
LEFT JOIN ad_level_allocations ad_alloc
  ON CAST(b.ad_id AS STRING) = CAST(ad_alloc.Ad_ID AS STRING)
  AND CAST(b.Original_Received AS DATE) = CAST(ad_alloc.date AS DATE)
LEFT JOIN adset_level_allocations adset_alloc
  ON CAST(b.fb_adset_id AS STRING) = CAST(adset_alloc.Ad_set_ID AS STRING)
  AND CAST(b.Original_Received AS DATE) = CAST(adset_alloc.date AS DATE)
LEFT JOIN campaign_level_allocations campaign_alloc
  ON CAST(b.campaign_id AS STRING) = CAST(campaign_alloc.Campaign_id AS STRING)
  AND CAST(b.Original_Received AS DATE) = CAST(campaign_alloc.date AS DATE)
LEFT JOIN platform_level_allocations platform_alloc
  ON CAST(b.ssid AS STRING) = CAST(platform_alloc.platform AS STRING)
  AND CAST(b.Original_Received AS DATE) = CAST(platform_alloc.date AS DATE)