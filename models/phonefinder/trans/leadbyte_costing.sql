WITH raw_sales AS (
    SELECT * FROM {{ ref('match_sales') }}
),

-- =================================================================
-- NEW STEP: METADATA BACKFILL
-- =================================================================
-- We prioritize the existing data. If it's missing/bad, we fetch it from the Cost table.
sales_with_backfill AS (
    SELECT 
        s.Original_Received,
        s.time_string,
        s.blds_check,
        s.ssid,
        s.clid,
        s.SID,
        s.ad_id,
        s.fb_ad_name,
        s.Id_number_valid,
        COALESCE(CASE 
                WHEN s.campaign_id IS NULL OR REGEXP_CONTAINS(CAST(s.campaign_id AS STRING), r'[^0-9]') 
                THEN CAST(c.Campaign_id AS STRING)
                ELSE CAST(s.campaign_id AS STRING)
            END, 
            CAST(s.campaign_id AS STRING)
        ) as campaign_id,
        COALESCE(
            CASE 
                WHEN s.fb_campaign_name IS NULL OR s.fb_campaign_name = '' 
                THEN c.Campaign_name
                ELSE s.fb_campaign_name
            END,
            s.fb_campaign_name
        ) as fb_campaign_name,
        COALESCE(
            CASE 
                WHEN s.fb_adset_id IS NULL OR REGEXP_CONTAINS(CAST(s.fb_adset_id AS STRING), r'[^0-9]') 
                THEN CAST(c.Ad_set_ID AS STRING)
                ELSE CAST(s.fb_adset_id AS STRING)
            END,
            CAST(s.fb_adset_id AS STRING)
        ) as fb_adset_id,

        COALESCE(
            CASE 
                WHEN s.fb_adset_name IS NULL OR s.fb_adset_name = '' 
                THEN c.Ad_set_name
                ELSE s.fb_adset_name
            END,
            s.fb_adset_name
        ) as fb_adset_name,

        s.leads,
        s.blds_mtn_pass,
        s.blds_mtn_fail,
        s.blds_cellc_pass,
        s.blds_cellc_fail,
        s.blds_blc_pass,
        s.blds_blc_fail,
        s.blds_mtn_Bureau_approved,
        s.blds_mtn_Bureau_failed,
        s.blds_mtn_BureauPass,
        s.mtn_sales,
        s.cellc_sales

    FROM raw_sales s
    -- We join loosely here just for lookup purposes. 
    -- DISTINCT is important because cost table might have duplicate rows for the same Ad ID (one per day).
    LEFT JOIN (
        SELECT DISTINCT 
            CAST(Ad_ID AS STRING) as Ad_ID, 
            Campaign_id, 
            Campaign_name, 
            Ad_set_ID, 
            Ad_set_name 
        FROM {{ ref('google_meta_join') }}
        WHERE Ad_ID IS NOT NULL
    ) c ON CAST(s.ad_id AS STRING) = c.Ad_ID
),

-- =================================================================
-- EXISTING LOGIC CONTINUES BELOW
-- =================================================================

sales_prep AS (
    SELECT 
        *,
        FARM_FINGERPRINT(CONCAT(IFNULL(ssid, ''), IFNULL(clid, ''), Original_Received)) as row_id,
        Original_Received as join_date
    FROM (
        select
            Original_Received,
            time_string,
            blds_check,
            ssid,
            clid,
            SID,
            ad_id,
            -- Now selecting the CLEANED columns from the step above
            campaign_id,
            fb_campaign_name,
            fb_adset_id,
            fb_adset_name,
            fb_ad_name,
            Id_number_valid,
            sum(leads) as leads,
            sum(blds_mtn_pass) as blds_mtn_pass,
            sum(blds_mtn_fail) as blds_mtn_fail,
            sum(blds_cellc_pass) as blds_cellc_pass,
            sum(blds_cellc_fail) as blds_cellc_fail,
            sum(blds_blc_pass) as blds_blc_pass,
            sum(blds_blc_fail) as blds_blc_fail,
            sum(blds_mtn_Bureau_approved) as blds_mtn_Bureau_approved,
            sum(blds_mtn_Bureau_failed) as blds_mtn_Bureau_failed,
            sum(blds_mtn_BureauPass) as blds_mtn_BureauPass,
            sum(mtn_sales) as mtn_sales,
            sum(cellc_sales) as cellc_sales
        from sales_with_backfill  -- CHANGED FROM match_sales to our new CTE
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    )
),

cost_data AS (
    SELECT * FROM {{ ref('google_meta_join') }}
),

total_ad_cost AS (
    SELECT date, Ad_ID, 
           SUM(cost) as cost, SUM(p_leads) as p_leads, SUM(q_leads) as q_leads, SUM(purchases) as purchases
    FROM cost_data WHERE Ad_ID IS NOT NULL GROUP BY 1, 2
),

total_adset_cost AS (
    SELECT date, Ad_set_ID, 
           SUM(cost) as cost, SUM(p_leads) as p_leads, SUM(q_leads) as q_leads, SUM(purchases) as purchases
    FROM cost_data WHERE Ad_set_ID IS NOT NULL GROUP BY 1, 2
),

total_campaign_cost AS (
    SELECT date, Campaign_id, 
           SUM(cost) as cost, SUM(p_leads) as p_leads, SUM(q_leads) as q_leads, SUM(purchases) as purchases
    FROM cost_data WHERE Campaign_id IS NOT NULL GROUP BY 1, 2
),

total_platform_cost AS (
    SELECT date, platform, 
           SUM(cost) as cost, SUM(p_leads) as p_leads, SUM(q_leads) as q_leads, SUM(purchases) as purchases
    FROM cost_data WHERE platform IS NOT NULL GROUP BY 1, 2
),

-- LEVEL 1: MATCH BY AD ID
match_level_1 AS (
    SELECT 
        s.*,
        1 as match_level,
        (c.cost / COUNT(*) OVER (PARTITION BY s.join_date, s.ad_id)) as allocated_cost,
        (c.p_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.ad_id)) as allocated_p_leads,
        (c.q_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.ad_id)) as allocated_q_leads,
        (c.purchases / COUNT(*) OVER (PARTITION BY s.join_date, s.ad_id)) as allocated_purchases
    FROM sales_prep s
    INNER JOIN total_ad_cost c 
        ON CAST(s.ad_id AS STRING) = CAST(c.Ad_ID AS STRING) 
        AND s.join_date = c.date
        AND s.blds_check = 'checked'
),

consumed_by_adset AS (
    SELECT join_date, fb_adset_id, SUM(allocated_cost) as used_cost 
    FROM match_level_1 
    WHERE fb_adset_id IS NOT NULL 
    GROUP BY 1, 2
),

-- LEVEL 2: MATCH BY AD SET ID
match_level_2 AS (
    SELECT 
        s.*,
        2 as match_level,
        (GREATEST(0, c.cost - COALESCE(used.used_cost, 0)) / COUNT(*) OVER (PARTITION BY s.join_date, s.fb_adset_id)) as allocated_cost,
        (c.p_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.fb_adset_id)) as allocated_p_leads,
        (c.q_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.fb_adset_id)) as allocated_q_leads,
        (c.purchases / COUNT(*) OVER (PARTITION BY s.join_date, s.fb_adset_id)) as allocated_purchases
    FROM sales_prep s
    LEFT JOIN match_level_1 m1 ON s.row_id = m1.row_id
    INNER JOIN total_adset_cost c 
        ON CAST(s.fb_adset_id AS STRING) = CAST(c.Ad_set_ID AS STRING) 
        AND s.join_date = c.date
        AND s.blds_check = 'checked'
    LEFT JOIN consumed_by_adset used 
        ON s.join_date = used.join_date AND CAST(s.fb_adset_id AS STRING) = CAST(used.fb_adset_id AS STRING)
    WHERE m1.row_id IS NULL
),

consumed_by_campaign AS (
    SELECT join_date, campaign_id, SUM(allocated_cost) as used_cost
    FROM (
        SELECT join_date, campaign_id, allocated_cost FROM match_level_1
        UNION ALL
        SELECT join_date, campaign_id, allocated_cost FROM match_level_2
    ) 
    WHERE campaign_id IS NOT NULL 
    GROUP BY 1, 2
),

-- LEVEL 3: MATCH BY CAMPAIGN ID
match_level_3 AS (
    SELECT 
        s.*,
        3 as match_level,
        (GREATEST(0, c.cost - COALESCE(used.used_cost, 0)) / COUNT(*) OVER (PARTITION BY s.join_date, s.campaign_id)) as allocated_cost,
        (c.p_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.campaign_id)) as allocated_p_leads,
        (c.q_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.campaign_id)) as allocated_q_leads,
        (c.purchases / COUNT(*) OVER (PARTITION BY s.join_date, s.campaign_id)) as allocated_purchases
    FROM sales_prep s
    LEFT JOIN match_level_1 m1 ON s.row_id = m1.row_id
    LEFT JOIN match_level_2 m2 ON s.row_id = m2.row_id
    INNER JOIN total_campaign_cost c 
        ON CAST(s.campaign_id AS STRING) = CAST(c.Campaign_id AS STRING) 
        AND s.join_date = c.date
        AND s.blds_check = 'checked'
    LEFT JOIN consumed_by_campaign used 
        ON s.join_date = used.join_date AND CAST(s.campaign_id AS STRING) = CAST(used.campaign_id AS STRING)
    WHERE m1.row_id IS NULL AND m2.row_id IS NULL
),

consumed_by_platform AS (
    SELECT join_date, ssid, SUM(allocated_cost) as used_cost
    FROM (
        SELECT join_date, ssid, allocated_cost FROM match_level_1
        UNION ALL
        SELECT join_date, ssid, allocated_cost FROM match_level_2
        UNION ALL
        SELECT join_date, ssid, allocated_cost FROM match_level_3
    ) GROUP BY 1, 2
),

-- LEVEL 4: MATCH BY PLATFORM (SSID)
match_level_4 AS (
    SELECT 
        s.*,
        4 as match_level,
        (GREATEST(0, c.cost - COALESCE(used.used_cost, 0)) / COUNT(*) OVER (PARTITION BY s.join_date, s.ssid)) as allocated_cost,
        (c.p_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.ssid)) as allocated_p_leads,
        (c.q_leads / COUNT(*) OVER (PARTITION BY s.join_date, s.ssid)) as allocated_q_leads,
        (c.purchases / COUNT(*) OVER (PARTITION BY s.join_date, s.ssid)) as allocated_purchases
    FROM sales_prep s
    LEFT JOIN match_level_1 m1 ON s.row_id = m1.row_id
    LEFT JOIN match_level_2 m2 ON s.row_id = m2.row_id
    LEFT JOIN match_level_3 m3 ON s.row_id = m3.row_id
    INNER JOIN total_platform_cost c 
        ON s.ssid = c.platform 
        AND s.join_date = c.date
        AND s.blds_check = 'checked'
    LEFT JOIN consumed_by_platform used 
        ON s.join_date = used.join_date AND s.ssid = used.ssid
    WHERE m1.row_id IS NULL AND m2.row_id IS NULL AND m3.row_id IS NULL
),

-- NO MATCH
no_match AS (
    SELECT 
        s.*,
        0 as match_level,
        0 as allocated_cost,
        0 as allocated_p_leads,
        0 as allocated_q_leads,
        0 as allocated_purchases
    FROM sales_prep s
    LEFT JOIN match_level_1 m1 ON s.row_id = m1.row_id
    LEFT JOIN match_level_2 m2 ON s.row_id = m2.row_id
    LEFT JOIN match_level_3 m3 ON s.row_id = m3.row_id
    LEFT JOIN match_level_4 m4 ON s.row_id = m4.row_id
    WHERE m1.row_id IS NULL 
      AND m2.row_id IS NULL 
      AND m3.row_id IS NULL 
      AND m4.row_id IS NULL
),

final_union AS (
    SELECT * FROM match_level_1
    UNION ALL
    SELECT * FROM match_level_2
    UNION ALL
    SELECT * FROM match_level_3
    UNION ALL
    SELECT * FROM match_level_4
    UNION ALL
    SELECT * FROM no_match
),

final as (
    SELECT
        Original_Received,
        ssid,
        clid,
        campaign_id,
        ad_id,
        fb_adset_id,
        case when ssid like 'Meta WhatsApp' then 'Meta WhatsApp' else fb_campaign_name end as fb_campaign_name,
        fb_adset_name,
        fb_ad_name,
        time_string,
        blds_check,
        SID,
        leads,
        blds_mtn_pass,
        blds_mtn_fail,
        blds_cellc_pass,
        blds_cellc_fail,
        blds_blc_pass,
        blds_blc_fail,
        blds_mtn_Bureau_approved,
        blds_mtn_Bureau_failed,
        blds_mtn_BureauPass,
        mtn_sales,
        cellc_sales,
        Id_number_valid,
        COALESCE(allocated_cost, 0) as cost,
        COALESCE(allocated_p_leads, 0) as p_leads,
        COALESCE(allocated_q_leads, 0) as q_leads,
        COALESCE(allocated_purchases, 0) as purchases
    FROM final_union
    WHERE ssid NOT LIKE 'WHATSAPPBROADCAST'
)

select
        Original_Received,
        ssid,
        clid,
        campaign_id,
        ad_id,
        fb_adset_id,
        case when ssid like 'Google' then 'Google' else fb_campaign_name end as campaign_name,
        fb_adset_name,
        fb_ad_name,
        time_string,
        blds_check,
        SID,
        Id_number_valid,
        sum(leads) as leads,
        sum(blds_mtn_pass) as blds_mtn_pass,
        sum(blds_mtn_fail) as blds_mtn_fail,
        sum(blds_cellc_pass) as blds_cellc_pass,
        sum(blds_cellc_fail) as blds_cellc_fail,
        sum(blds_blc_pass) as blds_blc_pass,
        sum(blds_blc_fail) as blds_blc_fail,
        sum(blds_mtn_Bureau_approved) as blds_mtn_Bureau_approved,
        sum(blds_mtn_Bureau_failed) as blds_mtn_Bureau_failed,
        sum(blds_mtn_BureauPass) as blds_mtn_BureauPass,
        sum(mtn_sales) as mtn_sales,
        sum(cellc_sales) as cellc_sales,
        sum(case when ssid like '%Meta%' and fb_campaign_name is null then 0 else cost end) as cost,
        sum(p_leads) as p_leads,
        sum(q_leads) as q_leads,
        sum(purchases) as purchases
from final
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
