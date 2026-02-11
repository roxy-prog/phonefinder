{{ config(materialized='table') }}


WITH google AS (
    SELECT * FROM {{ ref('google_spend') }}
    where Cost is not null
),

meta AS (
    SELECT * FROM {{ ref('metaspend') }}
    where Amount_spent__ZAR_ is not null
),

final as (

SELECT
    -- Dimensions
    COALESCE(google.Day, meta.Reporting_starts) AS date,
    COALESCE(meta.Campaign_ID, google.Campaign_ID) AS campaign_id,
    COALESCE(meta.Campaign_name, google.Campaign) AS campaign_name,
    
    -- Meta Specifics
    meta.Ad_set_ID,
    meta.Ad_set_name,
    meta.Ad_ID,
    meta.Ad_name,

    -- Metrics
    COALESCE(meta.Amount_spent__ZAR_, google.Cost) AS cost,
    COALESCE(meta.Leads, google.Conversions) AS p_leads,
    COALESCE(meta.QualifiedLead, google.Qualified_lead) AS q_leads,
    COALESCE(meta.Purchases, google.Purchase_conversions) AS purchases,

    -- Platform Logic based on Cost Origin
    CASE 
        WHEN google.Cost > 0 THEN 'Google'
        WHEN meta.Amount_spent__ZAR_ > 0 THEN 'Meta'
        WHEN meta.Campaign_name IS NOT NULL THEN 'Meta'
        ELSE 'Google'
    END AS platform

FROM google
FULL OUTER JOIN meta
    ON google.Day = meta.Reporting_starts 
    AND google.Campaign_ID = meta.Campaign_ID
)

select
date,
campaign_id as Campaign_id,
campaign_name,
Ad_set_ID,
Ad_set_name,
Ad_ID,
Ad_name,
cost,
p_leads,
q_leads,
purchases,
platform
from final