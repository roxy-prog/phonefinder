-- Step 1: Get the grain of your lead data and count leads per Ad/Date
with lead_grain as (
    select
        campaign_id,
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
        cellc_sale,
        campaign_costs,
        campaign_p_leads,
        campaign_q_leads,
        campaign_purchases,
        platform_costs,
        platform_p_leads,
        platform_q_leads,
        platform_purchases,
        count(*) over (partition by cast(ad_id as string), Original_Received) as leads_in_this_ad_bucket
    from {{ ref('leadbyte_costing_campaign_id') }}
),

marketing_costs as (
    select
        cast(Ad_ID as string) as adid,
        date as cost_date,
        sum(cost) as total_ad_cost,
        sum(p_leads) as total_p_leads,
        sum(q_leads) as total_q_leads,
        sum(purchases) as total_purchases
    from {{ ref('google_meta_join') }}
    group by 1, 2
),

joined_data as (
    select
        l.*,
        (m.total_ad_cost / nullif(l.leads_in_this_ad_bucket, 0)) as allocated_cost,
        (m.total_p_leads / nullif(l.leads_in_this_ad_bucket, 0)) as allocated_p_leads,
        (m.total_q_leads / nullif(l.leads_in_this_ad_bucket, 0)) as allocated_q_leads,
        (m.total_purchases / nullif(l.leads_in_this_ad_bucket, 0)) as allocated_purchases
    from lead_grain l
    left join marketing_costs m
        on l.ad_id = m.adid
        and l.Original_Received = m.cost_date
    -- ADDED FILTER HERE
    where l.blds_mtn_BureauStatus is not null
)

-- Final Output
select
    case when campaign_id like 'fb_campaign_id' then null when campaign_id like '' then null else campaign_id end as campaign_id,
    ssid,
    Lead_ID,
    clid,
    case when ad_id like '' then null else ad_id end as ad_id,
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
    cellc_sale,
    coalesce(allocated_cost, 0) as ad_cost,
    coalesce(allocated_p_leads, 0) as ad_p_leads,
    coalesce(allocated_q_leads, 0) as ad_q_leads,
    coalesce(allocated_purchases, 0) as ad_purchases,
    platform_costs,
    platform_p_leads,
    platform_q_leads,
    platform_purchases,
    campaign_costs,
    campaign_p_leads,
    campaign_q_leads,
    campaign_purchases
from joined_data