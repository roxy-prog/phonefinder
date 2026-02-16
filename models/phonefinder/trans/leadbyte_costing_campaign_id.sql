-- Step 1: Get the grain of your lead data and count leads per campaign/date
with lead_grain as (
    select
        cast((case when ssid like 'WHATSAPPBROADCAST' then null else campaign_id end) as string) as campaign_id,
        ssid,
        Lead_ID,
        clid,
        cast(ad_id as string) as ad_id,
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
        -- We keep these to maintain your original structure, but renamed for clarity
        cost as platform_costs,
        p_leads as platform_p_leads,
        q_leads as platform_q_leads,
        purchases as platform_purchases,
        -- Count leads per campaign/day to use as a divisor for cost allocation
        count(*) over (partition by (case when ssid like 'WHATSAPPBROADCAST' then null else campaign_id end), Original_Received) as leads_in_this_bucket
    from {{ ref('leadbyte_costing') }}
    where blds_mtn_BureauStatus is not null  -- << ADDED THIS FILTER
),

-- Step 2: Aggregate costs from your marketing platforms
marketing_costs as (
    select
        cast(Campaign_id as string) as camp_id,
        date as cost_date,
        sum(cost) as total_actual_cost,
        sum(p_leads) as total_p_leads,
        sum(q_leads) as total_q_leads,
        sum(purchases) as total_purchases
    from {{ ref('google_meta_join') }}
    group by 1, 2
),

-- Step 3: Join and divide costs across the leads
joined_data as (
    select
        l.*,
        -- Logic: Total Cost / Number of Leads = Cost per Lead
        -- We use nullif to avoid division by zero errors
        (m.total_actual_cost / nullif(l.leads_in_this_bucket, 0)) as allocated_cost,
        (m.total_p_leads / nullif(l.leads_in_this_bucket, 0)) as allocated_p_leads,
        (m.total_q_leads / nullif(l.leads_in_this_bucket, 0)) as allocated_q_leads,
        (m.total_purchases / nullif(l.leads_in_this_bucket, 0)) as allocated_purchases
    from lead_grain l
    left join marketing_costs m
        on l.campaign_id = m.camp_id
        and l.Original_Received = m.cost_date
)

-- Final Output: Clean selection with allocated costs
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
    coalesce(allocated_cost, 0) as campaign_costs,
    coalesce(allocated_p_leads, 0) as campaign_p_leads,
    coalesce(allocated_q_leads, 0) as campaign_q_leads,
    coalesce(allocated_purchases, 0) as campaign_purchases,
    platform_costs,
    platform_p_leads,
    platform_q_leads,
    platform_purchases
from joined_data