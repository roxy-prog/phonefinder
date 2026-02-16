with table1 as (
    select
        case when ssid like 'WHATSAPPBROADCAST' then null else campaign_id end as campaign_id,
        case when ssid like '%Meta%' then 'Meta'
        when ssid like '%FACEBOOK%' then 'Meta'
        when ssid like '%Google%' then 'Google' else ssid end as ssid,
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
        cellc_sale
    from {{ ref('match_sales') }}
    -- Added filter here
    where blds_mtn_BureauStatus is not null
),

table2 as (
    select
        Campaign_id as camp_id,
        campaign_name,
        Ad_set_ID,
        Ad_set_name,
        Ad_ID as adid,
        Ad_name,
        platform,
        date,
        sum(cost) as cost,
        sum(p_leads) as p_leads,
        sum(q_leads) as q_leads,
        sum(purchases) as purchases
        
    from {{ ref('google_meta_join') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

joined_data as (
    select * FROM table1
    LEFT JOIN table2
        ON table1.Original_Received = table2.date
        AND table1.ssid = table2.platform
),

final as (
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
        (sum(cost))/(case when (count(ssid) over (partition by Original_Received, ssid order by Original_Received)) = 0 then 1 else (count(Lead_ID) over (partition by Original_Received, ssid order by Original_Received)) end) as cost,
        (sum(p_leads))/(case when (count(ssid) over (partition by Original_Received, ssid order by Original_Received)) = 0 then 1 else (count(Lead_ID) over (partition by Original_Received, ssid order by Original_Received)) end) as p_leads,
        (sum(q_leads))/(case when (count(ssid) over (partition by Original_Received, ssid order by Original_Received)) = 0 then 1 else (count(Lead_ID) over (partition by Original_Received, ssid order by Original_Received)) end) as q_leads,
        (sum(purchases))/(case when (count(ssid) over (partition by Original_Received, ssid order by Original_Received)) = 0 then 1 else (count(Lead_ID) over (partition by Original_Received, ssid order by Original_Received)) end) as purchases
    from joined_data
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
)

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
    cost,
    p_leads,
    q_leads,
    purchases
from final