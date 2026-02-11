 {{ config(materialized='table') }}

with 

join_cellc_mtn as (
select
Date_Dispositioned,
Lead_Ref,
Client_Name,
Client_Surname,
Client_ID_Number,
Contact_Number1,
Campaign,
Client_Code,
App_Disposition,
Disposition_Reason,
Source,
leaLead_Ref,
lines_sold,
matched_to_network,
activated,
pf_leadid,
pf_lead_date,
filename,
insertdt
from {{ ref('cellc_sales') }}


union all

select
Date_Dispositioned,
Lead_Ref,
Client_Name,
Client_Surname,
Client_ID_Number,
Contact_Number1,
Campaign,
Client_Code,
App_Disposition,
Disposition_Reason,
Source,
leaLead_Ref,
lines_sold,
matched_to_network,
activated,
pf_leadid,
pf_lead_date,
filename,
insertdt
from {{ ref('mtn_sales') }}
)

select 
Date_Dispositioned,
Lead_Ref,
Client_Name,
Client_Surname,
Client_ID_Number,
Contact_Number1,
Campaign,
Client_Code,
App_Disposition,
Disposition_Reason,
Source,
leaLead_Ref,
lines_sold,
matched_to_network,
activated,
pf_leadid,
pf_lead_date,
filename,
insertdt,
case when Campaign like '%Cell C%' then 1 else 0 end as cellc_sale,
case when Campaign like '%MTN%' then 1 else 0 end as mtn_sale
from join_cellc_mtn
where pf_leadid is not null



