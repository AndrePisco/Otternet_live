
with

creditor_details as (
select
	a.id as creditor_id
	,a.organisation_id
	,a.name as merchant_name
	,a.geo
	,a.merchant_category_code
	,a.merchant_category_code_description
	,a.creditor_risk_label_parent as merchant_risk_label
	,a.creditor_risk_label_detail as merchant_risk_label_description
	,a.most_recent_risk_label_created_at
	,case when a.creditor_risk_label_detail in ("in_administration","insolvency","restructuring","dissolved","liquidation","inactivity") then true else false end as insolvency_flag
	,a.creditor_created_date 
	,a.is_account_closed
	,a.is_payment_provider
  ,a.organisation_with_multiple_creditors
	,b.current_revenue_account_type as account_type
  ,b.current_state
  ,b.parent_account_id
  ,b.parent_account_name
	,1 as var1
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join dbt_core_model.d_organisation as b
on a.organisation_id = b.organisation_id
where b.current_state != "preactive"  and date(most_recent_risk_label_created_at) >= date("2024-09-04")
and not a.is_payment_provider)

,exposure as (
select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)


,creditor_payments as (
select
	creditor_id
    ,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then amount_gbp          else 0 end) as merchant_payment_amt_gbp_last_365d
from dbt_core_model.x_payments
group by 1)


,history as (
						SELECT 
							runtime,
							JSON_VALUE(values, '$.creditor_id') AS creditor_id,
							JSON_VALUE(values, '$.ticket_id') AS ticket_id,
							row_number() over (partition by JSON_VALUE(values, '$.creditor_id') order by runtime desc) as rowno

						FROM `gc-prd-credit-risk-dev-81b5.otternet_dev.otternet_devlog` 
						where length(JSON_VALUE(values, '$.creditor_id')) > 0 and process_name = 'credit_insolvency'
						qualify rowno = 1
)

----------------------------------------------------------------------------
--Data Merge
----------------------------------------------------------------------------

,data_merge as (
select 
	a.creditor_id 
	,a.organisation_id
	,a.merchant_name
	,a.geo
	,a.merchant_category_code
	,a.merchant_category_code_description
	,a.is_payment_provider
	,a.account_type
  ,a.merchant_risk_label
	,a.merchant_risk_label_description
	,date(a.most_recent_risk_label_created_at) as most_recent_risk_label_created_at
	,a.insolvency_flag
	,a.parent_account_id
  ,a.parent_account_name
	,round(b.fds_exposure_current,1) as fds_exposure_current
	,round(c.merchant_payment_amt_gbp_last_365d,1) as merchant_payment_amt_gbp_last_365d
	,d.ticket_id

from creditor_details  			as a 
left join exposure   			as b on a.creditor_id=b.creditor_id
left join creditor_payments     as c on a.creditor_id=c.creditor_id
left join history as d on a.creditor_id=d.creditor_id
)


,payload as (
select * 
from data_merge
where insolvency_flag is true
-- and ticket_id is null
and (fds_exposure_current >= 100000 or merchant_payment_amt_gbp_last_365d >= 1000000)
)

select * 

			,'credit_insolvency' as process_name

			,TO_JSON_STRING(STRUCT(
        STRUCT(
            "normal" AS priority, 
            3285009 as brand_id, 
            360005611314 as group_id, 
            9724439852828 as requester_id, 
            5636997079964 AS ticket_form_id,
						4451452073116 as assignee_id,

            ARRAY<STRUCT<
                id INT64, 
                value STRING
            >>[
                -- Custom field entries
                STRUCT(28480929, 'credit__inactive_company')  -- Category
                -- STRUCT(15542500163356, '12345')  -- Exposure
                -- STRUCT(15545615128732, '123')  -- Fraud score (uncomment if needed)

            ] AS custom_fields,

            -- Comment object
            STRUCT(
                'Creditor ID: ' || creditor_id 
								||'\n' || 'Organisation ID: ' || organisation_id
								||'\n' || 'Merchant name: ' || merchant_name
								||'\n' || 'Geo: ' || geo
								||'\n' || 'MCC: ' || merchant_category_code_description
								||'\n' || 'Payment provider: ' || is_payment_provider
								||'\n' || 'Insolvency flag: ' || merchant_risk_label_description
								||'\n' || 'Risk Label Date: ' || most_recent_risk_label_created_at
								||'\n' || 'Parent ID: ' || parent_account_id
								||'\n' || 'Parent Name: ' || parent_account_name
								||'\n' || 'Account Type: ' || account_type
								
								||'\n' || 'Payments last 12m: ' || round(merchant_payment_amt_gbp_last_365d,2)
								||'\n' || 'FDS Exposure: ' || round(fds_exposure_current,2)
								-- ||'\n' || ': ' || xxxx
								-- ||'\n' || ': ' || xxxx
								-- ||'\n' || ': ' || xxxx
								||'\n' || '\n' || '\n' || 'Created by OtterNet'
								 AS body,
                false AS public
            ) AS comment,

            -- Subject
            'Credit Monitoring - Insolvency - ' || merchant_name || ' - ' || creditor_id AS subject


        ) AS ticket
				)) AS ActionField_ZendeskCreateTicket


from payload
where date(most_recent_risk_label_created_at) >= CURRENT_DATE()-30
limit 50

