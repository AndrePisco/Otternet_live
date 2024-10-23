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
  ,case when a.creditor_risk_label_detail in ("in_administration","insolvency","restructuring","dissolved","liquidation","inactivity") then true else false end as insolvency_flag
	,a.creditor_created_date 
	,a.is_account_closed
	,a.is_payment_provider
  ,a.organisation_with_multiple_creditors
	,b.current_revenue_account_type as account_type
  ,b.current_state
  ,(CASE WHEN case when b.is_cs_managed_salesforce is true or b.parent_account_stage in ('CS Managed - Ent&MM','CS Managed - SB') then true else false end  THEN 'Yes' ELSE 'No' END) AS is_cs_managed
  ,b.parent_account_id
  ,b.parent_account_name
	,1 as var1

from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_organisation` as b
on a.organisation_id = b.organisation_id
where b.current_state != 'preactive'
and not a.is_payment_provider)

,exposure as (
select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)

,db_failure_temp as (
select 
    creditor_id
    ,legal_events.has_insolvency as db_insolvency_flag
    ,dnb_assessment.failure_score.national_percentile as db_failure_score_current
    ,date(retrieved_at) as db_failure_score_current_date
    ,coalesce(lag(dnb_assessment.failure_score.national_percentile) over (partition by creditor_id order by retrieved_at),dnb_assessment.failure_score.national_percentile) as db_failure_score_last
    ,coalesce(lag(date(retrieved_at)) over (partition by creditor_id order by retrieved_at),date(retrieved_at)) as db_failure_score_last_date
from  `gc-prd-risk-prod-gdia.dun_bradstreet_reports.dun_bradstreet_report__4` 
where dnb_assessment.failure_score.national_percentile is not null
and date(retrieved_at) < current_date() )

,db_failure as (
select *
from db_failure_temp
where db_failure_score_current_date = current_date()-1)

,creditor_balances as (
											select
												owner_id as creditor_id 
												,calendar_date 
												,sum(balance_amount_sum_gbp) as balance_amount_sum_gbp
											from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.scd_abacus_available_merchant_funds_daily`
											where name = 'available_merchant_funds'
											and calendar_date = current_date()-1
											group by 1,2)

,creditor_payments as (
select
	creditor_id
    ,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then amount_gbp          else 0 end) as merchant_payment_amt_gbp_last_365d
from dbt_core_model.x_payments
group by 1)

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
	,a.insolvency_flag
  ,a.current_state
  ,a.is_cs_managed
  ,a.parent_account_id
  ,a.parent_account_name

	,b.fds_exposure_current

	,c.db_failure_score_current
	,c.db_failure_score_current_date
	,c.db_failure_score_last
	,c.db_failure_score_last_date
	,c.db_insolvency_flag
	,c.db_failure_score_current-db_failure_score_last as db_failure_score_change
    
	,case when d.balance_amount_sum_gbp <0 then d.balance_amount_sum_gbp else 0 end as nb_balance_current

	,e.merchant_payment_amt_gbp_last_365d

from creditor_details  			               as a 
left join exposure   			               as b on a.creditor_id=b.creditor_id
left join db_failure 			               as c on a.creditor_id=c.creditor_id
left join creditor_balances		               as d on a.creditor_id=d.creditor_id
left join creditor_payments                    as e on a.creditor_id=e.creditor_id
)

------------------------------------------------------
--Monitoring Qualifyer & Alert Criteria
------------------------------------------------------

,payload as (
select * 
,case when (fds_exposure_current >= 250000 and db_failure_score_current < 40) or (nb_balance_current <= -20000) or (fds_exposure_current >= 500000) then 1 else 0 end as merchant_monitoring_qualifyer
,case when 
		((db_failure_score_current >= 86) and (db_failure_score_change <= -20))
		or 
		((db_failure_score_current >= 51 and db_failure_score_current <= 85) and (db_failure_score_change <= -10))
		or
		((db_failure_score_current >= 11 and db_failure_score_current <= 50) and (db_failure_score_change <= -5))
		or 
		((db_failure_score_current >= 1 and db_failure_score_current <= 10) and (db_failure_score_change <= -2))
		then "New Alert"
    else "No Alert"
	  end as failure_score_monitoring_alert

from data_merge
)

------------------------------------------------------
--Failure Score Monitoring - Alert Criteria
------------------------------------------------------

select * 
      ,'Credit_Monitoring_DNB' as process_name

,TO_JSON_STRING(
    STRUCT(
        STRUCT(
            "normal" AS priority, 
            3285009 AS brand_id, 
            360005611314 AS group_id, 
            9724439852828 AS requester_id, 
            5636997079964 AS ticket_form_id,
            4451452073116 AS assignee_id,
            
            ARRAY<STRUCT<
                id INT64, 
                value STRING
            >>[
                STRUCT(28480929, 'credit__monitoring_fs')  -- Custom field entries
                -- Additional custom fields can be uncommented if needed
                -- STRUCT(15542500163356, '12345'),  -- Exposure
                -- STRUCT(15545615128732, '123')  -- Fraud score
            ] AS custom_fields,
            
            -- Comment object
            STRUCT(
                'Creditor ID: ' || COALESCE(creditor_id, '') 
                || '\nOrganisation ID: ' || COALESCE(organisation_id, '')
                || '\nMerchant name: ' || COALESCE(merchant_name, '')
                || '\nGeo: ' || COALESCE(geo, '')
                || '\nMCC: ' || COALESCE(merchant_category_code_description, '')
                || '\nPayment provider: ' || COALESCE(is_payment_provider, false)
                || '\nCS Managed?: ' || COALESCE(is_cs_managed, '')
                || '\nCurrent Risk Label: ' || COALESCE(merchant_risk_label_description, '')
                || '\nParent ID: ' || COALESCE(parent_account_id, '')
                || '\nParent Name: ' || COALESCE(parent_account_name, '')
                || '\nAccount Type: ' || COALESCE(account_type, '')
                || '\nPayments last 12m: ' || COALESCE(CAST(ROUND(merchant_payment_amt_gbp_last_365d, 2) AS STRING), '')
                || '\nFDS Exposure: ' || COALESCE(CAST(ROUND(fds_exposure_current, 2) AS STRING), '')
                || '\nNegative Balance: ' || COALESCE(nb_balance_current, 0)
                || '\n\n'
                || '\nCurrent D&B Score: ' || COALESCE(db_failure_score_current, null)
                || '\nPrevious D&B Score: ' || COALESCE(db_failure_score_last, null)
                || '\nScore Change: ' || COALESCE(db_failure_score_change, null)
                || '\n\n\nCreated by OtterNet'
                AS body,
                false AS public
            ) AS comment,

            -- Subject
            'Credit Monitoring - D&B Score - ' || COALESCE(merchant_name, '') || ' - ' || COALESCE(creditor_id, '') AS subject
        ) AS ticket
    )
) AS ActionField_ZendeskCreateTicket



from payload
where merchant_monitoring_qualifyer = 1 
			and failure_score_monitoring_alert = "New Alert"
			and insolvency_flag = false
			and date(db_failure_score_current_date) = CURRENT_DATE()-1
			-- LIMIT 1
