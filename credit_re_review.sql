with

/******************************************************************************************************/
/******************************************Creditor Details********************************************/
/******************************************************************************************************/
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
	,b.is_cs_managed
  ,b.csm_owner_name
	,1 as var1
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join dbt_core_model.d_organisation as b
on a.organisation_id = b.organisation_id
where not a.is_payment_provider)


/******************************************************************************************************/
/******************************************  FDS Exposure  ********************************************/
/******************************************************************************************************/
,exposure as (
select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)


/******************************************************************************************************/
/******************************************   D&B Scores   ********************************************/
/******************************************************************************************************/
,db_failure as (
select 
    creditor_id
    ,dnb_assessment.failure_score.national_percentile as db_failure_score_current
    ,date(retrieved_at) as db_failure_score_current_date
    ,row_number() over (partition by creditor_id order by retrieved_at desc) as rowno

from  `gc-prd-risk-prod-gdia.dun_bradstreet_reports.dun_bradstreet_report__4` 
where dnb_assessment.failure_score.national_percentile is not null
qualify rowno = 1)


/******************************************************************************************************/
/******************************************   PD  Scores   ********************************************/
/******************************************************************************************************/
,PD_score as (
select 
  creditor_id
  ,prediction_date
  ,date(concat(substr(prediction_date,1,4),"-",substr(prediction_date,5,2),"-",substr(prediction_date,7,2))) as prediction_calendar_date
  ,probability as PD_score_latest
from `gc-prd-credit-risk-dev-81b5.pd_model.probability_of_default_model_predictions_historic`
qualify row_number() over (partition by creditor_id order by prediction_date desc)= 1 )


/******************************************************************************************************/
/******************************************  NB Balances   ********************************************/
/******************************************************************************************************/
,creditor_balances as (
select
	 owner_id as creditor_id 
	,calendar_date 
	,sum(balance_amount_sum_gbp) as balance_amount_sum_gbp
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.scd_abacus_available_merchant_funds_daily`
where name = 'available_merchant_funds'
and calendar_date = current_date()-1
group by 1,2)


/******************************************************************************************************/
/******************************************    Payments    ********************************************/
/******************************************************************************************************/
,creditor_payments_temp as (select
	creditor_id
    ,sum(case when date(charge_date) between current_date() and current_date()+7 then amount_gbp else 0 end) as future_payments_7days

    ,sum(case when is_paid and date(charge_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_30d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_30d

    ,sum(case when is_paid and date(charge_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_90d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_90d
    ,sum(case when is_failed and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_failure_vol_last_90d
    ,sum(case when is_late_failure and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_late_failure_vol_last_90d
    ,sum(case when is_refunded  and date(refund_created_at) between current_date()-90 and current_date()-1 then 1  else 0 end) as merchant_refund_vol_last_90d

    ,sum(case when is_paid and date(charge_date)  between current_date()-365   and current_date()-1    then amount_gbp  else 0 end) as merchant_payment_amt_gbp_last_365d

from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` 
where 
date(charge_date) between current_date()-365 and current_date()-1
or date(charged_back_date) between current_date()-90 and current_date()-1
or date(failed_or_late_failure_date) between current_date()-90 and current_date()-1
or date(refund_created_at) between current_date()-90 and current_date()-1

group by 1)


,creditor_payments as (
select
	creditor_id

	,future_payments_7days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_30d,merchant_payment_vol_last_30d) as cb_rate_30days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_90d,merchant_payment_vol_last_90d) as cb_rate_90days
	,SAFE_DIVIDE(merchant_failure_vol_last_90d,merchant_payment_vol_last_90d) as failure_rate_90days
	,SAFE_DIVIDE(merchant_late_failure_vol_last_90d,merchant_payment_vol_last_90d) as late_failure_rate_90days
	,SAFE_DIVIDE(merchant_refund_vol_last_90d,merchant_payment_vol_last_90d) as refund_rate_90days

	,merchant_payment_amt_gbp_last_365d

	from creditor_payments_temp)

/******************************************************************************************************/
/******************************************    Existing Tickets    ********************************************/
/******************************************************************************************************/
,tickets as (SELECT
  ticket_id
  ,date(tickets.created_at) as created_at
  ,tickets.subject
  ,max(case when ticket_field_title = "Next Review Date" then SAFE_CAST(ticket_field_value AS DATE) else null end) AS next_review_date
  ,max(case when ticket_field_title = "Reason for next review" then ticket_field_value else null end) AS reason_for_next_review
  ,max(org_ids.gc_organization_id) as organisation_id
  


FROM `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_tickets` as tickets
  left join `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_ticket_fields`  as fields on tickets.id = fields.ticket_id
  left join `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_organizations` as org_ids on org_ids.id = tickets.organization_id
  group by 1,2,3
  having next_review_date is not null)

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
                    ,a.is_cs_managed
                    ,a.csm_owner_name

                    ,round(b.fds_exposure_current,1) as fds_exposure_current

                    ,c.db_failure_score_current
                    ,c.db_failure_score_current_date

                    ,d.PD_score_latest
                    ,d.prediction_calendar_date

                    ,case when e.balance_amount_sum_gbp <0 then e.balance_amount_sum_gbp else 0 end as nb_balance_current
                    
                    ,round(f.merchant_payment_amt_gbp_last_365d,1) as merchant_payment_amt_gbp_last_365d
                    ,f.cb_rate_90days
                    ,f.failure_rate_90days
                    ,f.late_failure_rate_90days
                    ,f.refund_rate_90days


                    ,g.ticket_id
                    ,g.created_at as ticket_created_at
                    ,g.next_review_date
                    ,g.reason_for_next_review

                  from creditor_details  			    as a 
                  left join exposure   			      as b on a.creditor_id = b.creditor_id
                  left join db_failure            as c on a.creditor_id = c.creditor_id
                  left join PD_score	            as d on a.creditor_id = d.creditor_id
                  left join creditor_balances     as e on a.creditor_id = e.creditor_id
                  left join creditor_payments     as f on a.creditor_id = f.creditor_id
                  left join tickets               as g on a.organisation_id = g.organisation_id
)

/******************************************************************************************************/
/******************************************  Payload & Logic  *****************************************/
/******************************************************************************************************/
,payload as (
select * from data_merge
where date(next_review_date) = current_date()
)


/******************************************************************************************************/
/**************************************  Action Fields   **********************************************/
/******************************************************************************************************/
select * 

			,'credit_re_review' as process_name

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
                STRUCT(28480929, 'credit__monitoring_rr')  -- Category
                -- STRUCT(15542500163356, '12345')  -- Exposure
                -- STRUCT(15545615128732, '123')  -- Fraud score (uncomment if needed)

            ] AS custom_fields,

            -- Comment object
	 STRUCT(
				'**Merchant Details:**'
				|| '\n' || '**Creditor ID:** [' || COALESCE(creditor_id, 'N/A') || '](https://manage.gocardless.com/admin/creditors/' || COALESCE(creditor_id, 'N/A') || ')'
				|| '\n' || '**Organisation ID:** ' || COALESCE(organisation_id, 'N/A')
				|| '\n' || '**Merchant name:** ' || COALESCE(merchant_name, 'N/A')
				|| '\n' || '**Geo:** ' || COALESCE(geo, 'N/A')
				|| '\n' || '**MCC:** ' || COALESCE(merchant_category_code_description, 'N/A')
				|| '\n' || '**Payment provider:** ' || COALESCE(cast(is_payment_provider as string), 'N/A')
				|| '\n' || '**Account Type:** ' || COALESCE(account_type, 'N/A')
				|| '\n' || '**CS Managed:** ' || COALESCE(cast(is_cs_managed as string), 'N/A')
				|| '\n' || '**CS Manager Name:** ' || COALESCE(csm_owner_name, 'N/A')

				|| '\n\n' || '**Parent Information:**'
				|| '\n' || '**Parent ID:** ' || COALESCE(parent_account_id, 'N/A')
				|| '\n' || '**Parent Name:** ' || COALESCE(parent_account_name, 'N/A')

				|| '\n\n' || '**Risk Labels:**'
				|| '\n' || '**Risk Label:** ' || COALESCE(merchant_risk_label_description, 'N/A')
				|| '\n' || '**Risk Label Date:** ' || COALESCE(cast(date(most_recent_risk_label_created_at) as string), 'N/A')

				|| '\n\n' || '**Failure Score:**'
				|| '\n' || '**D&B Score:** ' || COALESCE(cast(db_failure_score_current as string), 'N/A')
				|| '\n' || '**D&B Score date:** ' || COALESCE(cast(db_failure_score_current_date as string), 'N/A')
				|| '\n' || '**Internal PD Score:** ' || COALESCE(cast(round(PD_score_latest,2) as string), 'N/A')
				|| '\n' || '**Internal PD Score date:** ' || COALESCE(CAST(prediction_calendar_date AS STRING), 'N/A')

        || '\n\n' || '**Negative Balance:**'
				|| '\n' || '**Current Negative Balance:** £' || COALESCE(CAST(nb_balance_current AS STRING FORMAT '999,999,999.0'), 'N/A')

				|| '\n\n' || '**Payment Information:**'
				|| '\n' || '**FDS Exposure:** £' || COALESCE(CAST(fds_exposure_current AS STRING FORMAT '999,999,999.0'), 'N/A')
				|| '\n' || '**Payments last 12m:** £' || COALESCE(CAST(merchant_payment_amt_gbp_last_365d AS STRING FORMAT '999,999,999.0'), 'N/A')
				|| '\n' || '**Chargeback rate (90days):** ' || COALESCE(CAST(cb_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Failure rate (90days):** ' || COALESCE(CAST(failure_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Late Failure rate (90days):** ' || COALESCE(CAST(late_failure_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Refund rate (90days):** ' || COALESCE(CAST(refund_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'

		    || '\n\n' || '**Original ticket created at:** ' || date(ticket_created_at)
		    || '\n' || '**Previous ticket link here:** [' || COALESCE(ticket_id, 0)  || '](https://gocardless.zendesk.com/agent/tickets/' || COALESCE(ticket_id, 0)  || ')'
        || '\n' || '**Stated review reason:** ' || COALESCE(reason_for_next_review, 'N/A') 

				|| '\n\n' || '**Link to underwriter’s dashboard:** [Underwriter Dashboard](https://looker.gocardless.io/dashboards/3505?Organisation+ID=' || COALESCE(organisation_id, 'N/A') || '&Creditor+ID=&Company+Number=)'
				|| '\n\n\n' || 'Created by OtterNet'
		AS body,
              false AS public
            ) AS comment,

            -- Subject
            'Credit Monitoring - Re-Review - ' || merchant_name || ' - ' || creditor_id AS subject


        ) AS ticket
				)) AS ActionField_ZendeskCreateTicket


from payload
