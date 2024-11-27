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
  ,merchant_chargeback_vol_last_30d

	,SAFE_DIVIDE(merchant_chargeback_vol_last_90d,merchant_payment_vol_last_90d) as cb_rate_90days
	,SAFE_DIVIDE(merchant_failure_vol_last_90d,merchant_payment_vol_last_90d) as failure_rate_90days
	,SAFE_DIVIDE(merchant_late_failure_vol_last_90d,merchant_payment_vol_last_90d) as late_failure_rate_90days
	,SAFE_DIVIDE(merchant_refund_vol_last_90d,merchant_payment_vol_last_90d) as refund_rate_90days

	,merchant_payment_amt_gbp_last_365d

	from creditor_payments_temp)

/******************************************************************************************************/
/******************************   Additional CB Monitoring Data ***************************************/
/******************************************************************************************************/
,mcc_payments as (
									select 
										b.merchant_category_code
											,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end) as mcc_payment_vol_last_12m
											,sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as mcc_payment_amt_last_12m
											,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end) as mcc_chargeback_vol_last_12m
											,sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then amount_gbp else 0 end) as mcc_chargeback_amt_last_12m
										,safe_divide(sum(case when is_charged_back  and date(charged_back_date) between current_date()-365   and current_date()-1    then 1          else 0 end),sum(case when is_paid          and date(charge_date)       between current_date()-365   and current_date()-1    then 1          else 0 end)) as mcc_chargeback_rate_vol_last_12m
									from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` as a 
									left join `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as b
									on a.creditor_id=b.id
									where not b.is_payment_provider
									group by 1)

,portfolio_payments as (
												select
													1 as var1
														,sum(mcc_payment_vol_last_12m) as portfolio_payment_vol_last_12m
														,sum(mcc_payment_amt_last_12m) as portfolio_payment_amt_last_12m
														,sum(mcc_chargeback_vol_last_12m) as portfolio_chargeback_vol_last_12m
														,sum(mcc_chargeback_amt_last_12m) as portfolio_chargeback_amt_last_12m
													,safe_divide(sum(mcc_chargeback_vol_last_12m),sum(mcc_payment_vol_last_12m)) as portfolio_chargeback_rate_vol_last_12m
												from mcc_payments
												group by 1)

/******************************************************************************************************/
/*************************************  Historical Cases  *********************************************/
/******************************************************************************************************/
,tickets as (
																		select
																				JSON_VALUE(values, "$.creditor_id") AS creditor_id
																				,JSON_VALUE(values, "$.ticket_id") AS ticket_id
																				,cast(JSON_VALUE(values, "$.cb_rate_30days") as FLOAT64) AS chargeback_monitoring_rate_last
                                        ,cast(JSON_VALUE(values, "$.merchant_chargeback_vol_last_30d") as FLOAT64) AS merchant_chargeback_vol_last_30d_previous_trigger
																				,date(runtime) AS chargeback_monitoring_date_last
																				,date_diff(current_date(),date(runtime),day) AS chargeback_monitoring_days_since
																				,true AS chargeback_monitoring_trigger_last
																				,row_number() over (partition by JSON_VALUE(values, "$.creditor_id"),process_name order by runtime desc) as rowno

																		from `gc-prd-credit-risk-dev-81b5.otternet_dev.otternet_devlog` 
																		where logtype = "result"
																		and process_name = "credit_chargeback_monitoring"
																		qualify rowno = 1
)


/******************************************************************************************************/
/******************************************  Data Merge  **********************************************/
/******************************************************************************************************/
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
                    ,f.cb_rate_30days
                    ,f.merchant_chargeback_vol_last_30d

                    ,g.mcc_payment_vol_last_12m
                    ,g.mcc_payment_amt_last_12m
                    ,g.mcc_chargeback_vol_last_12m
                    ,g.mcc_chargeback_amt_last_12m
                    ,g.mcc_chargeback_rate_vol_last_12m

                    ,h.portfolio_payment_vol_last_12m
                    ,h.portfolio_payment_amt_last_12m
                    ,h.portfolio_chargeback_vol_last_12m
                    ,h.portfolio_chargeback_amt_last_12m
                    ,h.portfolio_chargeback_rate_vol_last_12m

                  ,case when g.mcc_chargeback_vol_last_12m >= 1000 then g.mcc_chargeback_rate_vol_last_12m else h.portfolio_chargeback_rate_vol_last_12m end as reference_cb_rate

                    ,i.ticket_id
                    ,i.chargeback_monitoring_rate_last
                    ,i.chargeback_monitoring_date_last
                    ,i.chargeback_monitoring_days_since
                    ,i.chargeback_monitoring_trigger_last
                    ,i.merchant_chargeback_vol_last_30d_previous_trigger


                  from creditor_details  			    as a 
                  left join exposure   			      as b on a.creditor_id = b.creditor_id
                  left join db_failure            as c on a.creditor_id = c.creditor_id
                  left join PD_score	            as d on a.creditor_id = d.creditor_id
                  left join creditor_balances     as e on a.creditor_id = e.creditor_id
                  left join creditor_payments     as f on a.creditor_id = f.creditor_id

                  left join mcc_payments			    as g on a.merchant_category_code = g.merchant_category_code
                  left join portfolio_payments    as h on a.var1 = h.var1
                  left join tickets  	            as i on a.creditor_id = i.creditor_id
)


/******************************************************************************************************/
/******************************************  Payload & Logic  *****************************************/
/******************************************************************************************************/
,payload as (
select * 
,case when (fds_exposure_current >= 250000 and db_failure_score_current < 40) or (nb_balance_current <= -20000) or (fds_exposure_current >= 500000) then 1 else 0 end as merchant_monitoring_qualifyer
,case when (cb_rate_30days - reference_cb_rate >= 0.04) and chargeback_monitoring_trigger_last is null then "New Alert"
      when (cb_rate_30days - chargeback_monitoring_rate_last >= 0.1) and (merchant_chargeback_vol_last_30d_previous_trigger is null or merchant_chargeback_vol_last_30d_previous_trigger < merchant_chargeback_vol_last_30d) and chargeback_monitoring_trigger_last = true then "Chargeback Rate Increase >10% since last trigger"
      when (cb_rate_30days - reference_cb_rate >= 0.04) and chargeback_monitoring_trigger_last = true and chargeback_monitoring_days_since >= 90 then "Chargeback Rate Re-Trigger after 90 days"
	  else "No Alert"
	  end as chargeback_monitoring_alert
from data_merge)


/******************************************************************************************************/
/**************************************  Action Fields   **********************************************/
/******************************************************************************************************/
SELECT *
	,'credit_chargeback_monitoring' AS process_name

	,TO_JSON_STRING(STRUCT(
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
				-- Custom field entries
				STRUCT(28480929, 'credit__monitoring_cb')  -- Category
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

				|| '\n\n' || '**Chargeback monitoring details:** '
				|| '\n' || '**Case Type:** ' || COALESCE(chargeback_monitoring_alert, 'N/A')

				|| CASE WHEN chargeback_monitoring_trigger_last THEN
				'\n' || '**Previous ticket link here:** [' || ticket_id || '](https://gocardless.zendesk.com/agent/tickets/' || ticket_id || ')'
				|| '\n' || '**CB Rate Previously:** ' || COALESCE(CAST(chargeback_monitoring_rate_last * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**CB Rate Now:** ' || COALESCE(CAST(cb_rate_30days  * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Reference CB Rate:** ' || COALESCE(CAST(reference_cb_rate  * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'				
				|| '\n' || '**Date Last Trigger:** ' || COALESCE(cast(date(chargeback_monitoring_date_last) as string), 'N/A') 
				|| '\n' || '**Days Since Last Trigger:** ' || COALESCE(cast(chargeback_monitoring_days_since as string), 'N/A') 				
				ELSE 
				'\n' || '**CB Rate:** ' || COALESCE(CAST(cb_rate_30days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Reference CB Rate:** ' || COALESCE(CAST(reference_cb_rate * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'				
				END

				|| '\n\n' || '**Link to underwriter’s dashboard:** [Underwriter Dashboard](https://looker.gocardless.io/dashboards/3505?Organisation+ID=' || COALESCE(organisation_id, 'N/A') || '&Creditor+ID=&Company+Number=)'
				|| '\n\n\n' || 'Created by OtterNet'
				AS body,
				false AS public
			) AS comment,

			-- Subject
			'Credit Monitoring - Chargebacks - ' || COALESCE(merchant_name, 'N/A') || ' - ' || COALESCE(creditor_id, 'N/A') AS subject
		) AS ticket
	)) AS ActionField_ZendeskCreateTicket
FROM payload
WHERE merchant_monitoring_qualifyer = 1 
AND chargeback_monitoring_alert IN ("New Alert","Chargeback Rate Increase >10% since last trigger","Chargeback Rate Re-Trigger after 90 days")
