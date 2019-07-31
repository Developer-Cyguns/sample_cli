--8ball--

CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success AS (  
SEL
evnt_key,
a.evnt_dt,
evnt_ts_epoch + evnt_ts_msecs AS evnt_ts,
geo_cntry,
COALESCE(CAST(td_sysfnlib.NVP(payload,'event_time', '&', '=') AS BIGINT),0) AS event_time,
sessn_id  AS user_session_guid,
COALESCE(is_bot_y_n,'#') AS bot_flag_yn,
COALESCE(buyer_ip,'#') AS buyer_ip,
COALESCE(state_name,'#') AS state_name,
COALESCE(context_id,'#') AS context_id,
COALESCE(transition_name,'#') AS transition_name,
COALESCE(CAST(td_sysfnlib.NVP(payload,'sub_state_name', '&', '=') AS VARCHAR(100)),'#') AS sub_state_name,
COALESCE(CAST(td_sysfnlib.NVP(payload,'ul_process_state', '&', '=') AS VARCHAR(100)),'#') AS ul_process_state,
COALESCE(int_error_code,'#') AS int_error_code,
COALESCE(int_error_desc,'#') AS int_error_desc,
COALESCE(CAST(td_sysfnlib.NVP(payload,'login_status', '&', '=') AS VARCHAR(256)),'#') AS login_status,
COALESCE(CAST(td_sysfnlib.NVP(payload,'channel', '&', '=') AS VARCHAR(100)),'#') AS channel,
COALESCE(CAST(td_sysfnlib.NVP(payload,'device_type', '&', '=') AS VARCHAR(100)),'#') AS device_type,
COALESCE(CAST(td_sysfnlib.NVP(payload,'browser_type', '&', '=') AS VARCHAR(100)),'#') AS browser_type,
COALESCE(component,'#') AS component,
COALESCE(CAST(td_sysfnlib.NVP(payload,'client_os', '&', '=') AS VARCHAR(100)),'#') AS client_os,
COALESCE(CAST(td_sysfnlib.NVP(payload,'device_name', '&', '=') AS VARCHAR(100)),'#') AS device_name,
COALESCE(CAST(td_sysfnlib.NVP(payload,'token_status', '&', '=') AS VARCHAR(256)),'#') AS token_status,
COALESCE(Cast(td_sysfnlib.Nvp(payload,'login_error', '&', '=') AS VARCHAR(100)),'#') AS login_error,
COALESCE(CAST(td_sysfnlib.NVP(payload,'state_status', '&', '=') AS VARCHAR(20)),'#') AS state_status

FROM 
pp_polestar_views.fact_ea_evnt a
WHERE
biz_evnt_key = 37 
AND component IN ('unifiedloginnodeweb', 'authchallengenodeweb')
AND a.evnt_dt = current_date - 2
AND user_Session_guid <> '#' and context_id = '#' 
and user_Session_guid not in (sel user_Session_guid from PP_PRODUCT_ANALYTICS_DATASTORE_VIEWS.thirdpartylogin_dataset where evnt_dt = current_date - 2 group by 1)
) WITH DATA  PRIMARY INDEX (evnt_key);



CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success1 AS 
(  
SEL 
a.*   
FROM
pp_oap_sing_pavithra_t.dashboard_login_success a

WHERE component = 'unifiedloginnodeweb'
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_session_guid ORDER BY event_time) =1 
) WITH DATA  unique PRIMARY INDEX (user_Session_guid);

CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success2 as 
(
SEL 
a.*           
,'0' AS  ul_called                     
,'0' AS  login_shown                   
,'0' AS  login_submit                  
,'0' AS  login_success                 
,'0' AS login_success_sts
FROM
pp_oap_sing_pavithra_t.dashboard_login_success a

WHERE component = 'unifiedloginnodeweb'

QUALIFY ROW_NUMBER() OVER(PARTITION BY user_session_guid ORDER BY event_time Desc) =1
) WITH DATA  unique PRIMARY INDEX (user_Session_guid);


CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success3 as 
(
SELECT 
User_session_guid,
MIN(evnt_dt) AS evnt_dt,
MAX(sub_state_name) as sub_state_name,    
MAX(channel) as channel,      
MAX(device_type) as device_type,     
MAX(browser_type) as browser_type,      
MAX(client_os) as client_os,   
CAST ('#' AS VARCHAR (100)) AS state_name,
CAST ('#' AS VARCHAR (100)) AS transition_name,
CAST ('#' AS VARCHAR (100)) AS int_error_code,


MAX(CASE WHEN ul_process_state = 'begin_ul' THEN 1 ELSE 0 end) AS ul_called,

--------- split screen login page ----------------
MAX(CASE WHEN state_name IN ('begin_email','begin_phone') AND transition_name IN ('prepare_email','prepare_phone','prepare_pin') THEN 1 ELSE 0 end) AS email_page_rendered_y_n,
MAX(CASE WHEN (state_name IN ('begin_email','begin_phone') AND transition_name IN ('process_email','process_next')) OR (state_name = 'login_ul_rm' AND transition_name = 'process_login_ul' AND token_status <> '#'  ) THEN 1 ELSE 0 end) AS email_page_submit_y_n,
MAX(CASE WHEN state_name IN ('begin_email','begin_phone') AND transition_name IN ('process_email','process_phone') AND int_error_code = '#' AND int_error_desc = '#' and login_error = '#' THEN 1 ELSE 0 end) AS email_page_success_y_n,
-------- split screen password page -------------
MAX(CASE WHEN ((state_name IN ('begin_pwd','begin_pin') AND transition_name IN ('prepare_pwd_ot_more_opt','prepare_pwd_more_opt','prepare_pwd','prepare_pwd_ot','prepare_pin')) OR (state_name LIKE 'login_ul%' AND transition_name LIKE 'prepare_login_ul' AND transition_name <> 'prepare_login_ul_rm')) THEN 1 ELSE 0 end)AS pwd_page_rendered_y_n,
MAX(CASE WHEN 
	state_name IN ('begin_pwd','begin_phone_pin','begin_phone_pwd','login_ul_rm','login_ul') AND 
	transition_name IN ('process_pwd_ot','process_pwd','process_phone_pwd','process_login_ul_rm','process_login_ul','process_stepupRequired','process_safeRequired','process_2fa')   THEN 1 ELSE 0 end) AS pwd_page_submit_y_n,
MAX(CASE WHEN state_name IN ('begin_pwd','begin_phone_pin','begin_phone_pwd','login_ul_rm','login_ul') AND transition_name IN ('process_pwd_ot','process_pwd','process_phone_pwd','process_login_ul_rm','process_login_ul') AND int_error_code = '#' AND int_error_desc = '#' and login_error = '#' THEN 1 ELSE 0 end) AS pwd_page_success_y_n,

---------- Normal login flow--------------------------
MAX(CASE WHEN state_name LIKE 'login_ul%' AND transition_name LIKE 'prepare_login_ul%' THEN 1 ELSE 0 end) AS login_pg_rendered_y_n,
MAX(CASE WHEN state_name LIKE 'login_ul%' AND (transition_name LIKE 'process_login_ul%' or transition_name in ('process_stepupRequired','process_safeRequired','process_2fa') )  THEN 1 ELSE 0 end) AS login_pg_submit_y_n,
MAX(CASE WHEN state_name LIKE 'login_ul%' AND transition_name LIKE 'process_login_ul%' AND int_error_code = '#' AND int_error_desc = '#' and login_error = '#' THEN 1 ELSE 0 end) AS login_pg_success_y_n,



MAX(CASE WHEN login_status = 'success' THEN 1 ELSE 0 end) AS login_success_status,
MAX(CASE WHEN (a.State_name <> '#' OR a.transition_name <> '#') AND login_status = 'failed' THEN 1 ELSE 0 end) AS login_failure_status,
MAX(CASE WHEN (a.State_name = '#' and a.transition_name = '#') AND login_status = 'failed' THEN 1 ELSE 0 end) AS login_failure_status_unknown,
MAX(CASE WHEN login_status = 'challenge' THEN 1 ELSE 0 end) AS login_challenge,
MAX(CASE WHEN transition_name = 'process_signup' THEN 1 ELSE 0 end) AS signup_click,


------------- Hybrid login page ------------------
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name = 'prepare_hybrid' THEN 1 ELSE 0 end) AS hybrid_page_rendered_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name IN ('process_hybrid','process_next')  THEN 1 ELSE 0 end) AS hybrid_page_submit_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_login' AND transition_name IN ('process_hybrid','process_next') AND Coalesce(int_error_code,'#') = '#' and  login_error = '#' THEN 1 ELSE 0 end) AS hybrid_page_success_y_n,

----------- Hybrid password page ---------------
Max(CASE WHEN state_name = 'begin_hybrid_pwd' AND transition_name IN ('prepare_hybrid_pwd', 'prepare_hybrid_pwd_ot') THEN 1 ELSE 0 end) AS hybrid_pwd_page_rendered_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name IN ('process_hybrid_pwd','process_hybrid_pwd_ot','process_2fa','process_stepupRequired','process_safeRequired')  THEN 1 ELSE 0 end) AS hybrid_pwd_page_submit_y_n,
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name IN ('process_hybrid_pwd', 'process_hybrid_pwd_ot') AND Coalesce(int_error_code,'#') = '#' and login_error = '#' THEN 1 ELSE 0 end) AS hybrid_pwd_page_success_y_n,

-- change in hybrid cookied
Max(CASE WHEN state_name = 'begin_hybrid_pwd'  AND transition_name = 'process_hybrid_pwd_not_you' THEN 1 ELSE 0 end) AS hybrid_pwd_page_change_y_n,


MAX(CASE WHEN state_name = 'PAYPAL_CAPTCHA_SERVED' AND state_status='SUCCESS' THEN 1 ELSE 0 end) AS pp_captcha_served,
MAX(CASE WHEN state_name = 'PAYPAL_CAPTCHA_VALIDATION' AND state_status='SUCCESS' THEN 1 ELSE 0 end) AS pp_captcha_success,
MAX(CASE WHEN state_name = 'GOOGLE_RECAPTCHA_SERVED' AND state_status='SUCCESS' THEN 1 ELSE 0 end) AS  google_captcha_served,
MAX(CASE WHEN state_name = 'GOOGLE_RECAPTCHA_VALIDATION' AND state_status='SUCCESS' THEN 1 ELSE 0 end) AS google_captcha_success,

CASE WHEN ((((ul_called = 1 and (email_page_rendered_y_n = 1  OR pwd_page_rendered_y_n = 1  OR login_pg_rendered_y_n = 1  or hybrid_page_rendered_y_n = 1  or hybrid_pwd_page_rendered_y_n = 1))) or  ((ul_called = 1 and email_page_rendered_y_n <> 1  and pwd_page_rendered_y_n <> 1  and login_pg_rendered_y_n <> 1  and hybrid_page_rendered_y_n <> 1  and hybrid_pwd_page_rendered_y_n <> 1 and hybrid_pwd_page_submit_y_n <> 1))) ) THEN 1 ELSE 0 end AS ul_called2,
CASE WHEN email_page_rendered_y_n = 1  OR pwd_page_rendered_y_n = 1  OR login_pg_rendered_y_n = 1  or hybrid_page_rendered_y_n = 1  or hybrid_pwd_page_rendered_y_n = 1   THEN 1 ELSE 0 end AS login_shown,
CASE WHEN login_shown = 1 and (pwd_page_submit_y_n = 1  OR login_pg_submit_y_n = 1   OR login_failure_status = 1 OR login_challenge = 1    or  hybrid_pwd_page_submit_y_n = 1)   THEN 1 ELSE 0 end AS login_submit,
CASE WHEN login_submit = 1 and  (pwd_page_success_y_n = 1 OR login_pg_success_y_n = 1 or hybrid_pwd_page_success_y_n = 1 OR login_success_status = 1) THEN 1 ELSE 0 end AS login_success,

'N' AS is_deduped_y_n2,
CAST ('#' AS VARCHAR (100)) AS Deduped_sessn_id2,
CAST ('#' AS VARCHAR (100)) AS flow_stage,
CAST ('#' AS VARCHAR (100))AS last_Error_Trsntn_state,
CAST ('#' AS VARCHAR (100)) AS flow_stage_UL,
CAST ('#' AS VARCHAR (100))AS last_Error_Trsntn_state_UL,
MAX(geo_cntry) as geo_cntry,
CAST ('#' AS VARCHAR (100)) as login_submit_error,
CAST ('#' AS VARCHAR (100)) AS  tpl_client_id,             
CAST ('#' AS VARCHAR (100)) AS  tpl_client_name, 
0 AS login_success_sts

FROM
pp_oap_sing_pavithra_t.dashboard_login_success a
GROUP BY 1 
) WITH DATA  unique PRIMARY INDEX (user_Session_guid);



CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success_STS as 
(
sel
cast(td_sysfnlib.nvp(payload,'user_session_guid_hdr', '&', '=') as varchar(100)) as user_session_guid
from pp_polestar_views.fact_ea_evnt a 
where a.evnt_dt =  current_date - 2
and  biz_evnt_key = 140 and component = 'identitysecuretokenserv' 
and COALESCE(CAST(td_sysfnlib.NVP(a.payload,'error_code', '&', '=') AS VARCHAR(256)),'#') = 'OK'
and  api_name ='/v1/oauth2/login' and user_session_guid is not null
group by 1
)WITH DATA  unique PRIMARY INDEX (user_Session_guid);


CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success4 as 
(
SELECT 
a.user_session_guid AS bogus_sessn_id,
b.user_session_guid AS actual_sessn_id,
b.evnt_ts AS actual_ts
FROM pp_oap_sing_pavithra_t.dashboard_login_success2  a 
JOIN pp_oap_sing_pavithra_t.dashboard_login_success1  b 
ON a.channel = b.channel
AND a.device_type = b.device_type
AND a.browser_type = b.browser_type
AND a.client_os = b.client_os
AND a.device_name = b.device_name
AND a.sub_state_name = b.sub_state_name
AND a.buyer_ip = b.buyer_ip
AND a.ul_process_state = 'begin_ul'
AND a.login_shown = 0 
AND a.user_session_guid <> b.user_session_guid
AND b.evnt_ts BETWEEN a.evnt_ts AND a.evnt_ts + 17

WHERE  
a.sub_state_name<> '#' AND             
a.buyer_ip<> '#' AND                   
a.Device_type<> '#' AND                    
a.browser_type<> '#' AND                   
a.Device_name<> '#' AND                    
a.client_os<> '#' AND             
a.channel<> '#' 
QUALIFY ROW_NUMBER() OVER(PARTITION BY bogus_sessn_id ORDER BY  actual_ts) = 1
) WITH DATA  unique PRIMARY INDEX (bogus_sessn_id);


CREATE MULTISET TABLE pp_oap_sing_pavithra_t.dashboard_login_success5 as 
(
SELECT 
Deduped_sessn_id2
,MAX ( ul_called)     ul_called                
,MAX ( email_page_rendered_y_n)  email_page_rendered_y_n
,MAX ( email_page_submit_y_n)  email_page_submit_y_n
,MAX ( email_page_success_y_n)  email_page_success_y_n
,MAX ( pwd_page_rendered_y_n)  pwd_page_rendered_y_n						
,MAX ( pwd_page_submit_y_n)  pwd_page_submit_y_n
,MAX ( pwd_page_success_y_n)pwd_page_success_y_n
,MAX ( login_pg_rendered_y_n)  login_pg_rendered_y_n
,MAX ( login_pg_submit_y_n)  login_pg_submit_y_n
,MAX ( login_pg_success_y_n)  login_pg_success_y_n
,MAX ( login_success_status)  login_success_status
,MAX ( login_failure_status)  login_failure_status
,MAX ( login_failure_status_unknown)  login_failure_status_unknown
,MAX ( login_challenge)  login_challenge
,MAX ( signup_click)  signup_click
,MAX ( pp_captcha_served) pp_captcha_served 
,MAX ( pp_captcha_success)  pp_captcha_success
,MAX ( google_captcha_served)  google_captcha_served
,MAX ( google_captcha_success)  google_captcha_success

,max(hybrid_page_rendered_y_n) hybrid_page_rendered_y_n
,max(hybrid_page_submit_y_n) hybrid_page_submit_y_n
,max(hybrid_page_success_y_n) hybrid_page_success_y_n
,max(hybrid_pwd_page_rendered_y_n) hybrid_pwd_page_rendered_y_n
,max(hybrid_pwd_page_submit_y_n) hybrid_pwd_page_submit_y_n
,max(hybrid_pwd_page_success_y_n) hybrid_pwd_page_success_y_n
,max(hybrid_pwd_page_change_y_n) hybrid_pwd_page_change_y_n

,MAX ( ul_called2)  ul_called2
,MAX ( login_shown)  login_shown
,MAX ( login_submit)  login_submit
,MAX ( login_success)   login_success
,MAX ( login_success_sts)   login_success_sts

FROM  pp_oap_sing_pavithra_t.dashboard_login_success3
GROUP BY 1
) WITH DATA  unique PRIMARY INDEX (Deduped_sessn_id2);


CREATE MULTISET TABLE pp_oap_sing_pavithra_t.error_table AS 
(
SELECT
a.* 

FROM 
pp_oap_sing_pavithra_t.dashboard_login_success A

WHERE 
(
(
state_name IN ('begin_pwd','begin_phone_pin','begin_phone_pwd','login_ul_rm','login_ul') AND 
	transition_name IN ('process_pwd_ot','process_pwd','process_phone_pwd','process_login_ul_rm','process_login_ul','process_stepupRequired','process_safeRquired','process_2fa')
)
OR
(state_name LIKE 'login_ul%' AND (transition_name LIKE 'process_login_ul%' or transition_name in ('process_stepupRequired','process_safeRquired','process_2fa')))
or login_status<> '#'	
)
or 
(state_name = 'begin_hybrid_pwd'  AND transition_name IN ('process_hybrid_pwd','process_hybrid_pwd_ot','process_2fa','process_stepupRequired','process_safeRequired') ) 


QUALIFY ROW_NUMBER() OVER ( PARTITION BY user_Session_guid ORDER BY evnt_ts DESC ) = 1
) WITH DATA UNIQUE PRIMARY INDEX (user_session_guid);

CREATE MULTISET TABLE pp_oap_sing_pavithra_t.login_final_summary AS (
SEL 
evnt_dt,
geo_cntry,
sub_state_name,				
channel,
browser_type,
client_os,
Device_type,
 Flow_Stage,
  Flow_Stage_UL,
CASE WHEN login_shown = 0 AND google_captcha_served = 1 THEN 'Y' ELSE 'N'end AS google_failure_y_n,
CASE WHEN login_shown = 0 AND login_failure_status_unknown = 1 THEN 'Y' ELSE 'N'end AS unknown_error_y_n,
CASE WHEN a.login_shown = 1 THEN 'Y' ELSE 'N' end AS login_rendered_y_n,
CASE WHEN a.login_shown = 1 and a.signup_click = 1 AND a.Login_submit = 0 THEN 'Y' ELSE 'N' end AS signUp_session,
Case when tpl_client_id = 'f29e246f4d66cbe690af709026ba4dc8' then 'Y' else 'N' end as is_ebaymp_y_n,
CASE WHEN a.login_success_sts = 1 THEN 'Success' ELSE state_name end AS Dropoff_state_name ,
CASE WHEN a.login_success_sts = 1 THEN 'Success' ELSE transition_name end AS Dropoff_transition_name ,
CASE WHEN a.login_success_sts = 1 THEN 'Success' ELSE int_error_code end AS Drop_off_error ,
last_Error_Trsntn_state,
last_Error_Trsntn_state_UL,
is_deduped_y_n2
,SUM (ul_called)      ul_called
,SUM (email_page_rendered_y_n)  email_page_rendered_y_n  
,SUM (email_page_submit_y_n)    email_page_submit_y_n
,SUM (email_page_success_y_n)    email_page_success_y_n
,SUM (pwd_page_rendered_y_n)    pwd_page_rendered_y_n
,SUM (pwd_page_submit_y_n)    pwd_page_submit_y_n
,SUM (pwd_page_success_y_n)    pwd_page_success_y_n
,SUM (login_pg_rendered_y_n)    login_pg_rendered_y_n
,SUM (login_pg_submit_y_n)    login_pg_submit_y_n
,SUM (login_pg_success_y_n)    login_pg_success_y_n
,SUM (login_success_status)    login_success_status
,SUM (login_failure_status)    login_failure_status
,SUM( login_failure_status_unknown )login_failure_status_unknown
,SUM( login_challenge)login_challenge
,SUM( signup_click)signup_click
,SUM( pp_captcha_served)pp_captcha_served
,SUM( pp_captcha_success)pp_captcha_success
,SUM( google_captcha_served)google_captcha_served
,SUM( google_captcha_success)google_captcha_success

,Sum(hybrid_page_rendered_y_n) hybrid_page_rendered_y_n
,Sum(hybrid_page_submit_y_n) hybrid_page_submit_y_n
,Sum(hybrid_page_success_y_n) hybrid_page_success_y_n
,Sum(hybrid_pwd_page_rendered_y_n) hybrid_pwd_page_rendered_y_n
,Sum(hybrid_pwd_page_submit_y_n) hybrid_pwd_page_submit_y_n
,Sum(hybrid_pwd_page_success_y_n) hybrid_pwd_page_success_y_n
,Sum(hybrid_pwd_page_change_y_n) hybrid_pwd_page_change_y_n

,SUM(ul_called2)ul_called2       
,SUM(login_shown)login_shown
,SUM(login_submit)login_submit       
,SUM(login_success)login_success1
,SUM(login_success_sts)login_success
,CAST ('#' AS VARCHAR (100)) as region

FROM
pp_oap_sing_pavithra_t.dashboard_login_success3 a

WHERE 
is_deduped_y_n2 = 'N'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

)WITH DATA PRIMARY INDEX (evnt_dt, sub_state_name, channel, browser_type, client_os, Device_type );

----------------------------------------------------------------------------------------------
----------Dolphin+Funnel+For+8ball+all+Countries-session[2]------------------------------------

------------DolphinMobileIDConfirmedPhones-latest-context--------------------------------
-----------------------------------------------------------------------------------------

--------------------------------Authflow[1]---------------------------------------------
-----------------------------------------------------------------------------------------


--------------------------------Hybrid---------------------------------------------
-----------------------------------------------------------------------------------------

--------------------------------XO_login_Rcvr -------------------------------------------
-----------------------------------------------------------------------------------------

--------------------------------------tdp------------------------------------------------


create multiset table pp_oap_sing_zhuali_t.tpd_ul_sessns_new as (
--delete from pp_oap_sing_zhuali_t.tpd_ul_sessns where evnt_dt = (select max_dt + 1 from max_dt);
--insert into pp_oap_sing_zhuali_t.tpd_ul_sessns
select
a.evnt_dt,
a.user_session_guid,

-- start
max(case when experimentation_experience like '%4090%' then 1 else 0 end) as tpd_expr,

max(case when state_name = 'begin_tpd' and transition_name ='process_tpd_autosend' then 1 else 0 end) as tpd_auto_start,
--begin_pwd	process_pwd_tpd_click
max(case when state_name = 'begin_tpd' and transition_name = 'process_tpd_tpdButton' then 1 else 0 end) as tpd_button_start,
--begin_pwd	process_pwd_more_opt
max(case when state_name = 'begin_tpd' and transition_name = 'process_tpd_moreOptions' then 1 else 0 end) as tpd_more_option_start,
-- sts and rcs
max(case when state_name = 'begin_sts_tpd' and transition_name = 'prepare_sts_tpd' then 1 else 0 end) as tpd_sts_start,
max(case when state_name = 'begin_sts_tpd' and transition_name = 'process_tpd_notification_failed' then 1 else 0 end) as tpd_sts_fail,
max(case when state_name = 'begin_tpd_rcs_call' and transition_name = 'prepare_rcs_response' then 1 else 0 end) as tpd_rcs_start,
max(case when state_name = 'begin_tpd_rcs_call' and transition_name = 'process_rcs_response' then 1 else 0 end) as tpd_rcs_success,
max(case when state_name = 'begin_tpd_rcs_call' and transition_name = 'process_tpd_notification_failed' then 1 else 0 end) as tpd_rcs_fail,
-- render
max(case when state_name = 'begin_tpd' and transition_name = 'prepare_verification' then 1 else 0 end) as tpd_notification_render,
-- click
--max(case when state_name = 'begin_tpd' and transition_name = 'process_use_pwd' then 1 else 0 end) as tpd_switch_pwd_1,
max(case when (state_name = 'begin_tpd' and transition_name = 'process_use_pwd') or state_name = 'begin_use_pwd' then 1 else 0 end) as tpd_switch_pwd_1,
max(case when state_name = 'begin_tpd' and transition_name = 'process_resend' then 1 else 0 end) as tpd_resend,
max(case when state_name = 'begin_tpd' and transition_name = 'process_not_you' then 1 else 0 end) as tpd_not_you,
--notiification
max(case when state_name = 'end_tpd_notification' and transition_name = 'process_tpd_notification_accepted' then 1 else 0 end) as tpd_notification_accepted,
max(case when state_name = 'end_tpd_notification' and transition_name = 'process_end_tpd_denied' then 1 else 0 end) as tpd_notification_denied,
max(case when state_name = 'end_tpd_notification' and transition_name = 'process_end_tpd_service_error' then 1 else 0 end) as tpd_notification_service_error,
max(case when state_name = 'end_tpd_notification' and transition_name = 'expired_tpd_no_action' then 1 else 0 end) as tpd_notification_expired,
max(case when state_name = 'complete_tpd_login' and transition_name = 'tpd_login_success' then 1 else 0 end) as tpd_success,

--pwd
MAX(CASE WHEN state_name in ('begin_pwd','begin_phone_pwd', 'begin_hybrid_pwd') AND transition_name in ('process_pwd','process_pwd_ot','process_phone_pwd','process_phone_pwd_ot','process_hybrid_pwd','process_hybrid_pwd_ot') AND int_error_code = '#' and login_error = '#' THEN 1 ELSE 0 end) AS pwd_success,

max(login_status) as login_status,

max(device_type) as device_type,
max(is_tpd) as is_tpd_optin,
max(buyer_id) as buyer_id,

cast('#' as varchar(100)) as new_existing,
cast('#' as varchar(100)) as is_cookied,
cast('#' as varchar(100))  as tpd_autosend,
cast('#' as varchar(100)) as pxp_test_group,

min(docid) as docid_min,
max(docid) as docid_max
from pp_oap_sing_rt_t.tpd_ul_evnts a
--left join pp_oap_sing_rt_t.user_sessn_cookied b on a.user_session_guid = b.user_session_guid
where 1=1
group by 1,2
having is_tpd_optin = 'Y' 
) with data primary index(user_session_guid)
;


create multiset table pp_oap_sing_zhuali_t.tpd_login_venice_evnts as (
--delete from pp_oap_sing_zhuali_t.tpd_login_venice_evnts where sessn_start_dt between '2018-05-02' and '2018-05-04';
--insert into pp_oap_sing_zhuali_t.tpd_login_venice_evnts
SELECT
      a.sessn_start_dt ,
	  a.page_name,
	  a.page_group as page_grp_name,
	  sessn_id,
	  cust_id,
	  client_os,
	 page_grp_lnk_name,
	 coalesce(cast(td_sysfnlib.NVP(payload,'webdocid','&','=') AS varchar(500)),'#') as webdocid
FROM  
   pp_polestar_views.web_evnt a
where 1=1
and evnt_dt = (select max_dt + 1 from max_dt)
and page_name  like any ('%consapp:tpd:%', '%consapp:pushnotification::notificationreceipt%','%consapp:ato::success%','mobile:consapp:pushnotification::tpd%')
 ) with data primary index(sessn_id)
   ;


-----------------------INSERT----------------------------------------------------------------------

insert into pp_oap_sing_karthikeyan_t.KA_login_8ball_summary
select 
evnt_dt,
geo_cntry,
sub_state_name,				
channel,
browser_type,
client_os,
Device_type,
Flow_Stage,
Flow_Stage_UL,
google_failure_y_n,
unknown_error_y_n,
login_rendered_y_n,
signUp_session,
is_ebaymp_y_n,
Dropoff_state_name ,
Dropoff_transition_name ,
Drop_off_error ,
last_Error_Trsntn_state,
last_Error_Trsntn_state_UL,
is_deduped_y_n2
,ul_called
,email_page_rendered_y_n  
,email_page_submit_y_n
,email_page_success_y_n
,pwd_page_rendered_y_n
,pwd_page_submit_y_n
,pwd_page_success_y_n
,login_pg_rendered_y_n
,login_pg_submit_y_n
,login_pg_success_y_n
,login_success_status
,login_failure_status
,login_failure_status_unknown
,login_challenge
,signup_click
,pp_captcha_served
,pp_captcha_success
,google_captcha_served
,google_captcha_success
,ul_called2       
,login_shown
,login_submit       
,login_success
,login_success1
,region
from pp_oap_sing_pavithra_t.login_final_summary;


insert into  pp_oap_sing_karthikeyan_t.KA_login_8ball_final_summary 

sel 
evnt_dt,
geo_cntry as Country,
region as Region,
case when sub_state_name like any ('mweb%') then 'm-web         '
         when sub_state_name like any ('web%') then 'web'
		 when sub_state_name like any ('unknown%') then 'unknown'
		 when sub_state_name = '#' then '#' 
		 else 'Others' end as sub_state_name_grp,
case when channel in ('mobile') then 'mobile         '
         when channel in ('tablet') then 'tablet'
		 when channel in ('web') then 'web'
		 else 'Others' end as channel_type,
Case when (channel_type = 'mobile' or channel_type = 'tablet') then 'Mobile' 
         when (channel_type = 'web') then 'Web'  else 'Others' end as Login_Channel,
case when browser_type IN ('S','Sa','Saf','Safa','Safar','Safari','SF')  then 'Safari                          '
         when browser_type IN ('CH','Chr','Chro','Chrom','Chrome') THEN 'Chrome'
		 WHEN browser_type IN ('Chrome M','Chrome Mo','Chrome Mob','Chrome Mobi','Chrome Mobil','Chrome Mobile') THEN 'Chrome Mobile'
		 WHEN browser_type IN ('F','FF','Fi','Fir','Fire','Firef','Firefo','Firefox') THEN 'Firefox'
		 WHEN browser_type IN ('Sam','Sams','Samsu','Samsun','Samsung ','Samsung B','Samsung Br','Samsung Bro','Samsung Brow','SM','Samsung Brows','Samsung Browse','Samsung Browser') THEN 'Samsung Browser'
		 WHEN browser_type IN ('I','IE','In','Int','Inte','Inter','Intern','Interne','Internet''Internet E','Internet Ex','Internet Exp','Internet Expl','Internet Explo','Internet Explor','Internet Explore','Internet Explorer') THEN 'Internet Explorer'
		 else 'Others' end as Browser,
case when client_os like any ('Windows%') then 'Windows         '
         when client_os like any ('(iPad%', '(iPhone%','iOS%') then 'iOS'
		 when client_os like any ('And%') then 'Andriod'
		 when client_os like any ('Lin%') then 'Linux'
		 else 'Others' end as Device_OS,
case when Device_type in ('MOBILE','Mobile Phone') then 'Mobile         '
         when Device_type in ('TABLET') then 'Tablet'
		 when Device_type in ('DESKTOP') then 'Desktop'
		 else 'Others' end as Device,
google_failure_y_n,   
unknown_error_y_n as Invalid_user_error,  
signUp_session as Signup_Session_Flag,   
Flow_Stage, 
Flow_Stage_UL,               
case when last_Error_Trsntn_state in 
(
'invalid_user'
,'GOOGLE_RECAPTCHA_SERVED'
,'PRE_JSCHALLENGE_SERVED'
,'ss_prepare_email'
,'process_hybrid_pwd'
,'invalid_public_credential'
,'process_begin_ul'
,'ss_prepare_pwd'
,'PRE_JSCHALLENGE_VERIFICATION'
,'safe_required'
,'process_gsl_session_check'
,'process_signup'
,'prepare_hybrid_pwd'
,'POST_JSCHALLENGE_VERIFICATION2'
,'PAYPAL_CAPTCHA_SERVED'
,'prepare_pwd'
,'POST_JSCHALLENGE_VERIFICATION0'
,'2fa_required'
,'stepup_required'
,'max_attempts_exceeded'
,'risk_decline'
,'locked_user'
,'login_with_unconfirmed_email'
) then 	 last_Error_Trsntn_state else 'Others' end as last_Error_Trsntn_state,
case when last_Error_Trsntn_state_UL in 
(
'invalid_user'
,'GOOGLE_RECAPTCHA_SERVED'
,'PRE_JSCHALLENGE_SERVED'
,'ss_prepare_email'
,'process_hybrid_pwd'
,'invalid_public_credential'
,'process_begin_ul'
,'ss_prepare_pwd'
,'PRE_JSCHALLENGE_VERIFICATION'
,'safe_required'
,'process_gsl_session_check'
,'process_signup'
,'prepare_hybrid_pwd'
,'POST_JSCHALLENGE_VERIFICATION2'
,'PAYPAL_CAPTCHA_SERVED'
,'prepare_pwd'
,'POST_JSCHALLENGE_VERIFICATION0'
,'2fa_required'
,'stepup_required'
,'max_attempts_exceeded'
,'risk_decline'
,'locked_user'
,'login_with_unconfirmed_email'
) then 	 last_Error_Trsntn_state_UL else 'Others' end as last_Error_Trsntn_state_UL,
sum(ul_called)as ul_called ,   
sum(ul_called2) as ul_called2 ,       
sum(login_shown) as login_shown,     
sum(login_submit) as login_submit,    
sum(login_success) as login_success,
sum(login_success1) as login_success1
from 
pp_oap_sing_karthikeyan_t.KA_login_8ball_summary
where 
is_ebaymp_y_n = 'N' and evnt_dt  = current_date - 2
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;


----------------------------------------------------------------------------------------------
----------Dolphin+Funnel+For+8ball+all+Countries-session[2]------------------------------------

insert into pp_oap_sing_shuqi_t.Dolphins_sessns_temp
SEL
sessn_id AS user_session_guid,
CASE WHEN context_id NOT LIKE 'NV%' THEN context_id END AS Context_id,
COALESCE(CAST(td_sysfnlib.NVP(payload,'experimentation_experience', '&', '=') AS VARCHAR(100)),'#') AS experimentation_experience,
evnt_dt

FROM 
pp_polestar_views.fact_ea_evnt
WHERE
1=1 
AND biz_evnt_key = 37 
AND component='unifiedloginnodeweb'
AND evnt_dt between current_date - 3 and current_date - 1
AND (experimentation_experience LIKE '%3958%')		------Dolphin US 
GROUP BY 1,2,3,4
;

INSERT INTO  pp_oap_sing_MANi_t.Dolphins_all_base1

SEL
evnt_key,
a.evnt_dt,
evnt_ts_epoch + evnt_ts_msecs AS evnt_ts,
geo_cntry,
cust_id,
----ID NVPS
visitor_id AS user_guid,
Sessn_id AS user_session_guid,
COALESCE(CAST(td_sysfnlib.NVP(payload,'decr_account_number', '&', '=') AS VARCHAR(100)),'#') AS decr_account_number,
COALESCE(CAST(td_sysfnlib.NVP(payload,'buyer_id', '&', '=') AS VARCHAR(20)),'#') AS buyer_id,
correlation_id as  cal_correlation_id,
COALESCE(CASE WHEN context_id NOT LIKE 'NV%' THEN context_id END,'#') AS context_id,
---Device NVP 
COALESCE(CAST(td_sysfnlib.NVP(payload,'client_os', '&', '=') AS VARCHAR(100)),'#') AS client_os,
COALESCE(CAST(td_sysfnlib.NVP(payload,'channel', '&', '=') AS VARCHAR(100)),'#') AS channel,
COALESCE(CAST(td_sysfnlib.NVP(payload,'is_cookied', '&', '=') AS VARCHAR(10)),'#') AS is_cookied,
--EXP NVP
COALESCE(CAST(td_sysfnlib.NVP(payload,'experimentation_experience', '&', '=') AS VARCHAR(100)),'#') AS experimentation_experience,
COALESCE(CAST(td_sysfnlib.NVP(payload,'experimentation_treatment', '&', '=') AS VARCHAR(100)),'#') AS experimentation_treatment,
-----backend NVP
COALESCE(CAST(td_sysfnlib.NVP(payload,'sub_state_name', '&', '=') AS VARCHAR(100)),'#') AS sub_state_name,
COALESCE(component,'#') AS component,
COALESCE(CAST(td_sysfnlib.NVP(payload,'traffic_source', '&', '=') AS VARCHAR(100)),'#') AS traffic_source,
----Stage NVPS
COALESCE(state_name,'#') AS state_name,
COALESCE(transition_name,'#') AS transition_name,
COALESCE(CAST(td_sysfnlib.NVP(payload,'ul_process_state', '&', '=') AS VARCHAR(100)),'#') AS ul_process_state,
COALESCE(CAST(td_sysfnlib.NVP(payload,'login_status', '&', '=') AS VARCHAR(256)),'#') AS login_status,
COALESCE(ext_error_code,'#') AS ext_error_code,
COALESCE(ext_error_desc,'#') AS ext_error_desc,
COALESCE(int_error_code,'#') AS int_error_code,
COALESCE(int_error_desc,'#') AS int_error_desc,
COALESCE(eligibility_reason,'#') AS eligibility_reason,
COALESCE(CAST(td_sysfnlib.NVP(payload,'source_decisioning', '&', '=') AS VARCHAR(100)),'#') AS source_decisioning,
COALESCE(CAST(td_sysfnlib.NVP(payload,'intent', '&', '=') AS VARCHAR(100)),'#') AS intent

FROM 
pp_polestar_views.fact_ea_evnt a
JOIN pp_oap_sing_mani_t.Dolphins_sessions b
ON a.Sessn_id= b.user_session_guid
AND a.evnt_dt = b.evnt_dt

WHERE
1=1 
AND biz_evnt_key = 37 
AND component='unifiedloginnodeweb'
AND a.evnt_dt between current_date - 3 and current_date - 1
;

INSERT INTO pp_oap_sing_shuqi_t.Me_Basic_funnel_dolphin1
SELECT
a.User_Session_guid,
a.evnt_dt,
COALESCE( MAX (CASE 
WHEN intent='enablePhonePassword' OR source_decisioning = 'ENABLE_PHONE_PASSWORD'  THEN 'MobileID_US'
WHEN intent='postLoginPhoneConfirmation' OR source_decisioning = 'POST_LOGIN_PHONE_CONFIRMATION' THEN 'Dolphin_US'
WHEN source_decisioning LIKE '%XO_Toast%' THEN 'XO_Toast_US'
end),
MAX(CASE WHEN b.experimentation_experience LIKE '%3957%' THEN 'MobileID_US'
     WHEN b.experimentation_experience LIKE '%3958%' THEN 'Dolphin_US'
     WHEN b.experimentation_experience LIKE '%3956%' THEN 'MobileID_US'
	WHEN b.experimentation_experience LIKE '%4049%' THEN 'XO_Toast_US'
     end))  Experiment, 
MAX(channel) AS channel,
--intertitial Clicks
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'prepare_interstitial' THEN 1 ELSE 0 end) AS Intertitial_Shown_Y_N,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_not_now' THEN 1 ELSE 0 end) AS Not_now_interstitial_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'process_next' THEN 1 ELSE 0 end) AS next_interstitial_click_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_change' THEN 1 ELSE 0 end) AS Change_interstitial_click_y_n,
--SMS 
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_add_sms_confirmation') THEN 1 ELSE 0 end ) AS Add_SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_sms_confirmation') THEN 1 ELSE 0 end ) AS SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND  transition_name IN ( 'click_not_now')THEN 1 ELSE 0 end ) AS SMS_Confrirm_not_now_Click_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_resend') THEN 1 ELSE 0 end) AS SMS_Confrim_resend_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_confirm') THEN 1 ELSE 0 end) AS SMS_Confrim_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'process_sms_confirm') AND int_error_code = '#' AND int_error_desc = '#' THEN 1 ELSE 0 end) AS SMS_Confirm_Success,
'#' as eligibility_reason

FROM 
 pp_oap_sing_MANi_t.Dolphins_all_base1 a
JOIN  pp_oap_sing_shuqi_t.Me_Session_base b
ON a.user_Session_guid = b.user_Session_guid
AND a.evnt_dt = b.evnt_dt 
WHERE a.evnt_dt between current_date - 3 and current_date - 1
and b.experimentation_experience LIKE ANY ('%3956%', '%3957%', '%3958%', '%4049%')
GROUP BY 1,2
;

INSERT INTO pp_oap_sing_shuqi_t.Me_Basic_funnel_dolphin1
SELECT
a.User_Session_guid,
a.evnt_dt,
'Dolphin_AU'  Experiment, 
MAX(channel) AS channel,
--intertitial Clicks
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'prepare_interstitial' THEN 1 ELSE 0 end) AS Intertitial_Shown_Y_N,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_not_now' THEN 1 ELSE 0 end) AS Not_now_interstitial_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'process_next' THEN 1 ELSE 0 end) AS next_interstitial_click_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_change' THEN 1 ELSE 0 end) AS Change_interstitial_click_y_n,
--SMS 
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_add_sms_confirmation') THEN 1 ELSE 0 end ) AS Add_SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_sms_confirmation') THEN 1 ELSE 0 end ) AS SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND  transition_name IN ( 'click_not_now')THEN 1 ELSE 0 end ) AS SMS_Confrirm_not_now_Click_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_resend') THEN 1 ELSE 0 end) AS SMS_Confrim_resend_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_confirm') THEN 1 ELSE 0 end) AS SMS_Confrim_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'process_sms_confirm') AND int_error_code = '#' AND int_error_desc = '#' THEN 1 ELSE 0 end) AS SMS_Confirm_Success,
'#' as eligibility_reason

FROM 
 pp_oap_sing_MANi_t.Dolphins_all_base1 a
JOIN  pp_oap_sing_shuqi_t.Me_Session_base b
ON a.user_Session_guid = b.user_Session_guid
AND a.evnt_dt = b.evnt_dt 
WHERE a.evnt_dt between current_date - 3 and current_date - 1
and b.experimentation_experience LIKE  ('%4588%')
GROUP BY 1,2
;


INSERT INTO pp_oap_sing_shuqi_t.Me_Basic_funnel_dolphin1
SELECT
a.User_Session_guid,
a.evnt_dt,
'Dolphin_APAC'  Experiment, 
MAX(channel) AS channel,
--intertitial Clicks
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'prepare_interstitial' THEN 1 ELSE 0 end) AS Intertitial_Shown_Y_N,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_not_now' THEN 1 ELSE 0 end) AS Not_now_interstitial_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'process_next' THEN 1 ELSE 0 end) AS next_interstitial_click_y_n,
MAX ( CASE WHEN state_name = 'begin_interstitial' AND transition_name = 'click_change' THEN 1 ELSE 0 end) AS Change_interstitial_click_y_n,
--SMS 
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_add_sms_confirmation') THEN 1 ELSE 0 end ) AS Add_SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN ('prepare_sms_confirmation') THEN 1 ELSE 0 end ) AS SmS_Confirm_page_shown_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND  transition_name IN ( 'click_not_now')THEN 1 ELSE 0 end ) AS SMS_Confrirm_not_now_Click_y_n,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_resend') THEN 1 ELSE 0 end) AS SMS_Confrim_resend_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'click_confirm') THEN 1 ELSE 0 end) AS SMS_Confrim_Click,
MAX ( CASE WHEN state_name = 'begin_sms_confirm' AND transition_name IN( 'process_sms_confirm') AND int_error_code = '#' AND int_error_desc = '#' THEN 1 ELSE 0 end) AS SMS_Confirm_Success,
'#' as eligibility_reason

FROM 
 pp_oap_sing_MANi_t.Dolphins_all_base1 a
JOIN  pp_oap_sing_shuqi_t.Me_Session_base b
ON a.user_Session_guid = b.user_Session_guid
AND a.evnt_dt = b.evnt_dt 
WHERE a.evnt_dt between current_date - 3 and current_date - 1
and b.experimentation_experience LIKE  ('%4688%')
GROUP BY 1,2
;


INSERT INTO pp_oap_sing_mani_t.Mobile_Dolphin_funnel_agg1
SEL 
evnt_dt,
experiment, 
channel,
COALESCE (elgblty_rsn, '#') eligibility_reason,
COUNT ( DISTINCT User_session_guid)Starts,
SUM(Intertitial_Shown_Y_N)       Interstitial_shown
,SUM(CASE WHEN next_interstitial_click_y_n = 0 THEN  Not_now_interstitial_y_n ELSE 0 END)Interstitial_Not_Now_Click
,SUM( CASE WHEN next_interstitial_click_y_n = 1 OR Add_SmS_Confirm_page_shown_y_n = 1 OR SmS_Confirm_page_shown_y_n = 1 THEN 1 ELSE 0 END )Next_interstitial_click
,SUM(Change_interstitial_click_y_n) interstitial_change_click
,SUM(CASE WHEN Add_SmS_Confirm_page_shown_y_n = 1 OR SmS_Confirm_page_shown_y_n = 1 THEN 1 ELSE 0 END )SMS_confirmation_page_seen
,SUM(CASE WHEN SMS_Confrim_Click = 1 THEN 0 ELSE SMS_Confrirm_not_now_Click_y_n END )SMS_Not_now_click
,SUM(SMS_Confrim_resend_Click)SMS_resend_Click
,SUM(SMS_Confrim_Click) SMS_Confrim_Click
,SUM(SMS_Confirm_Success)SMS_Confirm_Success

FROM
pp_oap_sing_shuqi_t.Me_Basic_funnel_dolphin1
WHERE evnt_dt BETWEEN current_date - 3 AND current_date - 1
GROUP BY 1,2,3,4
 -- ) WITH DATA
 -- PRIMARY INDEX ( evnt_dt , experiment, channel);
;


------------DolphinMobileIDConfirmedPhones-latest-context--------------------------------
-----------------------------------------------------------------------------------------


INSERT INTO pp_Oap_sing_anish_t.tbl_dolphinmobidevents 
select 
evnt_key, evnt_dt,
cust_id AS buyer_id,
sessn_id AS user_session_guid,
context_id AS context_id,
component AS component,
int_error_code AS int_error_code,
eligibility_reason AS eligibility_reason,
state_name AS state_name,
transition_name AS transition_name,
CAST(td_sysfnlib.NVP(payload,'intent') AS VARCHAR(200)) AS intent,
CAST(td_sysfnlib.NVP(payload,'experimentation_experience') AS VARCHAR(200)) AS experimentation_experience,
CAST(td_sysfnlib.NVP(payload,'experimentation_treatment') AS VARCHAR(200)) AS experimentation_treatment,
CAST(td_sysfnlib.NVP(payload,'source_decisioning') AS VARCHAR(200)) AS source_decisioning,
'#' as decr_cust_id,
COALESCE (CAST(td_sysfnlib.NVP(payload,'banner_type') AS VARCHAR(100) ) , '#') AS banner_type
FROM pp_polestar_views.fact_ea_evnt t1
where biz_evnt_key = 37 and evnt_dt >= (select max_date from pp_oap_sing_anish_t.tbl_dolphconfirm_date)
AND  component='unifiedloginnodeweb' 
and transition_name IN ('prepare_interstitial','process_next','prepare_add_sms_confirmation','prepare_sms_confirmation','click_change','click_not_now','process_sms_confirm')
and buyer_id not like 'EAP%';


INSERT INTO pp_oap_sing_anish_t.tbl_dolphinagg
SELECT t1.evnt_dt,
project,
phone_type,
profile_check, 
Count(DISTINCT buyer_id) AS customer_cnt, exp_country 
FROM pp_oap_sing_anish_t.tbl_dolmobid t1  
LEFT JOIN  ( SEL evnt_dt , cust_id , Max(confirm_success) confirm_success FROM  pp_oap_sing_mani_t.me_autotrigg_tokens GROUP BY 1,2)  b
ON t1.evnt_dt = b.evnt_dt 
AND t1.buyer_id = b.cust_id 
AND project = 'XO_Toast'
WHERE 
		(CASE WHEN b.cust_id IS NOT NULL	AND b.confirm_success = 'Y' THEN 1 
		END = 1  OR 	phone_confirmed =1)  
--AND evnt_dt BETWEEN 1190601 AND 1190613
GROUP BY 1,2,3,4,6
--) with data primary index(evnt_dt,project,phone_type,profile_check)
;



insert into pp_oap_Sing_shuqi_t.XO_toast_funnel_conversion
SELECT 
t1.evnt_dt,
 Expr ,
t1.WPS_expr,
exp_country,
CASE WHEN mobile_platform IN('Android') THEN mobile_platform
     WHEN mobile_platform LIKE 'iOS%' THEN 'iOS'
     WHEN mobile_platform = '#' THEN 'WEB'
	 WHEN mobile_platform IS NULL THEN '#'
     ELSE 'Other - platform' end AS mobile_platform,
CASE WHEN 
t2.xo_product = 'EC'
AND t2.pp_test_merch_y_n = 'N' 
AND t2.intrnl_traffic_y_n = 'N'
AND t2.rt_rp_txn_y_n = 'N'
AND t2.redirect_blacklist_y_n = 'N'
AND t2.multi_slr_unilateral_y_n = 'N'
AND t2.ec_token_deduped_y_n = 'N'
AND t2.rcvr_id <> '1219054521827007990'
AND t2.cntxt_type NOT IN ('flowlogging','WPS-Token','Cart-ID') THEN 'Y' 
WHEN t2.xo_product = 'WPS'
AND t2.wps_official_conversion_y_n = 'Y' 
AND t2.wps_bot_y_n = 'N' THEN 'Y'
ELSE 'N' end AS official_conversion_y_n,
Count( DISTINCT t1.cust_id) AS Cust_cnt, 
Count(*) AS expr_total,
Sum(t1.xo_toast_dcsn)xo_toast_dcsn,
Sum(t1.banner_shown) banner_shown,
Sum(t1.banner_clicked) banner_clicked,
Sum(t1.click_loaded_overlay) click_loaded_overlay,
Sum(t1.interstital_shown) interstital_shown,
Sum(t1.next_click_on_interstitial) next_click_on_interstitial,
Sum(t1.submit_sms_confirm) submit_sms_confirm,
Sum (CASE WHEN  b.context_id IS NOT NULL AND b.confirm_success = 'Y' THEN 1 ELSE t1.sms_confirm_success END  ) sms_confirm_success,
Sum(Coalesce (starts,0) )starts,
Sum(Coalesce (dones,0) )dones

FROM  pp_oap_sing_mani_t.me_xotoastevents t1

LEFT JOIN  pp_product_views.fact_fpti_ms_ec_rpt  t2   
ON t1.evnt_Dt=t2.ec_cre_dt AND t1.context_id=t2.ec_token_id
AND  t2.ec_cre_dt BETWEEN current_date - 5 and current_date

LEFT JOIN pp_oap_sing_mani_t.me_autotrigg_tokens b
ON t1.context_id = b.context_id
AND t1.evnt_dt = b.evnt_dt 

WHERE t1.expr IS NOT NULL
and  t1.evnt_Dt between current_date - 5 and current_date
GROUP BY 1,2,3,4, 5,6;


--------------------------------Authflow[1]---------------------------------------------
-----------------------------------------------------------------------------------------

insert into pp_oap_sing_zhuali_t.AF_funnel_gsid
select 
a.evnt_dt,
a.gsid,
authchpt,
authflowcxt,

--cut
max(channel) as channel,
max(case when context_id <> '#' then 1 else 0 end) as XO,
max(geo_cntry) as geo_cntry,
max(b.FPR_REV_LVL_1) as Region,  
max(a.client_os) as client_os,
max(a.browser_type) as browser_type,
max(case when c.gsid is not null then 1 else 0 end) as peeked,


max(case when authchlng is not null then 1 else 0 end) as AF_shown,
max(case when authchlng is not null and authchsn is not null then 1 else 0 end) as AF_chosen,
max(case when authchlng is not null and authchsn is not null and authflow = 'Passed' then 1 else 0 end) as AF_pass,

-- shown
max(case when authchlng like '%sms|%' or authchlng like '%sms'  then 1 else 0 end) as sms_shown,
max(case when authchlng like '%ivr%' then 1 else 0 end) as ivr_shown,
max(case when authchlng like '%email%' then 1 else 0 end) as email_shown,
max(case when authchlng like '%card%' then 1 else 0 end) as card_shown,
max(case when authchlng like '%securityQuestions%' then 1 else 0 end) as sq_shown,
max(case when authchlng like '%identityDocument%' then 1 else 0 end) as identityDocument_shown,
max(case when authchlng like '%sms2%' then 1 else 0 end) as sms2_shown,
max(case when authchlng like '%pn%' then 1 else 0 end) as pn_shown,
max(case when authchlng like '%kba%' then 1 else 0 end) as kba_shown,
max(case when authchlng like '%pinLessIvr%' then 1 else 0 end) as pinLessIvr_shown,
max(case when authchlng like '%facebook%' then 1 else 0 end) as facebook_shown,
max(case when authchlng like '%ssn%' then 1 else 0 end) as ssn_shown,

-- chosen
max(case when  (authchlng like '%sms|%' or authchlng like '%sms') and authchsn = 'sms' then 1 else 0 end) as sms_chosen,
max(case when authchlng like '%ivr%' and authchsn = 'ivr' then 1 else 0 end) as ivr_chosen,
max(case when authchlng like '%email%' and authchsn = 'email' then 1 else 0 end) as email_chosen,
max(case when authchlng like '%card%' and authchsn = 'card' then 1 else 0 end) as card_chosen,
max(case when authchlng like '%securityQuestions%' and authchsn = 'securityQuestions' then 1 else 0 end) as sq_chosen,
max(case when authchlng like '%identityDocument%' and authchsn = 'identityDocument' then 1 else 0 end) as identityDocument_chosen,
max(case when authchlng like '%sms2%' and authchsn = 'sms2' then 1 else 0 end) as sms2_chosen,
max(case when authchlng like '%pn%' and authchsn = 'pn' then 1 else 0 end) as pn_chosen,
max(case when authchlng like '%kba%' and authchsn = 'kba' then 1 else 0 end) as kba_chosen,
max(case when authchlng like '%pinLessIvr%' and authchsn = 'pinLessIvr' then 1 else 0 end) as pinLessIvr_chosen,
max(case when authchlng like '%facebook%' and authchsn = 'facebook' then 1 else 0 end) as facebook_chosen,
max(case when authchlng like '%ssn%' and authchsn = 'ssn' then 1 else 0 end) as ssn_chosen,

-- pass
max(case when (authchlng like '%sms|%' or authchlng like '%sms') and authchsn = 'sms' and authflow = 'passed' then 1 else 0 end) as sms_pass,
max(case when authchlng like '%ivr%' and authchsn = 'ivr' and authflow = 'passed' then 1 else 0 end) as ivr_pass,
max(case when authchlng like '%email%' and authchsn = 'email' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as email_pass,
max(case when authchlng like '%card%' and authchsn = 'card' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as card_pass,
max(case when authchlng like '%securityQuestions%' and authchsn = 'securityQuestions' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as sq_pass,
max(case when authchlng like '%identityDocument%' and authchsn = 'identityDocument' and authflow = 'passed' then 1 else 0 end) as identityDocument_pass,
max(case when authchlng like '%sms2%' and authchsn = 'sms2' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as sms2_pass,
max(case when authchlng like '%pn%' and authchsn = 'pn' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as pn_pass,
max(case when authchlng like '%kba%' and authchsn = 'kba' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as kba_pass,
max(case when authchlng like '%pinLessIvr%' and authchsn = 'pinLessIvr' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as pinLessIvr_pass,
max(case when authchlng like '%facebook%' and authchsn = 'facebook' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as facebook_pass,
max(case when authchlng like '%ssn%' and authchsn = 'ssn' and (authflow = 'passed' or active_challenge_status = 'passed') then 1 else 0 end) as ssn_pass,

max(cust_id) as cust_id
from pp_oap_sing_zhuali_t.AF_evnt_day a 
left join pp_access_views.dim_cntry as b on a.geo_cntry=b.CNTRY_CODE
left join pp_oap_sing_zhuali_t.peeking_global_sessn_day  as c on a.gsid = c.gsid
group by 1,2,3,4
--)with data primary index(gsid)
;


--------------------------------Hybrid---------------------------------------------
-----------------------------------------------------------------------------------------

INSERT INTO pp_oap_sing_mani_t.me_sts_last_evnt_login

SELECT
* 
FROM 
pp_scratch.me_sts_login_status_value

WHERE  context_id <> '#'
AND  evnt_dt = (select rpt_dt - 1 from rpt_dt)
AND context_id NOT IN ( SEL Context_id FROM pp_oap_sing_mani_t.me_sts_last_evnt_login GROUP BY 1)
QUALIFY Row_Number() Over( PARTITION BY Context_id ORDER BY evnt_ts DESC) = 1 ; 


INSERT INTO pp_oap_Sing_shuqi_t.me_hybrid_pxp_login_tokens

SELECT
a.evnt_dt,
a.context_id,
a.user_session_guid,

-- seg
a.pxp_test_group,
a.is_cookied,
e.buyer_ip_country,
e.browser_type,
e.device_type,

--rpt
official_conversion_y_n,
starts,
prepare_rvw_y_n,
dones,

-- login
e.login_status,

-- Regular
CASE WHEN email_page_rendered_y_n = 1  THEN 1 ELSE 0 end AS email_shown,
CASE WHEN email_page_submit_y_n = 1 THEN 1 ELSE 0 end AS email_submit,
CASE WHEN email_page_success_y_n = 1 THEN 1 ELSE 0 end AS email_success,
CASE WHEN pwd_page_rendered_y_n = 1  THEN 1 ELSE 0 end AS pwd_shown,
CASE WHEN pwd_page_submit_y_n = 1 THEN 1 ELSE 0 end AS pwd_submit,
CASE WHEN pwd_page_success_y_n = 1 THEN 1 ELSE 0 end AS pwd_success,

-- hybrid
CASE WHEN hybrid_page_rendered_y_n = 1 THEN 1 ELSE 0 end AS hybrid_shown,
CASE WHEN hybrid_page_submit_y_n = 1 THEN 1 ELSE 0 end AS hybrid_submit,
CASE WHEN hybrid_page_success_y_n = 1 THEN 1 ELSE 0 end AS hybrid_success,
CASE WHEN hybrid_pwd_page_rendered_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_shown,
CASE WHEN hybrid_pwd_page_submit_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_submit,
CASE WHEN hybrid_pwd_page_success_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_success,
CASE WHEN hybrid_pwd_page_change_y_n = 1 THEN 1 ELSE 0 end AS hybrid_pwd_change,

--- Signup and STS success
CASE WHEN b.auth_req_status = '0'  THEN 1  ELSE 0  END AS STS_success, 
CASE WHEN b.bizeventname = 'LOGIN_ENDPOINT_PASSWORD_Phone' THEN 1 ELSE 0 END AS phone_login,
CASE WHEN c.feed_name = 'xoonboardingnodeweb' THEN 1 ELSE 0 end AS Signup,
CASE WHEN d.cust_id IS NOT NULL THEN 'Loginable' ELSE 'Non-Loginable' end  AS Login_flag,

---hybrid pubcred
hybrid_email_submit_pub_cred,
hybrid_pwd_render_pub_cred,
hybrid_pwd_submit_pub_cred,

---regular pubcred
regular_email_submit_pub_cred,
regular_pwd_render_pub_cred,
regular_pwd_submit_pub_cred,

--last pubcred
last__pub_cred,
login_error,
EXPER_NAME,
CASE WHEN EXPER_NAME = 'Hybrid-Uncookied'  THEN 'N'  ELSE subsequent_y_n END AS subsequent_y_n,
Country
  
FROM pp_oap_sing_agnal_k_t.dl_ul_hybrid_pxp_start_rpt a 

LEFT JOIN pp_oap_sing_agnal_k_t.dl_ul_hybrid_tokens e 
ON a.evnt_dt = e.evnt_dt AND a.context_id = e.context_id

LEFT JOIN pp_oap_sing_mani_t.me_sts_last_evnt_login b
ON a.context_id = b.context_id 
AND a.evnt_dt = b.evnt_dt

LEFT JOIN pp_oap_sing_mani_t.me_evnt_login_last_feed_name c
ON a.context_id = c.context_id 

LEFT JOIN pp_oap_sing_mani_t.me_cust_id_token d
ON a.context_id = d.context_id

WHERE 1=1
AND a.evnt_dt = (select rpt_dt - 1 from rpt_dt)




--------------------------------XO_login_Rcvr -------------------------------------------
-----------------------------------------------------------------------------------------

insert into pp_oap_sing_saran_t.rcvr_id_agg_dtl
select 
a.ec_cre_dt,
a.rcvr_id,
b.busn_name,
case
when a.mobile_platform like 'iOS%' then 'iOS            '
when a.mobile_platform like 'Android%' then 'Android'
when a.mobile_platform like 'Windows%' then 'Windows'
when c.mobile_platform ='#' and c.platform='WEB' then 'WEB'
when c.mobile_platform ='#' and c.platform='#' then 'Unknown'
else 'OtherMobile' end as web_mob,
case when a.billing_type_code = 0 then 0 else 1 end as billing,
case when a.last_feed_name_evnt= 'xoonboardingnodeweb' then 1 else 0 end as signup,
sum(case when a.billing_type_code = 0 then a.dones when a.billing_type_code <> 0 and c.billing_agmt_id like 'B-%' then 1 else 0 end) as dones_creations,
sum(case when a.login_exprnce = 'Split Screen' then manual_login_shown else 0 end) as split_login_shown,
sum(case when a.login_exprnce = 'Split Screen' then manual_login_submit else 0 end) as split_login_submit,
sum(case when a.login_exprnce = 'Split Screen' then manual_login_success else 0 end) as split_login_success,
sum(case when a.login_exprnce = 'UL as Landing' then manual_login_shown else 0 end) as ul_login_shown,
sum(case when a.login_exprnce = 'UL as Landing' then manual_login_submit else 0 end) as ul_login_submit,
sum(case when a.login_exprnce = 'UL as Landing' then manual_login_success else 0 end) as ul_login_success,
sum(case when a.login_exprnce = 'UL iFrame' then manual_login_shown else 0 end) as iframe_login_shown,
sum(case when a.login_exprnce = 'UL iFrame' then manual_login_submit else 0 end) as iframe_login_submit,
sum(case when a.login_exprnce = 'UL iFrame' then manual_login_success else 0 end) as iframe_login_success,

sum(one_touch_starts) as one_touch,
sum(case when one_touch_starts = 1 and one_touch_success = 1 then 1 else 0 end) as one_touch_pure_success,
sum(case when one_touch_starts = 1 and one_touch_success = 0 and manual_login_success = 1 then 1 else 0 end) as one_touch_manual_success,
sum(case when one_touch_starts = 1 and one_touch_success = 0 and one_touch_login_contingency = 1 then 1 else 0 end) as one_touch_login_contingency,
sum(case when one_touch_starts = 1 and one_touch_success = 0 and one_touch_planning_contingency = 1 then 1 else 0 end) as one_touch_planning_contingency
from pp_oap_sing_zhuali_t.token_login a left join pp_oap_sing_zhuali_t.busn_name b on a.rcvr_id = b.rcvr_id left join pp_product_views.fact_fpti_ms_ec_rpt c on a.ec_token_id = c.ec_token_id and  c.ec_cre_dt = (select max_dt_rpt from max_dt_rpt)
 where a.ec_cre_dt = (select max_dt_rpt from max_dt_rpt) and c.ec_cre_dt = (select max_dt_rpt from max_dt_rpt)
 and venice_login_starts = 0
 and a.login_exprnce in ('One Touch','Split Screen', 'UL as Landing', 'UL iFrame')
and official_conversion_y_n = 'Y'
and a.rcvr_id in ( 
 select 
rcvr_id
from pp_oap_sing_zhuali_t.token_login
where ec_cre_dt = (select max_dt_rpt from max_dt_rpt)
and official_conversion_y_n = 'Y'
group by 1 having COUNT(*) >10
)
group by 1,2,3,4,5,6;


------------------------VENICELOGIN-----------------------------------------------

INSERT INTO PP_OAP_SING_MIT_T.Venice_evnt_level_polestar 
Select 
sessn_start_dt as event_date, 
sessn_id, 
evnt_key, 
page_name,
 evnt_ts_epoch +evnt_ts_msecs,
CASE WHEN client_os like '%ios%' then 'iOS' when client_os like '%android%' then 'Android' end as Os, 
cast(td_sysfnlib.nvp(payload,'goal', '&', '=') as varchar(100)) as goal,
CASE WHEN  
page_group in ('mobile:consapp:relogin:system:fingerprintlogin', 'mobile:consapp:relogin:system:nativefingerprintlogin', 'mobile:consapp:relogin:system:touchIDlogin') then 'fingerprint' 
WHEN page_group in ('mobile:consapp:relogin::pin') then 'pin' 
WHEN  page_group in ('mobile:consapp:relogin::pwd', 'mobile:consapp:login::fulllogin')  then 'pwd'
WHEN page_group in ('mobile:consapp:relogin:system:faceIDlogin')  then 'Face_id' 
WHEN page_group in ( 'mobile:consapp:relogin:userpreviewlogin:success','mobile:consapp:relogin:userpreviewlogin:success::', 'mobile:consapp:relogin:userpreviewlogin:failure::','mobile:consapp:relogin:userpreviewlogin:failure') then 'LLS' 
WHEN page_group in ( 'mobile:consapp:signup:createaccount:success', 'mobile:consapp:signup::mobilefirst:signupform:createaccount:success') then 'onboarding_autologin'
end as login_method,
case when goal in ('login_face', 'login_biometric_face') then 'face_id'
            when goal in ('login_native_fp','login_fp', 'login_biometric_fp') then 'fingerprint'
            when goal = 'login_pin' then 'pin'
            when goal in ('login_pwd','login_phone_pwd') then 'pwd' 
			when goal in ('createaccount_success', 'success') then 'onboarding_autologin'
			else '#' end as login_method_2,
			page_group,
			geo_cntry,
			cust_id,
			'#' as region,
			cast(td_sysfnlib.nvp(payload,'mapv', '&', '=') as varchar(100)) as app_version
--cast(NEW JSON(x_paypal_fpti_hdr).JSONExtractValue('$..user_session_guid') as varchar(200)) as user_session_guid_sts,
			FROM  PP_POLESTAR_VIEWS.WEB_EVNT
WHERE sessn_start_dt > (select max_date FROM PP_OAP_SING_MIT_T.Venice_max_dt)
and page_group in ('mobile:consapp:relogin:userpreviewlogin:success','mobile:consapp:relogin:userpreviewlogin:success::', 'mobile:consapp:login:success:', 'mobile:consapp:signup:createaccount:success', 'mobile:consapp:signup::mobilefirst:signupform:createaccount:success', 
'mobile:consapp:relogin:system:fingerprintlogin', 'mobile:consapp:relogin::pin', 'mobile:consapp:relogin::pwd', 'mobile:consapp:relogin:system:nativefingerprintlogin', 'mobile:consapp:relogin:system:touchIDlogin', 'mobile:consapp:login::fulllogin', 'mobile:consapp:relogin:system:faceIDlogin')
--) with data primary index  (evnt_key);
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;


insert into PP_OAP_SING_MIT_T.Venice_aggregate
select event_date, 
first_login_method, 
last_login_method, 
login_success,
Os, 
country,
region,
xO_session,
app_version,
COUNT(distinct sessn_id) as distinct_starts,
count(sessn_id) as starts,
COUNT(distinct CASE WHEN login_success = 'y' then sessn_id else null end) as distinct_dones,
COUNT( CASE WHEN login_success = 'y' then sessn_id else null end) as dones,
count(Distinct cust_id) as cust_count
FROM PP_OAP_SING_MIT_T.Veince_sessn_level
where event_date > (select max_date FROM PP_OAP_SING_MIT_T.Venice_max_dt)
group by 1,2,3,4,5,6,7,8,9;

--------------------------SSPWR--------------------------------------------------------------

INSERT INTO ${adsWrkDB}.SS_PWR
(
	evnt_key,
	evnt_dt,
	evnt_ts,
	sessn_id,
	component,
	sub_comp,
	geo_cntry,
	ip_address,
	channel,
	device_type,
	client_os,
	device_name,
	browser_type,
	browser_version,
	cust_id,
	user_guid,
	user_session_guid,
	correlation_id,
	context_id,
	api_name,
	biz_api_name,
	biz_api_operation,
	int_error_code,
	int_error_desc,
	api_operation,
	api_response_code,
	auth_state,
	authflowcxt,
	authchpt,
	authchlng,
	authchsn,
	authflow
)
select  
	evnt_key,
	evnt_dt,
	evnt_ts_epoch+evnt_ts_msecs as evnt_ts,
	sessn_id,
	component,
	sub_comp,
	geo_cntry,
	ip_addr  as ip_address,
	coalesce(cast(td_sysfnlib.nvp(payload,'channel', '&', '=') as varchar(100)),'#') as channel,
	coalesce(cast(td_sysfnlib.nvp(payload,'device_type', '&', '=') as varchar(100)),'#') as device_type,
	coalesce(cast(td_sysfnlib.nvp(payload,'client_os', '&', '=') as varchar(100)),'#') as client_os,
	coalesce(cast(td_sysfnlib.nvp(payload,'device_name', '&', '=') as varchar(100)),'#') as device_name,
	coalesce(cast(td_sysfnlib.nvp(payload,'browser_type', '&', '=') as varchar(100)),'#') as browser_type,
	coalesce(cast(td_sysfnlib.nvp(payload,'browser_version', '&', '=') as varchar(100)),'#') as browser_version,
	cust_id,
	visitor_id as user_guid,
	sessn_id as user_session_guid,
	correlation_id,
	context_id, 
	api_name,
	biz_api_name,
	coalesce(cast(td_sysfnlib.nvp(payload,'biz_api_operation', '&', '=') as varchar(100)),'#') as biz_api_operation,
	int_error_code,
	int_error_desc,
	coalesce(cast(td_sysfnlib.nvp(payload,'api_operation', '&', '=') as varchar(100)),'#') as api_operation,
	api_response_code,
	coalesce(cast(td_sysfnlib.nvp(payload,'auth_state', '&', '=') as varchar(100)),'#') as auth_state,
	coalesce(cast(td_sysfnlib.nvp(payload,'authflowcxt', '&', '=') as varchar(100)),'#') as authflowcxt,
	coalesce(cast(td_sysfnlib.nvp(payload,'authchpt', '&', '=') as varchar(100)),'#') as authchpt,
	coalesce(cast(td_sysfnlib.nvp(payload,'authchlng', '&', '=') as varchar(100)),'#') as authchlng,
	coalesce(cast(td_sysfnlib.nvp(payload,'authchsn', '&', '=') as varchar(100)),'#') as authchsn,
	coalesce(cast(td_sysfnlib.nvp(payload,'authflow', '&', '=') as varchar(100)),'#') as authflow
 from ${polereadDB}.fact_ea_evnt
where biz_evnt_key = 55
  and evnt_dt >= cast('$MAXDT' as date format 'yyyy-mm-dd')
  and sessn_id <> '#' ;


  INSERT INTO ${adsWrkDB}.SS_PWR_sessns
(
	evnt_dt,
	user_session_guid,
	cust_id,
	pwr_start,
	Email_Valid,
	Email_Invalid,
	Email_Locked,
	AF_Start,
	AF_Shown,
	AF_Chosen,
	AF_Pass,
	Reset_PWD,
	safe_start
)
select
	evnt_dt,
	user_session_guid,
	max(case when cust_id not like 'EAP%' 
	         then cust_id 
	         else '#' 
	     end) as cust_id,
	max(case when biz_api_name = '/password-recovery/' 
	         then 1 
	         else 0 
	     end )as pwr_start,
	max(case when sub_comp = 'password_recovery' and  api_name='/v1/oauth2/token/security-context' and auth_state='PARTIALLY_AUTHENTICATED'  
	         then 1 
	         else 0
	     end )as Email_Valid,
	max(case when sub_comp = 'password_recovery' and  api_name like'/v1/oauth2/login%' and int_error_code = 'invalid_user'  and int_error_desc = 'Invalid user credentials' 
	         then 1 
	         else 0 
	     end ) as Email_Invalid,
	max(case when sub_comp = 'password_recovery' and  api_name like'/v1/oauth2/login%' and int_error_code = 'locked_user'  
	         then 1 
	         else 0 
	     end )  as Email_Locked,
	max(case when sub_comp='auth_challenges' and authchpt='pwr'
	         then 1
	         else 0
	     end) as AF_Start,
	max(case when sub_comp='auth_challenges' and authchpt='pwr' and authchlng <>'#' and authchlng <> 'noChallenges' 
	         then 1
	         else 0
	     end) as AF_Shown,
	max(case when sub_comp='auth_challenges' and authchpt='pwr' and authchsn <> '#' 
	         then 1
	         else 0 
	     end) as AF_Chosen,
	max(case when sub_comp='auth_challenges' and authchpt='pwr' and authflow='PASSED'
	         then 1 
	         else 0
	     end) as AF_Pass,
	max(case when sub_comp='password_recovery' and api_response_code like '2%' and api_name='/v1/identity/credentials-reset' and api_operation='POST'
	         then 1
	         else 0 
	     end )as Reset_PWD,
	max(case when sub_comp='auth_challenges' and biz_api_name='/safe/' and api_name='safeController.renderSafePage'
	         then 1
	         else 0 
	     end) as safe_start
  from ${adsWrkreadDB}.SS_PWR
 group by 1,2  
;


------------------------------ONETOUCHOPTION BASE-------------------------------------

INSERT INTO pp_oap_sing_brandedxo_t.gm_optin_source_00
SELECT 
evnt_key,
evnt_dt, 
evnt_ts_epoch,
cust_id,
geo_cntry,
encr_cust_id AS buyer_id,
site_chnl AS channel,
component,
COALESCE(CAST(td_sysfnlib.NVP(payload,'device_name', '&', '=') AS VARCHAR(100)),'#') AS device_type,
int_error_code AS failure_msg,
context_id,
correlation_id AS cal_correlation_id,
visitor_id AS user_guid,
COALESCE(CAST(td_sysfnlib.NVP(payload,'kmli_optin', '&', '=') AS VARCHAR(100)),'#') AS kmli_optin,
COALESCE(CAST(td_sysfnlib.NVP(payload,'one_touch_interstitial_optin', '&','=') AS VARCHAR(100)),'#') AS one_touch_interstitial_optin,
COALESCE(CAST(td_sysfnlib.NVP(payload,'optin_source', '&', '=') AS VARCHAR(100)),'#') AS optin_source,
COALESCE(CAST(td_sysfnlib.NVP(payload,'onboarding_experience', '&', '=') AS VARCHAR(100)),'#') AS onboarding_experience,
COALESCE(CAST(td_sysfnlib.NVP(payload,'environment', '&', '=') AS VARCHAR(100)),'#') AS environment

FROM pp_polestar_views.fact_ea_evnt a

WHERE 1=1
AND biz_evnt_key = 37
AND evnt_dt between current_date - 4 and current_date - 2
--and payload like any ('%checkout_login%','%checkout_interstitial%','%marketing%','%8ball%','%merchantProfile%','%consumerOnBoarding%','%signUpVariantOne%','%signUpVariantTwo%','%checkoutOnlyMember%')
AND payload LIKE '%optin_source%' 
and component = 'unifiedloginnodeweb';

COLLECT STATS ON pp_oap_sing_brandedxo_t.gm_optin_source_00 COLUMN context_id;
COLLECT STATS ON pp_oap_sing_brandedxo_t.gm_optin_source_00 COLUMN cal_correlation_id;
COLLECT STATS ON pp_oap_sing_brandedxo_t.gm_optin_source_00 COLUMN component;
COLLECT STATS ON pp_oap_sing_brandedxo_t.gm_optin_source_00 COLUMN optin_source;
COLLECT STATS ON pp_oap_sing_brandedxo_t.gm_optin_source_00 COLUMN evnt_ts_epoch;

collect stats on pp_oap_sing_brandedxo_t.gm_optin_source_final_01 column evnt_dt;

------------------------AUTH1-----------------------------------------

Insert into ${adsWrkDB}.AF_evnt_day 
(
      evnt_dt 
      ,evnt_key 
      ,evnt_ts_epoch 
      ,cust_id 
      ,geo_cntry 
      ,user_guid 
      ,user_session_guid 
      ,cal_correlation_id 
      ,gsid 
      ,context_id 
      ,component 
      ,sub_comp 
      ,traffic_source 
      ,channel 
      ,client_os 
      ,device_type 
      ,device_name 
      ,browser_type 
      ,browser_version 
      ,authchpt 
      ,authflowcxt 
      ,authchlng 
      ,authchsn 
      ,authflow 
      ,active_challenge_status 
      ,ivrcode 
      ,smscode 
      ,twowaysmscode 
      ,addphn 
      ,pushNotificationDisabled 
)
select
evnt_dt
,evnt_key
,evnt_ts_epoch
,cust_id
,geo_cntry
,visitor_id as user_guid
,sessn_id as user_session_guid
,correlation_id as cal_correlation_id
,cast(td_sysfnlib.nvp(payload,'global_session_id', '&', '=') as varchar(100)) as gsid
,context_id
,component
,sub_comp
,cast(td_sysfnlib.nvp(payload,'traffic_source', '&', '=') as varchar(100)) as traffic_source
,cast(td_sysfnlib.nvp(payload,'channel', '&', '=') as varchar(100)) as channel
,cast(td_sysfnlib.nvp(payload,'client_os', '&', '=') as varchar(100)) as client_os
,cast(td_sysfnlib.nvp(payload,'device_type', '&', '=') as varchar(100)) as device_type
,cast(td_sysfnlib.nvp(payload,'device_name', '&', '=') as varchar(100)) as device_name
,cast(td_sysfnlib.nvp(payload,'browser_type', '&', '=') as varchar(100)) as browser_type
,cast(td_sysfnlib.nvp(payload,'browser_version', '&', '=') as varchar(100)) as browser_version
,cast(td_sysfnlib.nvp(payload,'authchpt', '&', '=') as varchar(100)) as authchpt
,cast(td_sysfnlib.nvp(payload,'authflowcxt', '&', '=') as varchar(100)) as authflowcxt
,cast(td_sysfnlib.nvp(payload,'authchlng', '&', '=') as varchar(100)) as authchlng
,cast(td_sysfnlib.nvp(payload,'authchsn', '&', '=') as varchar(100)) as authchsn
,cast(td_sysfnlib.nvp(payload,'authflow', '&', '=') as varchar(100)) as authflow
,cast(td_sysfnlib.nvp(payload,'active_challenge_status', '&', '=') as varchar(100)) as active_challenge_status
,cast(td_sysfnlib.nvp(payload,'ivrcode', '&', '=') as varchar(100)) as ivrcode
,cast(td_sysfnlib.nvp(payload,'smscode', '&', '=') as varchar(100)) as smscode
,cast(td_sysfnlib.nvp(payload,'2waysmscode', '&', '=') as varchar(100)) as twowaysmscode
,cast(td_sysfnlib.nvp(payload,'addphn', '&', '=') as varchar(100)) as addphn
,cast(td_sysfnlib.nvp(payload,'pushNotificationDisabled', '&', '=') as varchar(100)) as pushNotificationDisabled
from pp_polestar_views.fact_ea_evnt a
where evnt_dt  > (select max_evnt_dt - 7  from ${adsWrkDB}.max_evnt_dt)
and biz_evnt_key = 55 
and sub_comp='auth_challenges'
--and authchpt is not null   --- Commented by NJ
and cast(td_sysfnlib.nvp(payload,'authchpt', '&', '=') as varchar(100)) is not null    
---Added NJ
;

collect stats on ${adsWrkDB}.AF_evnt_day column (geo_cntry); 

--------------------------------------UPDATE----------------------------------------------------

UPDATE a 
FROM pp_oap_sing_pavithra_t.dashboard_login_success3 A, 
pp_oap_sing_pavithra_t.dashboard_login_success_STS B
SET 	
login_success_sts = 1 

WHERE A.user_session_guid = b.user_Session_guid and a.login_submit = 1;

UPDATE a 
FROM pp_oap_sing_pavithra_t.dashboard_login_success2 A, 
pp_oap_sing_pavithra_t.dashboard_login_success3 B
SET 	
ul_called = b.ul_called                            
,login_shown =b.login_shown                   
,login_submit =b.login_submit                  
,login_success = b.login_success                
,login_success_sts = b.login_success_sts  
WHERE A.user_session_guid = b.user_Session_guid ;

UPDATE a
FROM
pp_oap_sing_pavithra_t.dashboard_login_success3 a,
pp_oap_sing_pavithra_t.dashboard_login_success4 b
SET 
is_Deduped_y_n2  = 'Y',
Deduped_sessn_id2 = actual_sessn_id
WHERE a.user_session_guid = b.bogus_sessn_id;


UPDATE a 
FROM pp_oap_sing_pavithra_t.dashboard_login_success3 a
SET 
flow_stage = CASE WHEN a.login_success_sts = 1 THEN '4.Login Success'	end,
last_Error_Trsntn_state = CASE WHEN a.Login_success_sts = 1 THEN 'Success'	end,
flow_stage_UL = CASE WHEN a.login_success = 1 THEN '4.Login Success'	end,
last_Error_Trsntn_state_UL = CASE WHEN a.Login_success = 1 THEN 'Success'	end 

WHERE 
login_success_sts = 1 
AND is_deduped_y_n2 = 'N';


UPDATE a 
FROM pp_oap_sing_pavithra_t.dashboard_login_success3 a, 
pp_oap_sing_pavithra_t.outliers_logins b
SET state_name = b.state_name,
int_error_code = b.int_error_code,
transition_name = b.transition_name,
flow_stage = CASE WHEN a.login_success_sts = 1 THEN '4.Login Success'
				  WHEN a.login_submit = 1 THEN '3.login Submit'
				  WHEN a.login_shown = 1 THEN '2.Login Rendered'
				  else '1.UL Called'
				end ,
last_Error_Trsntn_state = CASE WHEN a.Login_success_sts = 1 THEN 'Success'
								when a.login_submit_error <> '#' then login_submit_error
								WHEN b.transition_name <> '#' AND b.int_error_code <>'#' THEN b.transition_name||' '||'-'||' '||b.int_error_code
								WHEN b.int_error_code <>'#' THEN b.int_error_code
								WHEN b.int_error_code = '#' AND  b.transition_name <> '#' THEN b.transition_name
								WHEN b.int_error_code = '#' AND b.transition_name = '#' THEN b.state_name
							end, 
flow_stage_UL = CASE WHEN a.login_success = 1 THEN '4.Login Success'
				    WHEN a.login_submit = 1 THEN '3.login Submit'
				    WHEN a.login_shown = 1 THEN '2.Login Rendered'
				    else '1.UL Called'
				    end ,
last_Error_Trsntn_state_UL = CASE WHEN a.Login_success = 1 THEN 'Success'
								when a.login_submit_error <> '#' then login_submit_error
								WHEN b.transition_name <> '#' AND b.int_error_code <>'#' THEN b.transition_name||' '||'-'||' '||b.int_error_code
								WHEN b.int_error_code <>'#' THEN b.int_error_code
								WHEN b.int_error_code = '#' AND  b.transition_name <> '#' THEN b.transition_name
								WHEN b.int_error_code = '#' AND b.transition_name = '#' THEN b.state_name
							end 
WHERE a.user_session_guid = b.user_session_guid;




UPDATE a FROM 
pp_oap_sing_agnal_k_t.dl_gsl_ot_token_base a,
pp_oap_sing_agnal_k_t.dl_gsl_rtn_ot_rt_err b 
SET rt_error = case 
when b.int_error_code <> '#' then int_error_code else int_error_desc end
WHERE a.ec_token_id = b.ec_token_id
AND a.evnt_Dt = (select max_dt_rpt from max_dt_rpt);


--------------------------------------------------DROP----------------------------------------

drop TABLE pp_oap_sing_pavithra_t.dashboard_login_success;

---------------------------------------------DELETE-------------------------------------------

delete from pp_oap_sing_karthikeyan_t.KA_login_8ball_final_summary where evnt_dt = current_date - 2; 


-----------------------------------------------------------------------------------------

DELETE FROM pp_scratch.me_sts_login_status_value  ;



