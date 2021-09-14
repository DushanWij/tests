with FTE as (
    select date_month, region_name, FTE, AGENT_EMAIL, active_status, IS_AGENT
from reports.cs_monthly_headcount
    where active_status = 'true' and IS_AGENT = 'true' and lead in ('Wassim Seraoui', 'Zsolt Flórián')
),

tickets as (
    SELECT  date_trunc(month,slas.created_at) as date_month,
            ceil((case when count(distinct case when fcr_ticket <= 1 then slas.ticket_id end) = 0 then null else
            count(distinct case when fcr_ticket = 1 then slas.ticket_id end) / count(distinct case when fcr_ticket <= 1 then
              slas.ticket_id end) end::double precision)*100,2) as FCR,
            ceil(avg(FIRST_RESPONSE_HOUR),2) as first_response,
            ceil(avg(HANDLING_TIME_SEC)/60,2) as handling_time,
            ceil(sum(case when ticket_first_response BETWEEN 0 and 86400 THEN 1 else 0 end) / sum(case when ticket_first_response > 0
                then 1 else Null end)*100,2)::double precision  AS cases_replied_within_24hrs,
            COALESCE(SUM(case when zc.sent_by = 'end-user' and contains_ninjas_link = False and zc.channel = 'EMAIL' then 1 ELSE 0 END),0) as email_vol,
            COALESCE(SUM(case when zc.sent_by = 'end-user' and contains_ninjas_link = False and zc.channel = 'CHAT_CASE' then 1 ELSE 0 END),0) as chat_vol
from reports.zendesk_tickets as zd
    left join reports.zendesk_tickets_extra_info as slas
    on zd.TICKET_ID = slas.TICKET_ID
    left join reports.zendesk_comments zc
    on zd.ticket_id = zc.ticket_id
    WHERE group_name = 'CS French' and zc.channel IN ('EMAIL', 'CHAT_CASE') and real_contact = 'true'
    group by 1
),

chats as (
    select date_trunc(MONTH, CHAT_STARTED_AT) as date_month,
           ceil (sum(case when IS_ABANDONED_FLAG = true then 1 else 0 end) / sum(case when chat_id is not null then 1 else null end)*100,2) as missed_chats
           from REPORTS.CS_TWILIO_CHAT_SUMMARY
           where queue = 'French - Global'
    group by 1
    )

select fte.DATE_MONTH,
       sum(FTE) actual_FTE,
       FCR,
       first_response,
       handling_time,
       missed_chats,
       cases_replied_within_24hrs,
       email_vol,
       chat_vol,
       actual_FTE/email_vol AS ratio

FROM FTE
left JOIN tickets on FTE.date_month = tickets.date_month
left join chats on fte.date_month = chats.date_month
where fte.date_month > '2019-05-01'
group by 1,3,4,5,6,7,8,9
order by 1 DESC ;

-------------------------------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM STAGING_CLEAR.lookup_sap_calls
WHERE concat(call_id, agent_email, start_time) IN( select concat( call_id, agent_email, start_time) from (
    SELECT
        call_id, agent_name_sap, start_time, end_time, queue_name, agent_email, flag_arrived, flag_handled,
       flag_false_attempt, flag_abandoned, flag_service_closed, flag_agent_disconnect, waiting_time_sec, talking_time_sec, after_work_time_sec, total_time_sec,
       source_phone, destination_phone, flag_call_in, flag_call_out, flag_voicemail, call_records_index, user_id, wiser_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                call_id, agent_name_sap, start_time, end_time, queue_name, agent_email, flag_arrived, flag_handled,
       flag_false_attempt, flag_abandoned, flag_service_closed, flag_agent_disconnect, waiting_time_sec, talking_time_sec, after_work_time_sec, total_time_sec,
       source_phone, destination_phone, flag_call_in, flag_call_out, flag_voicemail, call_records_index, user_id, wiser_id
            ORDER BY
               call_id, agent_name_sap, start_time, end_time, queue_name, agent_email, flag_arrived, flag_handled,
       flag_false_attempt, flag_abandoned, flag_service_closed, flag_agent_disconnect, waiting_time_sec, talking_time_sec, after_work_time_sec, total_time_sec,
       source_phone, destination_phone, flag_call_in, flag_call_out, flag_voicemail, call_records_index, user_id, wiser_id
        ) row_num
     FROM STAGING_CLEAR.lookup_sap_calls)
where row_num > 1
    );
----------------------------------------------------------------------------------------------------------------------------------------------------------------
