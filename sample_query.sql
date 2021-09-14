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

with agent_days as(
SELECT DISTINCT to_char(date_trunc(day,ts),'yyyy-mm-dd') date,count(*) t, channel_id
                      FROM slack.messages
                     WHERE channel_id = 'CLYNPV90R' AND ts LIKE '%2021-06-01 13:39:13%'
                        OR ts LIKE '%2021-01-26 11:41:27%'
                        OR ts LIKE '%2021-04-13 08:25:04%'
                        OR ts LIKE '%2021-07-14 10:35:12%'
                        OR ts LIKE '%2021-01-11 11:57:05%'
                        OR ts LIKE '%2021-02-03 16:10:29%'
                        OR ts LIKE '%2021-02-17 09:01:12%'
                        OR ts LIKE '%2021-03-09 10:21:58%'
                        OR ts LIKE '%2021-03-03 11:48:30%'
                        OR ts LIKE '%2021-03-11 09:09:27%'
                        OR ts LIKE '%2021-03-23 08:54:42%'
                        OR ts LIKE '%2021-05-11 14:13:52%'
                        OR ts LIKE '%2021-05-18 11:01:51%'
                        OR ts LIKE '%2021-05-18 10:24:55%'
                        OR ts LIKE '%2021-06-30 10:56:09%'
                        OR ts LIKE '%2021-07-08 15:08:24%'
                        OR ts LIKE '%2021-07-22 13:28:05%'
                        OR ts LIKE '%2021-02-16 02:57:41%'
                        OR ts LIKE '%2021-02-22 15:04:25%'
                        OR ts LIKE '%2021-04-20 12:27:43'
                        OR ts LIKE '%2021-04-14 07:26:36%'
                        OR ts LIKE '%2021-04-28 08:51:06%'
                        OR ts LIKE '%2021-05-20 11:04:17%'
                        OR ts LIKE '%2021-05-19 20:41:25%'
                        OR ts LIKE '%2021-06-21 18:40:50%'
                        OR ts LIKE '%2021-06-29 18:17:44%'
                        OR ts LIKE '%2021-07-05 06:54:30%'
                        OR ts LIKE '%2021-07-09 15:17:47%'
                        OR ts LIKE '%2021-07-15 16:03:18%'
                        OR ts LIKE '%2021-01-11 11:15:57%'
                        OR ts LIKE '%2021-01-08 11:54:08%'
                        OR ts LIKE '%2021-01-08 08:16:04%'
                        OR ts LIKE '%2021-01-07 12:06:34%'
                        OR ts LIKE '%2021-02-08 15:53:01%'
                        OR ts LIKE '%2021-02-08 07:13:23%'
                        OR ts LIKE '%2021-02-22 16:20:47%'
                        OR ts LIKE '%2021-03-10 10:09:31%'
                        OR ts LIKE '%2021-03-23 09:41:50%'
                        OR ts LIKE '%2021-04-06 06:50:28%'
                        OR ts LIKE '%2021-04-08 07:33:59%'
                        OR ts LIKE '%2021-04-07 13:43:18%'
                        OR ts LIKE '%2021-04-26 03:20:39%'
                        OR ts LIKE '%2021-06-07 11:20:49%'
                        OR ts LIKE '%2021-06-05 08:44:40%'
                        OR ts LIKE '%2021-06-15 14:16:35%'
                        OR ts LIKE '%2021-06-11 14:38:50%'
                        OR ts LIKE '%2021-06-16 09:51:24%'
                        OR ts LIKE '%2021-06-23 15:26:33%'
                        OR ts LIKE '%2021-07-09 09:52:28%'
                        OR ts LIKE '%2021-07-20 06:44:21%'
                        OR ts LIKE '%2021-07-19 13:54:11%'
                        OR ts LIKE '%2021-01-06 16:56:12%'
group by 1,3
order by 1 ASC),

all_texts as (select DISTINCT to_char(date_trunc(day,ts),'yyyy-mm-dd') date,count(*) t, channel_id
                      FROM slack.messages
 WHERE TS > '2021-01-01'
   and channel_id = 'CLYNPV90R'
   and ts < '2021-07-28'
group by 1,3
order by 1 asc)
select all_texts.date,all_texts.t,agent_days.t
from all_texts LEFT JOIN agent_days on all_texts.channel_id = agent_days.channel_id and all_texts.date = agent_days.date;

----------------------------------------------------------------------------------------------------------------------------------------------------------------

UPDATE sandbox_db.sandbox_dushan_wijesinghe.lookup_agent_hours1 as lah1 set lah1.wiser_id = hc.wiser_id
FROM reports.cs_monthly_headcount as hc
where lower(hc.agent_email) = lower(lah1.agent_email);

UPDATE sandbox_db.sandbox_dushan_wijesinghe.lookup_agent_hours1 as lah1 set lah1.wiser_id = h.wiser_id
FROM workday.headcount as h
where lower(h.primary_work_email) = lower(lah1.agent_email) and lah1.wiser_id is null;

UPDATE sandbox_db.sandbox_dushan_wijesinghe.lookup_agent_hours1 as lah1 set lah1.wiser_id =  case
                                                                              when lah1.wiser_id = 2618 then 3821
                                                                              when lah1.wiser_id = 2086 then 3078
                                                                              when lah1.wiser_id = 3073 then 3750
                                                                              when lah1.wiser_id = 4110 then 4637
                                                                              else wiser_id END;
                                                                              
-----------------------------------------------------------------------------------------------------------------------------------------------------------------


