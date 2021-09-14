with agent_hours as(
 SELECT period_start
              , period_end
              , channel_id
              , lower(tsksth.agent_email) as agent_email
              , channel_type
              , channel_name
              , timezone_agent
              , last_updated
              , 0.5::DOUBLE PRECISION as hours_worked
              , shift_status
              , rank () over (PARTITION BY period_start, tsksth.agent_email ORDER BY last_updated desc) as rank
              , headcount.wiser_id as wiser_id
        FROM reports.tmp_split_karmatime_shifts_to_hours as tsksth
        left join reports.cs_monthly_headcount as headcount
            on lower(tsksth.agent_email) = lower(headcount.AGENT_EMAIL)
            and date_trunc(month,tsksth.period_start) = headcount.date_month
        WHERE shift_status in (1,2,3,4,-10)
        ),

 workday as (
    select primary_work_email as agent_email,
           wiser_id
          , date_trunc(month,REPORT_EFFECTIVE_DATE_AND_TIME) as date
    FROM workday.headcount)

    select      period_start
              , period_end
              , channel_id
              , lower(agent_hours.agent_email) as agent_email
              , channel_type
              , channel_name
              , timezone_agent
              , last_updated
              , 0.5::DOUBLE PRECISION as hours_worked
              , shift_status
              , period_start || '_' || agent_hours.agent_email  as agent_hours_index
              , coalesce (agent_hours.wiser_id,workday.wiser_id) as wiser_id
    from agent_hours left join workday on agent_hours.agent_email = workday.agent_email
WHERE rank = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
