create table if not exists {{params.reports}}.chat_csat_scores (
      CHAT_ID               varchar
    , SURVEY_SENT_AT        timestamp
    , RESPONDED_AT          timestamp
    , CSAT_SCORE            float
    , FEEDBACK              varchar
    , COMMENT               varchar
    , ZENDESK_TICKET_ID     number
);

TRUNCATE TABLE {{params.reports}}.chat_csat_scores;

insert into {{params.reports}}.chat_csat_scores(
    select CHAT_ID
         , case
             when CHAT_OUTCOME NOT IN ('TRANSFERRED', 'MISSED') then CHAT_ENDED_AT
             else null end                                                as SURVEY_SENT_AT
         , case when answer is null then null else css.created_at end     as RESPONDED_AT
         , MAX(Case
                   when chat_survey.QUESTION = 'HOW_WOULD_YOU_RATE_YOUR_CHAT_WITH' then chat_survey.answer
                   else null end)                                         as csat_score
         , MAX(Case
                   when chat_survey.QUESTION = 'ONE_MORE_THING_DID_WE_ANSWER_YOUR_QUESTION' then chat_survey.answer
                   else null end)                                         as FEEDBACK
         , MAX(Case
                   when chat_survey.QUESTION = 'SO_MAKE_SURE_YOU_VE_TOLD_US_EVERYTHING_BEFORE_SUBMITTING'
                       then chat_survey.answer
                   else null end)                                         as COMMENT
         , TICKET_ID                                                      as ZENDESK_TICKET_ID
    from {{params.reports}}.cs_twilio_chat_summary tcs
             left join CONTACTDB.chat_session_survey css on tcs.chat_id = css.chat_session_id
             left join CONTACTDB.chat_session_survey_surveys csss on css.id = csss.chat_session_survey_id
             left join CONTACTDB.chat_survey on csss.surveys_id = chat_survey.id
    group by 1, 2, 3, 7
);
