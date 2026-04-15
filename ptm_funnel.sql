query ="""SET enable_case_sensitive_identifier TO true;

WITH base AS (
    SELECT distinct
        ptm.id AS ptm_id,
        ptm.created_on::DATE AS ptm_created_date,
        ptm.scheduled_on::DATE AS ptm_scheduled_date,
        ptm.due_date,
        ptm.student_id,
        ptm.teacher_id,
        ptm.teacher_classroom_id,
        ptm.ptm_state as This_is_scheduled_and_this_is,
        ptm.is_deleted,

        CASE WHEN ptm.scheduled_on IS NOT NULL THEN 'SCHEDULED' ELSE 'NOT_SCHEDULED' END AS scheduled_status,
        CASE WHEN S.meta.derived_region::text = 'UK' THEN 'ROW' ELSE S.meta.derived_region::text END AS derived_region,

        -- Meta fields
        ptm.meta.is_moment_ptm,
        ptm.meta.opportunity_action_id::text  AS opportunity_action_id,
        ptm.meta.ptm_platform::text AS ptm_platform,
        ptm.meta.student_classroom_id::text AS student_classroom_id,
        ptm.meta.cancel_reason::text AS cancel_reason,
        ptm.meta.parent_satisfied AS parent_satisfied,
        ptm.meta.teacher_cancellation_comment::text AS teacher_cancellation_comment,
        ptm.meta.reschedule_reason::text AS reschedule_reason,
        ptm.meta.referral_feedback.selection::text AS referral_feedback_selection,
        ptm.meta.referral_feedback."reasonDesc"::text AS referral_feedback_reason_desc,
        ptm.report.report_sections."focus_area"::text as teacher_observation,

        -- UOA fields
        uoa.id AS uoa_id,
        uoa.created_on AS moment_created_on,
        to_char(uoa.created_on, 'mm-yyyy') as moment_month,
        uoa.code AS uoa_code,
        uoa.state AS moment_state,
        uoa.sub_state AS action_stage,
        uoa.moment_id AS uoa_moment_id,

        -- Duration
        CASE
            WHEN ptm.ptm_state = 'DONE'
            AND ptm.scheduled_on IS NOT NULL
            AND ptm.meta.ptm_end_time IS NOT NULL
            THEN DATEDIFF('minute', ptm.scheduled_on, ptm.meta.ptm_end_time::TIMESTAMP)
            ELSE NULL
        END AS ptm_duration_minutes,

        -- Extend
        CASE WHEN ptm.meta.reschedule_reason::text IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_rescheduled,

        -- Parent Feedback Fields (Casted to varchar for Redshift SUPER)
      ptm.feedback.parent_feedback[0].liked AS is_report_insightful,
      ptm.feedback.parent_feedback[1].liked AS understands_child_needs,
      ptm.feedback.parent_feedback[2].liked AS addressed_concerns,
      ptm.feedback.parent_feedback[3].liked AS proper_engagement,

        -- Parent feedback received flag
        CASE WHEN ptm.meta.parent_satisfied IS NOT NULL THEN 'YES' ELSE 'NO' END AS parent_feedback_received

    FROM application_service_ptm.ptm ptm
    LEFT JOIN application_service_intelenrollment.user_opportunity_action uoa
        ON ptm.meta.opportunity_action_id::text = uoa.id::text
    LEFT JOIN application_service_intelenrollment.student S
        ON ptm.student_id = S.id
    WHERE ptm.is_deleted = FALSE
        AND uoa.created_on::DATE >= '2025-04-01'
)
SELECT distinct
    b.moment_month,
    b.moment_created_on,
    b.ptm_scheduled_date,
    b.ptm_id,
    b.student_id,
    b.teacher_id,
    b.teacher_classroom_id,
    b.This_is_scheduled_and_this_is,
    b.scheduled_status,
    b.derived_region,
    b.ptm_platform,
    b.uoa_code,
    b.moment_state,
    b.action_stage,
    b.is_rescheduled,
    b.reschedule_reason,
 --   (EXTRACT(HOUR FROM (s.scheduled_end_time::time - s.scheduled_start_time::time)) * 60 + 
-- EXTRACT(MINUTE FROM (s.scheduled_end_time::time - s.scheduled_start_time::time))) AS ptm_duration_minutes,
   --(s.scheduled_end_datetime - s.scheduled_start_datetime) AS ptm_duration_minutes,
   DATEDIFF('minute', s.scheduled_start_datetime, s.scheduled_end_datetime) AS ptm_duration_mins,
    b.referral_feedback_reason_desc,
    b.parent_satisfied,
    b.teacher_cancellation_comment,
    b.cancel_reason,
    b.teacher_observation,
    b.is_report_insightful,
    b.understands_child_needs,
    b.addressed_concerns,
    b.proper_engagement
FROM base b
left JOIN application_service_classroom.student_classroom s on b.teacher_classroom_id=s.teacher_classroom_id
-- WHERE b.teacher_id = '3977eb88-6155-11eb-beda-0624d341e5a7'
ORDER BY moment_created_on DESC"""

# df = pd.read_sql(query, conn)
# ws = client.open("PTM_Data").worksheet("Data")
# gd.set_with_dataframe(ws, df)
