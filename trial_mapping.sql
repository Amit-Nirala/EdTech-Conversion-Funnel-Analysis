query = """
-- --no_cache
WITH Final_Region_Flag_for_student as (
SELECT 
    s.id ,
    s.intel_enrollment_id AS student_id,
    s.meta.source::TEXT AS source,
    u.source_channel AS source_channel,
    MAX(s.current_grade) AS ssid_grade,
    MAX(CASE
        WHEN p.meta.region_data.opportunity_region::TEXT = 'MIDDLE-EAST'  THEN 'IME'
        WHEN p.meta.region_data.opportunity_region::TEXT = 'EUROPE'       THEN 'EUK'
        WHEN p.meta.region_data.opportunity_region::TEXT = 'AUS'          THEN 'APAC'
        WHEN p.meta.region_data.opportunity_region::TEXT = 'ASIA'         THEN 'APAC'
        WHEN p.meta.region_data.opportunity_region::TEXT = 'IND'          THEN 'IME'
        WHEN p.meta.region_data.opportunity_region::TEXT = 'USA'          THEN 'NAM'
        WHEN p.meta.region_data.region::TEXT = 'UK-EU-ME-AFRICA'          THEN 'IME'
        WHEN p.meta.region_data.region::TEXT = 'APAC-AUS-NZ'              THEN 'APAC'
        WHEN p.meta.region_data.region::TEXT = 'IND-SUB'                  THEN 'IME'
        WHEN p.meta.region_data.region::TEXT = 'US-CANADA'                THEN 'NAM'
        WHEN u.meta.opportunity_region_data.region::TEXT = 'UK-EU-ME-AFRICA'   THEN 'EUK'
        WHEN u.meta.opportunity_region_data.region::TEXT = 'APAC-AUS-NZ'       THEN 'APAC'
        WHEN u.meta.opportunity_region_data.region::TEXT = 'IND-SUB'           THEN 'IME'
        WHEN u.meta.opportunity_region_data.region::TEXT = 'US-CANADA'         THEN 'NAM'
        WHEN u.sales_process = 'DIRECT_ADMISSION'                         THEN 'IME'
        WHEN u.sales_process = 'DIRECT_ADMISSION_USA'                     THEN 'NAM'
        ELSE p.meta.region_data.region::TEXT
    END) AS region,
    MAX(CASE
        WHEN p.meta.region_data.opportunity_country IS NOT NULL
            THEN p.meta.region_data.opportunity_country::TEXT
        ELSE
            p.meta.region_data.country::TEXT
    END) AS country
FROM application_service_parent_student.student AS s
LEFT JOIN application_service_parent_student.parent AS p ON p.id = s.parent_id
LEFT JOIN application_service_parent_student.user_source_log AS u ON u.user_id = s.id
GROUP BY 1, 2, 3, 4
  )


, STUDENT_DATA AS (
    SELECT 
        s.id AS student_id, 
        s.student_service_id, 
        s.region, 
        s.student_type,
        s.first_name||' '||s.last_name as student_name,
        p_table.primary_user_id AS parent_id -- Parent ID fetch logic
    FROM application_service_intelenrollment.student s
    LEFT JOIN application_service_parent_student.parent p_table ON s.parent_service_id = p_table.id
    WHERE s.is_demo = false -- Filter from your new logic
), 
payment_data AS (
  SELECT 
    student_id, 
    state, 
    invoice_id, 
    amount, 
    paid_on_dt,
    JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(meta), 'teacher_id') AS real_teacher,
    ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY paid_on_dt ASC) as payment_rank
  FROM application_service_intelenrollment.student_payment_link
  WHERE 0=0
    AND invoice_id NOT LIKE 'Dummy%'
    AND invoice_id NOT LIKE 'cls_extension%'
    AND state = 'CNF'
    AND invoice_id NOT LIKE '%dummy%'
    AND invoice_id NOT LIKE '%test%'
    AND invoice_id NOT LIKE '%dup%'
    AND invoice_id NOT LIKE '%can%'
    AND invoice_id NOT LIKE '%cls_transfer%'
    AND invoice_id NOT LIKE '%cls_extension%'
    AND invoice_id NOT LIKE '%tets%'
    AND invoice_id NOT LIKE '%stuent_UP_%'
    AND invoice_id NOT LIKE '%upgrade_v2%'  
    AND invoice_id is NOT null
),
region_code as( 
               SELECT teacher_id,region_code,is_revoked 
               FROM application_service_teacher.teacher_license_region
               where is_revoked = false
--                and teacher_id='430b6e4c-f0f1-11ea-acc9-06eaa8ca921d'
               )


, student_demo AS (
   SELECT d.id as student_demo_id,
          d.student_id,
          s.student_name,
          d.teacher_id,
          tr.region_code,
          t.first_name||' '||t.last_name as teacher_name,
          d.state,
          d.demo_state,
          d.scheduled_start_time,
          d.source,
          d.grade,
          d.meta.is_transfer_trial::boolean as is_transfer_trial,
          d.meta.action_source::text as action_source,
--           s.region,
          s.student_type,
          s.parent_id,
          d.is_deleted,
          d.student_classroom_id,
          mbt.channel,
          COALESCE(mbt.prospectid, 'stud_id_'||d.student_id) as prospectid,
          COALESCE(mbt.prospectstage,'Others') as prospect_stage
   FROM application_service_classroom.student_demo d
   LEFT JOIN STUDENT_DATA s on d.student_id=s.student_id 
   LEFT JOIN data_playground.mbt_trials mbt on d.student_id=mbt.student_id
   LEFT JOIN application_service_teacher.teacher t on d.teacher_id=t.id
   LEFT JOIN region_code tr on d.teacher_id=tr.teacher_id
   
   WHERE d.is_deleted = false
   -- Past 3 Months Filter (Adjusted as per your snippet)
   AND d.scheduled_start_time >= '2025-07-01 07:00:00'
),
base_data AS (
    SELECT 
        sd.teacher_id,
        sd.teacher_name,
        sd.student_id,
        sd.student_name,
        sd.grade,
        a.meta."assessment_id"::text AS assessment_id,
        CASE WHEN a.meta."assessment_id"::text is NOT NULL and a.demo_teacher_id is NOT NULL THEN 'Auto_Mapped' ELSE 'Manual_Mapped' END as mapping_state,
        --CASE WHEN a.demo_teacher_id is NOT NULL THEN 'Auto_Mapped' ELSE 'Manual_Mapped' END as mapping_state,
        --CASE WHEN a.meta."assessment_id"::text IS NOT NULL THEN 'Auto_Mapped' ELSE 'Manual_Mapped' END as mapping_state,
        sd.scheduled_start_time,
        p.paid_on_dt,
        p.real_teacher,
        sd.demo_state,
        sd.student_classroom_id,
        sd.channel,
        sd.prospectid,
        sd.prospect_stage,
        sd.is_transfer_trial,
        a.state,
        sd.student_type,
        sd.parent_id,
        convert_timezone(
        'UTC',
        'Asia/Kolkata',
        timestamp 'epoch' + (a.trial_slot."from")::bigint * interval '1 second'
    ) as slottime_requested,
        ROW_NUMBER() OVER (PARTITION BY sd.student_id, COALESCE(a.meta."assessment_id"::text,'no_assessment'), sd.scheduled_start_time ORDER BY  a.created_on DESC, sd.scheduled_start_time DESC) as student_overall_rn,
  CASE WHEN sd.teacher_id = sd.parent_id THEN 1 ELSE 0 END AS own_child,
        --COALESCE(a.region, sr.region, sd.region_code) as region
        COALESCE(
            NULLIF(TRIM(a.region), ''),        --  1st priority: auto-mapping region
            NULLIF(TRIM(sr.region), ''),       -- 2nd priority: student's parent/USL region
            NULLIF(TRIM(sd.region_code), '')   -- 3rd priority: teacher's licensed region
        ) AS region
  FROM student_demo sd
  LEFT JOIN application_service_teacher.auto_mapping_trial_request a on sd.student_demo_id=a.student_demo_id AND (a.is_deleted = false OR a.is_deleted IS NULL)
  LEFT JOIN payment_data p on sd.student_id=p.student_id AND sd.teacher_id=p.real_teacher
  LEFT JOIN Final_Region_Flag_for_student sr on sd.student_id=sr.student_id
),
metrics_calc AS (
    SELECT 
        to_char(scheduled_start_time, 'mm-yyyy') as demo_month_3,
        scheduled_start_time::date as demo_scheduled_date,
        scheduled_start_time::time as demo_scheduled_timestamp,
        prospectid,
        prospect_stage,
        CASE WHEN grade <= 0 then '0' else grade end as grade,
        student_id,
        student_name,
        teacher_id,
        teacher_name,
        student_classroom_id,
        --demo_state,
        case when slottime_requested = scheduled_start_time then 
    case when state='AUTO_MAPPED' and demo_state in('DONE','SCHEDULED','NO_ATTEMPTS','REQUESTED','HALF_DONE','RESCHEDULED') THEN demo_state else state end
else demo_state end as demo_status,
        paid_on_dt,
        real_teacher,
        COALESCE(channel, 'Perf_others') as channel,
        mapping_state,
        assessment_id,
        is_transfer_trial, 
        region,
        own_child
       
    FROM base_data
    WHERE student_overall_rn = 1
    and student_type='STUDENT'
)

SELECT * FROM metrics_calc
where 0=0
 and teacher_id !='3b5323b0-eb05-11ec-a2a8-86eb6a300e32'
 and own_child !=1
"""

-- df = pd.read_sql(query, conn)
-- ws = client.open("Manual+Auto trial AI").worksheet("Data_Dump")
-- gd.set_with_dataframe(ws, df)
