{{ config(materialized='table') }}

  WITH 
  prefix_cte AS (
    SELECT
      key AS project_key,
      TRIM(BOTH '"' FROM value::text) AS prefix
    FROM
      zoho_sprint_bronze._airbyte_raw_project_details,
      jsonb_each(_airbyte_data->'prefixObj')
  ),
  project_cte AS (
    SELECT
      key::text AS project_key,
      value->>1 AS project_id,
      value->>0 AS project_name,
      value->>13 AS project_group,
      CASE WHEN value->>15 = '1' THEN 'Active Project' ELSE value->>15 END AS project_status
    FROM
      zoho_sprint_bronze._airbyte_raw_project_details,
      jsonb_each(_airbyte_data->'projectJObj')
    WHERE
      value->>13 IN ('Kastech US', 'Kastech MENA', 'Kastech APAC')
  ),
  log_data_cte AS (
    SELECT
      logJObj.value->>30 AS project_id,
      logJObj.value->>31 AS project_name, 
      logJObj.value->>26 AS sprint_id,
      logJObj.value->>27 AS sprint_name,
      logJObj.value->>28 AS sprint_type,
      logJObj.key::text AS log_id,
      REPLACE(logJObj.value->>21, '#', '') AS log_name,
      logJObj.value->>7 AS log_owner_id,
      logJObj.value->>8 AS approved_by,
      TO_CHAR((logJObj.value->>4)::date, 'YYYY-Mon-DD') AS log_submitted_date, 
      TO_CHAR((logJObj.value->>4)::date, 'Mon') AS log_submitted_month,
      EXTRACT(YEAR FROM (logJObj.value->>4)::date) AS log_submitted_year,
      TO_CHAR((logJObj.value->>5)::bigint / 3600000.0 * interval '1 hour', 'HH24:MI') AS log_hours,
      (logJObj.value->>5)::bigint / 3600000.0 AS hours_for_calculation
    FROM
      zoho_sprint_bronze._airbyte_raw_global_log_hours,
      jsonb_each(_airbyte_data->'logJObj') logJObj   
  ),
  user_display_names AS (
    SELECT DISTINCT
      key::text AS user_id,
      REPLACE(value::text, '"', '') AS user_name
    FROM
      zoho_sprint_bronze._airbyte_raw_global_log_hours,
      jsonb_each(_airbyte_data->'userDisplayName')
  ),
  archived_project_cte AS (
    SELECT 
      key::text AS project_key,
      value->>1 AS project_id,
      value->>0 AS project_name,
      value->>11 AS project_group,
      CASE WHEN value->>8 = '2' THEN 'Archived Project' ELSE value->>8 END AS project_status
    FROM
      zoho_sprint_bronze._airbyte_raw_archived_projects,
      jsonb_each(_airbyte_data->'projectJObj')
    WHERE 
      value->>11 IN ('Kastech US', 'Kastech MENA', 'Kastech APAC')
  ), 
  sprint_data_cte AS (
    SELECT
      sprint_jobj.key::text AS sprint_id,
      sprint_jobj.value->>0 AS sprint_name,
      r."_airbyte_data"->>'project_id' AS project_id,
      sprint_jobj.value->>5 AS sprint_type,
      TO_CHAR((sprint_jobj.value->>7)::date, 'YYYY-Mon-DD') AS sprint_created_date
    FROM
      zoho_sprint_bronze._airbyte_raw_archived_sprints r,
      jsonb_each(_airbyte_data->'sprintJObj') sprint_jobj
  ),
  log_data_archived_cte AS (
    SELECT
      logJObj.key::text AS log_id,
      logJObj.value->>0 AS sprint_id,
      logJObj.value->>2 AS log_name,
      logJObj.value->>10 AS log_owner_id,
      logJObj.value->>18 AS approved_by,
      TO_CHAR((logJObj.value->>11)::date, 'YYYY-Mon-DD') AS log_submitted_date, 
      TO_CHAR((logJObj.value->>11)::date, 'Mon') AS log_submitted_month,
      EXTRACT(YEAR FROM (logJObj.value->>11)::date) AS log_submitted_year,
      TO_CHAR((logJObj.value->>12)::bigint / 3600000.0 * interval '1 hour', 'HH24:MI') AS log_hours,
      ROUND((logJObj.value->>12)::bigint / 3600000.0, 2) AS hours_for_calculation
    FROM
      zoho_sprint_bronze._airbyte_raw_archived_timesheets,
      jsonb_each(_airbyte_data->'logJObj') logJObj
  ),  
  user_display_names_archived AS (
    SELECT DISTINCT
      key::text AS user_id,
      REPLACE(value::text, '"', '') AS user_name
    FROM
      zoho_sprint_bronze._airbyte_raw_archived_timesheets,
      jsonb_each(_airbyte_data->'userDisplayName')
  )
SELECT
  ldc.project_id,
  ldc.project_name,
  ldc.sprint_id,
  ldc.sprint_name,
  CASE
    WHEN CAST(ldc.sprint_type AS INTEGER) = 1 THEN 'Upcoming sprint'
    WHEN CAST(ldc.sprint_type AS INTEGER) = 2 THEN 'Active  sprint'
    WHEN CAST(ldc.sprint_type AS INTEGER) = 3 THEN 'Completed  sprint'
    WHEN CAST(ldc.sprint_type AS INTEGER) = 4 THEN 'Canceled  sprint'
    WHEN CAST(ldc.sprint_type AS INTEGER) = 5 THEN 'Backlog'
  END AS sprint_type,
  ldc.log_id,
  ldc.log_name,
  udn.user_name AS log_owner_name,
  udn_approved.user_name AS approved_by_name,
  ldc.log_submitted_date,
  ldc.log_submitted_month,
  ldc.log_submitted_year,
  ldc.log_hours,
  ldc.hours_for_calculation,
  pc.project_group,
  pc.project_status
FROM
  log_data_cte ldc
LEFT JOIN
  user_display_names udn ON ldc.log_owner_id = udn.user_id 
JOIN 
  project_cte pc ON pc.project_key = ldc.project_id
LEFT JOIN
  user_display_names udn_approved ON ldc.approved_by = udn_approved.user_id
RIGHT JOIN
  zoho_sprint_bronze.employee_data ev ON REPLACE(udn.user_name, '"', '') = ev."concat"
WHERE
  udn_approved.user_name IS NOT NULL
UNION ALL
SELECT
  adc.project_key AS project_id,
  adc.project_name,
  ldac.sprint_id,
  sdc.sprint_name,
  CASE
    WHEN CAST(sdc.sprint_type AS INTEGER) = 1 THEN 'Upcoming sprint'
    WHEN CAST(sdc.sprint_type AS INTEGER) = 2 THEN 'Active  sprint'
    WHEN CAST(sdc.sprint_type AS INTEGER) = 3 THEN 'Completed  sprint'
    WHEN CAST(sdc.sprint_type AS INTEGER) = 4 THEN 'Canceled  sprint'
    WHEN CAST(sdc.sprint_type AS INTEGER) = 5 THEN 'Backlog'
  END AS sprint_type,
  ldac.log_id,
  ldac.log_name,
  udn.user_name AS log_owner_name,
  udn_approved.user_name AS approved_by_name,
  ldac.log_submitted_date,
  ldac.log_submitted_month,
  ldac.log_submitted_year,
  ldac.log_hours,
  ldac.hours_for_calculation,
  adc.project_group,
  adc.project_status
FROM
  log_data_archived_cte ldac
LEFT JOIN
  sprint_data_cte sdc ON ldac.sprint_id = sdc.sprint_id
JOIN
  archived_project_cte adc ON sdc.project_id = adc.project_key
LEFT JOIN
  user_display_names_archived udn ON ldac.log_owner_id = udn.user_id
LEFT JOIN
  user_display_names_archived udn_approved ON ldac.approved_by = udn_approved.user_id
WHERE
  udn_approved.user_name IS NOT NULL