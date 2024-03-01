{{ config(materialized='table') }}

WITH prefix_cte AS (
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
    jsonb_array_elements_text(_airbyte_data->'projectIds') AS proj_id,
    value->>1 AS project_id,
    value->>0 AS project_name,
    value->>13 AS project_group,
    TO_CHAR((value->>9)::date, 'YYYY-Mon-DD') AS project_created_date,
    value->>15 as project_status
  FROM
    zoho_sprint_bronze._airbyte_raw_project_details,
    jsonb_each(_airbyte_data->'projectJObj')
),
archived_project_cte AS (
  select 
    key::text AS project_key,
--    jsonb_array_elements_text(_airbyte_data->'projectIds') AS proj_id,
    value->>1 AS project_id,
    value->>0 AS project_name,
    value->>11 AS project_group,
--    TO_CHAR((value->>3)::date, 'YYYY-Mon-DD') AS project_created_date,
    value->>8 as project_status
  FROM
    zoho_sprint_bronze._airbyte_raw_archived_projects,
    jsonb_each(_airbyte_data->'projectJObj')
),
project_status_cte AS (
    SELECT
      project_key, project_group,
      'Active Project' AS project_status
    FROM
      project_cte
    WHERE
      project_status = '1'     
    UNION   
    SELECT
      project_key,project_group,
      'Archived Project' AS project_status
    FROM
      archived_project_cte
    WHERE
      project_status = '2'
  ),
log_data_cte AS (
   select
    logJObj.value->>30 as project_id,
    logJObj.value->>31 AS project_name, 
    logJObj.value->>26 AS sprint_id,
    logJObj.value->>27 as sprint_name,
    logJObj.value->>28 as sprint_type,
    logJObj.key::text AS log_id,
	REPLACE(logJObj.value->>21, '#', '') AS log_name,
    logJObj.value->>7 AS log_owner_id,
    logJObj.value->>8 as approved_by,
	TO_CHAR((logJObj.value->>4)::date, 'YYYY-Mon-DD') AS log_submitted_date, 
	TO_CHAR((logJObj.value->>4)::date, 'Mon') AS log_submitted_month,
    EXTRACT(YEAR FROM (logJObj.value->>4)::date) AS log_submitted_year,
    TO_CHAR((logJObj.value->>5)::bigint / 3600000.0 * interval '1 hour', 'HH24:MI') AS log_hours,
    (logJObj.value->>5)::bigint / 3600000.0 AS hours_for_calculation
--	ROUND((logJObj.value->>5)::bigint / 3600000.0, 3) AS hours_for_calculation
    FROM
    zoho_sprint_bronze._airbyte_raw_global_log_hours,
    jsonb_each(_airbyte_data->'logJObj') logJObj   
    ),
user_display_names AS (
  select DISTINCT
    key::text AS user_id,
    REPLACE(value::text, '"', '') AS user_name
  FROM
    zoho_sprint_bronze._airbyte_raw_global_log_hours,
    jsonb_each(_airbyte_data->'userDisplayName')
)
select
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
--  ldc.log_name,
  udn.user_name AS log_owner_name,
  udn_approved.user_name AS approved_by_name,
  ldc.log_submitted_date,
  ldc.log_submitted_month,
  ldc.log_submitted_year,
  ldc.log_hours,
  ldc.hours_for_calculation,
  project_group,
  COALESCE(ps.project_status, 'Unknown') AS project_status
FROM
  log_data_cte ldc
LEFT JOIN
  user_display_names udn ON ldc.log_owner_id = udn.user_id 
LEFT JOIN
  user_display_names udn_approved ON ldc.approved_by = udn_approved.user_id
JOIN
  project_status_cte ps ON ldc.project_id = ps.project_key
RIGHT JOIN
  zoho_sprint_bronze.employee_data ev ON REPLACE(udn.user_name, '"', '') =  ev."Concat"
where		--  check only for approved users
  udn_approved.user_name IS NOT null
  AND ps.project_group IN ('Kastech US', 'Kastech MENA', 'Kastech APAC')