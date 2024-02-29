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
    logJObj.value->>21 as log_name,
    logJObj.value->>7 AS log_owner_id,
    logJObj.value->>8 as approved_by_name,
	TO_CHAR((logJObj.value->>4)::date, 'YYYY-Mon-DD') AS log_submitted_date,
	EXTRACT(MONTH FROM (logJObj.value->>4)::date) AS log_submitted_month,
	TO_CHAR((logJObj.value->>4)::date, 'Mon') AS log_submitted_month_name,
    EXTRACT(YEAR FROM (logJObj.value->>4)::date) AS log_submitted_year,
    TO_CHAR((logJObj.value->>5)::bigint / 3600000.0 * interval '1 hour', 'HH24:MI') AS log_hours,
	(logJObj.value->>5)::bigint / 3600000.0 AS hours_for_calculation
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
),
calendar_data_cte AS (
  SELECT
    "Year",
    "Month",
    "Total Working Hours"
  FROM
    zoho_sprint_bronze.holiday_calendar
)
select
  udn.user_name AS log_owner_name,
  ldc.log_submitted_year as Year,
  ldc.project_name,
  ldc.log_submitted_month_name as month,
  (SUM(ldc.hours_for_calculation)) AS actual_hours,
  udn_approved.user_name AS approved_by,
  cd."Total Working Hours" AS available_hours,
  round((SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100, 2) AS "utilization %",
    CASE
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 120 THEN 'A'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 100 THEN 'B'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 75 THEN 'C'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 50 THEN 'D'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 0 THEN 'E'
  END AS grade,
  CASE
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 120 THEN '120% - 150%'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 100 THEN '100% - 119%'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 75 THEN '75% - 99%'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 50 THEN '50% - 74%'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 0 THEN '0% - 49%'
  END AS range,
  CASE
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 120 THEN 'Overutilized'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 100 THEN 'Balance Utilized'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 75 THEN 'Underutilized'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 50 THEN 'Underutilized'
    WHEN (SUM(ldc.hours_for_calculation) / cd."Total Working Hours") * 100 >= 0 THEN 'Underutilized'
  END AS bucket
FROM
  log_data_cte ldc
LEFT JOIN
  user_display_names udn ON ldc.log_owner_id = udn.user_id 
LEFT JOIN
  user_display_names udn_approved ON ldc.approved_by_name = udn_approved.user_id
JOIN
  project_status_cte ps ON ldc.project_id = ps.project_key 
JOIN
  calendar_data_cte cd ON ldc.log_submitted_year = cd."Year" AND ldc.log_submitted_month_name = cd."Month"
RIGHT JOIN
  zoho_sprint_bronze.employee_data ev ON REPLACE(udn.user_name, '"', '') = ev."concat"
where		--  check only for approved users
  udn_approved.user_name IS NOT null
  AND ps.project_group IN ('Kastech US', 'Kastech MENA', 'Kastech APAC')
  GROUP by	--  calculating the total_hours according to month wise.
  udn.user_name,
  ldc.log_submitted_year,
  ldc.project_name,
  ldc.log_submitted_month_name,
  udn_approved.user_name,
  cd."Total Working Hours"