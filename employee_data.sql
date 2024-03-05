{{ config(materialized='table') }}
 
 select "employeeid","concat","Reporting To","Business Unit","practice","Sub Practice","Employee Type",
 "HR Status","Employee Status",
  COALESCE(NULLIF("Date of Joining", ''),'01-01-0001')::DATE as "Date of Joining",
  COALESCE(NULLIF("Date of Exit", ''),'01-01-0001')::DATE AS "Date of Exit"
 from
 zoho_sprint_bronze.employee_dump