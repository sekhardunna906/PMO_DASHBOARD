{{ config(materialized='table') }}

 select "EmployeeID","Concat","Reporting To","Business Unit","Practice","Sub Practice","Employee Type",
 "HR Status","Employee Status",
 COALESCE(TO_CHAR(NULLIF("Date of Joining", '')::DATE, 'DD-MM-YYYY'), 'NULL') as "Date of Joining",
 COALESCE(TO_CHAR(NULLIF("Date of Exit", '')::DATE, 'DD-MM-YYYY'), 'NULL') as "Date of Exit"
 from
 zoho_sprint_bronze.employee_dump