use database tickets_db;
use warehouse compute_wh;

---************************************************
--- Truncate staging tables from sales_stg schema
---************************************************

/*
put file://allevents_pipe.csv @tickets_stage/events/;
put file://venue_pipe.csv @tickets_stage/venue/;
put file://listings_pipe.csv @tickets_stage/listings/;
put file://category_pipe.csv @tickets_stage/category/;
put file://allusers_pipe.csv @tickets_stage/users/;
put file://sales_tab.csv @tickets_stage/sales/;
put file://date2008_pipe.csv @tickets_stage/date/;
*/
list @tickets_stage;

---************************************************
--- Truncate staging tables from sales_stg schema
---************************************************
use schema sales_stg;
truncate table CATEGORY_STG;
truncate table DATE_STG;
truncate table EVENTS_STG;
truncate table LISTINGS_STG;
truncate table SALES_STG;
truncate table USERS_STG;
truncate table VENUE_STG;

---************************************************
--- Execute Copy To Stage task Manually
---************************************************

-- Executed Task Ontime
execute task tickets_ingest_tasks;

show tasks;

---************************************************
--- validating staging data
---************************************************

use tickets_db;
use schema sales_stg;
select 'CATEGORY_STG' as table_name,count(*) as total_rows from CATEGORY_STG UNION ALL
select 'DATE_STG' as table_name,count(*) as total_rows from DATE_STG UNION ALL
select 'EVENTS_STG' as table_name,count(*) as total_rows from EVENTS_STG UNION ALL
select 'LISTINGS_STG' as table_name,count(*) as total_rows from LISTINGS_STG UNION ALL
select 'SALES_STG' as table_name,count(*) as total_rows from SALES_STG UNION ALL
select 'USERS_STG' as table_name,count(*) as total_rows from USERS_STG UNION ALL
select 'VENUE_STG' as table_name,count(*) as total_rows from VENUE_STG;


---************************************************
--- Verify Streams
---************************************************
use schema sales;
select 'CATEGORY_STG' as table_name,count(*) as total_rows from CATEGORY_STG_STREAM UNION ALL
select 'DATE_STG' as table_name,count(*) as total_rows from DATE_STG_STREAM UNION ALL
select 'EVENTS_STG' as table_name,count(*) as total_rows from EVENTS_STG_STREAM UNION ALL
select 'LISTINGS_STG' as table_name,count(*) as total_rows from LISTINGS_STG_STREAM UNION ALL
select 'SALES_STG' as table_name,count(*) as total_rows from SALES_STG_STREAM UNION ALL
select 'USERS_STG' as table_name,count(*) as total_rows from USERS_STG_STREAM UNION ALL
select 'VENUE_STG' as table_name,count(*) as total_rows from VENUE_STG_STREAM;

---************************************************
--- Execute Final Tasks to load Dim's and Fact's
---************************************************

USE SCHEMA SALES;
execute task CATEGORY_DIM_TASK;
execute task DATE_DIM_TASK;
execute task USERS_DIM_TASK;
execute task VENUE_DIM_TASK;
execute task EVENTS_FACT_TASK;
execute task LISTINGS_FACT_TASK;
execute task SALES_FACT_TASK;


---************************************************
--- Validating Dim's and Facts Data
---************************************************

use tickets_db;
use schema sales;

select 'CATEGORY_DIM' as table_name,count(*) as total_rows from CATEGORY_DIM UNION ALL
select 'DATE_DIM' as table_name,count(*) as total_rows from DATE_DIM UNION ALL
select 'EVENTS_FACT' as table_name,count(*) as total_rows from EVENTS_FACT UNION ALL
select 'LISTINGS_FACT' as table_name,count(*) as total_rows from LISTINGS_FACT UNION ALL
select 'SALES_FACT' as table_name,count(*) as total_rows from SALES_FACT UNION ALL
select 'USERS_DIM' as table_name,count(*) as total_rows from USERS_DIM UNION ALL
select 'VENUE_DIM' as table_name,count(*) as total_rows from VENUE_DIM;


