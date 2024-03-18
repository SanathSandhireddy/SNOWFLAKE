--********************************************************************************
--Cron Jab schedule synax
--********************************************************************************
//# __________ minute (0-59)
//# | ________ hour (0-23)
//# | | ______ day of month (1-31, or L)
//# | | | ____ month (1-12, JAN-DEC)
//# | | | | _ day of week (0-6, SUN-SAT, or L)
//# | | | | |
//# | | | | |
//  * * * * *

--********************************************************************************
--Creating Notification integration for alerts
--********************************************************************************
 
--- Creating notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_int
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('call2raveendra@gmail.com')
COMMENT = 'Snowflake Users Created by Accountadmin or Super Role';

--********************************************************************************
-- Testing Email Notification just calling system$send_email
--********************************************************************************

-- calling email notification alert
call system$send_email(
    'my_email_int',
    'call2raveendra@gmail.com',
    'Email Alert: Task A has finished.',
    'Task A has successfully finished.\nStart Time: 10:10:32\nEnd Time: 12:15:45\nTotal Records Processed: 115678'
);


SELECT
		Warehouse_name,
		SUM(CREDITS_USED) AS credits
		FROM snowflake.account_usage.warehouse_metering_history
		// aggregate warehouse Credit_used for the past 24 hours
	WHERE datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24
	GROUP BY 1
	--HAVING credits > 10
	ORDER BY 2 DESC;

    
--********************************************************************************
--Creating Alert for Warehouse monitoring
--********************************************************************************
 
--- while using timezone use Asia/Kolkata instead of IST. 
CREATE OR REPLACE ALERT Warehouse_Credit_Usage_Alert
	WAREHOUSE = compute_wh
	SCHEDULE = 'USING CRON 0 7 * * * Asia/Kolkata' // everyday at 7 am
	IF (EXISTS (SELECT
		Warehouse_name,
		SUM(CREDITS_USED) AS credits
		FROM snowflake.account_usage.warehouse_metering_history
		// aggregate warehouse Credit_used for the past 24 hours
	WHERE datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24
	GROUP BY 1
	HAVING credits > 10
	ORDER BY 2 DESC))
	THEN call system$send_email (
		'my_email_int', 
        'call2raveendra@gmail.com',
        'Email Alert: Excessive warehouse usage!',
        'Warehouse usage exceeds 10 credits in the past 24 hours'
);

alter alert Warehouse_Credit_Usage_Alert suspend;
alter alert Warehouse_Credit_Usage_Alert resume;


create table customers(id int,name varchar(100),updated_time TIMESTAMP);
insert into customers values(1,'ravi',current_timestamp()),(2,'sindhu',current_timestamp()),(3,'reshwanth',current_timestamp());


       
--********************************************************************************
-- Creating Alert for monitoring table DML Operations
--********************************************************************************
create or replace alert alert_new_rows
  warehouse = compute_wh
  schedule = '1 MINUTE'
  if (exists (
      select *
      from customers
      where updated_time between snowflake.alert.last_successful_scheduled_time()
       and snowflake.alert.scheduled_time()
  ))
  then call system$send_email (
		'my_email_int', 
        'call2raveendra@gmail.com',
        'Email Alert: Customers Tables Upsert',
        'New Rows has been inserted in customers table'
);

show alerts;

alter alert alert_new_rows suspend;
alter alert alert_new_rows resume;



--Retrieve records for the 10 most recent completed alert runs

select name, condition, condition_query_id, action, action_query_id, state
from snowflake.account_usage.alert_history
limit 10;

--Retrieve records for alert runs completed in the past hour
select name, condition, condition_query_id, action, action_query_id, state
from snowflake.account_usage.alert_history
where completed_time > dateadd(hours, -1, current_timestamp());


-- show alerts
show alerts;

--- drop alert
drop alert WAREHOUSE_CREDIT_USAGE_ALERT;
drop alert ALERT_NEW_ROWS;

 
create or replace stage my_s3_stage
  url = 's3://snowflakebucket-copyoption/'
  file_format = (type=csv,field_delimiter=',',skip_header=1);

list @my_s3_stage;

create or replace stage my_s3_stage1
  url = 's3://bucketsnowflakes3/'
  file_format = (type=csv,field_delimiter=',',skip_header=1);

list @my_s3_stage1;

create or replace stage my_s3_stage2
  url = 's3://data-snowflake-fundamentals/'
  file_format = (type=csv,field_delimiter=',',skip_header=1);

  
  
list @my_s3_stage2;

drop stage my_s3_stage2;
 

