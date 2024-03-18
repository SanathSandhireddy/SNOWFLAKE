-- Create Warehouse for tasks
CREATE OR REPLACE WAREHOUSE jobs_wh;
use warehouse jobs_wh;
--- Create Database
CREATE OR REPLACE DATABASE jobs_db;
use database jobs_db;
-- Create Schema
CREATE OR REPLACE SCHEMA jobs_schema;
use schema jobs_schema;
-- Create Sequence 
create or replace sequence seq1 start = 1 increment = 1;
CREATE or REPLACE TABLE logs(id number,updated_time timestamp_ntz);

-- Create task
CREATE OR REPLACE TASK insert_logs_task
    WAREHOUSE = jobs_wh
    SCHEDULE = '1 MINUTE'
    AS 
    INSERT INTO logs(id,updated_time) VALUES(seq1.nextval,CURRENT_TIMESTAMP);

-- show tasks
SHOW TASKS;

-- Task starting and suspending
ALTER TASK insert_logs_task RESUME;

--ALTER TASK insert_logs_task SUSPEND;

-- show tasks
SHOW TASKS;


select * from logs;



-- USING CRON
-- Start with 'USING CRON', then the normal CRON statement, and finally, specifying the time zone.

CREATE OR REPLACE TASK insert_logs_task
    WAREHOUSE = jobs_wh
    SCHEDULE = 'USING CRON */1 * * * * Asia/Kolkata'
    AS 
    INSERT INTO logs(id,updated_time) VALUES(seq1.nextval,CURRENT_TIMESTAMP);
    

-- __________ minute (0-59)
-- | ________ hour (0-23)
-- | | ______ day of month (1-31, or L)
-- | | | ____ month (1-12, JAN-DEC)
-- | | | | __ day of week (0-6, SUN-SAT, or L)


--- ****************
-- task history
--- *****************

SHOW TASKS;
-- Use the table function "TASK_HISTORY()"
select *
  from table( )
  order by scheduled_time desc;
  
-- See results for a specific Task in a given time
select *
from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-4,current_timestamp()),
    result_limit => 5,
    task_name=>'insert_logs_task'));
 
-- See results for a given time period
-- Cannot retrieve data from more than 7 days ago
--TO_TIMESTAMP_LTZ (timestamp with local time zone)
--TO_TIMESTAMP_NTZ (timestamp with no time zone)
--TO_TIMESTAMP_TZ (timestamp with time zone)
select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>to_timestamp_ltz('2023-02-09 01:28:32.776 -0530'),
    scheduled_time_range_end=>to_timestamp_ltz('2023-02-15 11:35:32.776 -0530')));  
  
SELECT TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP)  




--- ****************
-- tree of tasks
--- ****************
 
SHOW TASKS;

SELECT * FROM logs;

-- Prepare a second table
CREATE OR REPLACE TABLE logs2 (
    it INT,
    CREATE_DATE DATE)
    
    
-- Suspend parent task
ALTER TASK insert_logs_task SUSPEND;
    
-- Create a child task
CREATE OR REPLACE TASK insert_logs_task2
    WAREHOUSE = jobs_wh
    AFTER insert_logs_task
    AS 
    INSERT INTO logs2 SELECT * FROM logs;
    
    
-- Prepare a third table
CREATE OR REPLACE TABLE logs3 (
    id INT,
    CREATE_DATE DATE,
    INSERT_DATE DATE DEFAULT DATE(current_date));
    

-- Create a child task
CREATE OR REPLACE TASK insert_logs_task3
    WAREHOUSE = jobs_wh
    AFTER insert_logs_task2
    AS 
    INSERT INTO logs3 (id,CREATE_DATE) SELECT * FROM logs2;


SHOW TASKS;



ALTER TASK insert_logs_task 
SET SCHEDULE = '1 MINUTE';

-- Resume tasks (first root task)
-- root task should be suspended before resuming child tasks
ALTER TASK insert_logs_task SUSPEND;
ALTER TASK insert_logs_task2 RESUME;
ALTER TASK insert_logs_task3 RESUME;
ALTER TASK insert_logs_task RESUME;


-- suspend tasks (first root task)
ALTER TASK insert_logs_task SUSPEND;
ALTER TASK insert_logs_task2 SUSPEND;
ALTER TASK insert_logs_task3 SUSPEND;



select * from logs;

select * from logs2;

select * from logs3;


    