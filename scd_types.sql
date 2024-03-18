--********************************************************************************
-- Slowly Changing Dimension Types
-- Type 0 , Type 1 , Type 2 and Type 3 Examples
--********************************************************************************
-- create database 
create or replace database scd_db;
use scd_db;
create or replace schema scd_schema;
use schema scd_schema;
use warehouse compute_wh;

/****************
SCD TYPE 0
****************/
DROP TABLE IF EXISTS DEPT;
CREATE TABLE DEPT
(DEPTNO DECIMAL(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) );
/******************************
When we can choose Type 0
* if you are loading full data then use delete and insert or truncate and insert.
* one time loads
* every day if we get full data the we can choose truncate/delete and Insert
* append/inserting only
-- *****************************/
truncate table DEPT;
INSERT INTO DEPT 
select 10 as deptno, 'ACCOUNTING' as dname, 'NEW YORK' as loc
union all  
select 20, 'RESEARCH', 'DALLAS'
union all
select 30, 'SALES', 'CHICAGO'
union all 
select  40, 'OPERATIONS', 'BOSTON';

select * from dept;


/*************************
SCD TYPE 1  - Overwriting
**************************/
/*
In a Type 1 SCD the new data overwrites the existing data. The existing data is lost as it is not stored anywhere else. 
This is the default type of dimension you create. You do not need to specify any additional information to create a Type 1 SCD.
*/



create table customer_src(id int,name string,location string);
insert into customer_src values (1,'Ram','Mumbai'),(4,'Raj','Hyderabad'),(5,'Prasad','Pune');

select * from customer_src;

create table customer_tgt(id int,name string,location string);
insert into customer_tgt values (1,'Ram','Chennai'),(2,'Reshwanth','Hyderabad'),(3,'Vikranth','Bangalore');

select * from customer_tgt;

--insert into customer_tgt select * from customer_src;

merge into customer_tgt as t
using customer_src as s
on t.id = s.id
when matched then 
update set id=s.id,name=s.name,location=s.location
when not matched then 
insert (id,name,location) values(s.id,s.name,s.location);

select * from customer_tgt order by id;

/************************************************
SCD TYPE 2 - Managing Multiple times history 
************************************************/
 /* Merge statement to perform SCD Type 2

This merge statement simultaneously does both for each customer in the source table.
Inserts the new address with its current set to true, and
Updates the previous current row to set current to false, and update the endDate from null to the effectiveDate from the source.
*/

DROP TABLE IF EXISTS customers_scd2;
CREATE TABLE IF NOT EXISTS customers_scd2(cust_id int, name string,address string, status boolean, start_date TIMESTAMP_LTZ, end_date TIMESTAMP_LTZ);
insert into customers_scd2 VALUES (1,'Mahesh','Bangalore',1,current_timestamp(),'9999-12-31'),
(2,'ram','Hyderabad',1,current_timestamp(),'9999-12-31'),
(3,'ravi','Chennai',1,current_timestamp(),'9999-12-31'),
(4,'raj','Pune',1,current_timestamp(),'9999-12-31');

select * from customers_scd2;

DROP TABLE IF EXISTS customers_source;
CREATE TABLE IF NOT EXISTS customers_source(cust_id int,name string, address string, start_date TIMESTAMP_LTZ);
insert into customers_source VALUES (5,'Sridhar','Delhi',current_timestamp()),
(6,'Prasad','Mumbai',current_timestamp()),
(2,'ram','Bangalore',current_timestamp()),
(1,'Mahesh','Hyderabad',current_timestamp());


select * from customers_source;

SELECT updates.cust_id as mergeKey, updates.*
  FROM customers_source as updates
  union all
  SELECT NULL as mergeKey, updates.*
  FROM customers_source as updates JOIN customers_scd2 as customers
  ON updates.cust_id = customers.cust_id 
  WHERE customers.status = 1 AND updates.address <> customers.address;  

-- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
 -- These rows will INSERT new addresses of existing customers 
  -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTED.
MERGE INTO customers_scd2 as customers
USING (SELECT updates.cust_id as mergeKey, updates.*
  FROM customers_source as updates
  UNION ALL
  SELECT NULL as mergeKey, updates.*
  FROM customers_source as updates JOIN customers_scd2 as customers
  ON updates.cust_id = customers.cust_id 
  WHERE customers.status = 1 AND updates.address <> customers.address  
) staged_updates
ON customers.cust_id = mergeKey
WHEN MATCHED AND customers.status = 1 AND customers.address <> staged_updates.address THEN  
  UPDATE SET status = 0, end_date = staged_updates.start_date   
WHEN NOT MATCHED THEN 
  INSERT(cust_id, name,address, status, start_date, end_date) 
  VALUES(staged_updates.cust_id, staged_updates.name, staged_updates.address, 1, staged_updates.start_date, '9999-12-31');
  
-- Set status to true along with the new address and its effective date.
 -- Set status to false and endDate to source's effective date.  !=

 select * from customers_scd2;

/***********************************************
SCD TYPE 3 - Managing one time history 
***********************************************/
/*
Merge statement to perform SCD Type 3
This merge statement simultaneously update prev_loc and curr_loc columns.
its should update curr_loc to prev_loc if source loc and curr_loc is not matching.
*/

drop table if exists customer_scd3;
create table customer_scd3(id int,name string,curr_loc string,prev_loc string);
insert into customer_scd3(id,name,curr_loc,prev_loc) 
select 1,'Ravi','Bangalore',null 
union all 
select 2,'Ram','Chennai',null
union all 
select 3,'Prasad','Hyderabad',null;

select * from customer_scd3;


DROP TABLE IF EXISTS customer_src;
CREATE TABLE IF NOT EXISTS customer_src(id int, name string, loc string);
insert into customer_src VALUES (1,'Ravi','Chennai'),
(2,'Ram','Hyderabad'),
(4,'Mahesh','Bangalore'),
(5,'Sridhar','Hyderabad');

select * from customer_src;

-- Approach 1
--without joining with target table. 

merge into customer_scd3 as tgt
using customer_src as src
on tgt.id = src.id
WHEN MATCHED and lower(tgt.curr_loc)<>lower(src.loc) THEN
 update set tgt.prev_loc = tgt.curr_loc ,tgt.curr_loc =src.loc
WHEN NOT MATCHED THEN
insert   (id,name,curr_loc) values(src.id,src.name,src.loc);

select * from customer_scd3;
/*
case when expression/condition then result 
     when expression/condition then result 
     when expression/condition then result 
     when expression/condition then result 
     when expression/condition then result 
     when expression/condition then result 
*/
select src.id as id,
src.name as name,
src.loc as curr_loc,
case when src.loc <> tgt.curr_loc then tgt.curr_loc
else  tgt.prev_loc end as prev_loc
from customer_src as src 
left join customer_scd3 as tgt on src.id=tgt.id;

--approach 2
--joining with target table get the comparision

MERGE into customer_scd3 as tgt
using (select src.id as id,
src.name as name,
src.loc as curr_loc,
case when src.loc <> tgt.curr_loc then tgt.curr_loc
else  tgt.prev_loc end as prev_loc
from customer_src as src 
left join customer_scd3 as tgt on src.id=tgt.id) src
on tgt.id = src.id
when matched then 
update set tgt.id = src.id,tgt.name=src.name,tgt.curr_loc = src.curr_loc,tgt.prev_loc = src.prev_loc
when not matched then 
insert  (id,name,curr_loc,prev_loc) values  (src.id,src.name,src.curr_loc,src.prev_loc);



select * from customer_scd3;





