-- creating streams

-- create source table
create or replace table customer_src(id int,name varchar(100));
-- create target table
create or replace table customer_tgt(id int,name varchar(100));

-- insert data into target table
insert into customer_tgt values(1,'ravi'),(2,'ram'),(3,'vikranth'),(4,'sridhar');

select * from customer_src;

select * from customer_tgt;
-- creating stream on source table
create or replace stream stream_customer_src on table customer_src;
-- verify streaming table
select * from stream_customer_src;
-- insert data into source table to verify on streaming
insert into customer_src values(8,'prasad'),(9,'mahesh'),(10,'srinu');
--- now verify streaming table
select * from stream_customer_src;
-- using merge we can implement streaming
merge into customer_tgt as t
using (select * from stream_customer_src) as s
on t.id = s.id
when matched and s.metadata$action='INSERT' and s.metadata$ISUPDATE THEN
UPDATE SET t.id = s.id,t.name=s.name
WHEN   matched and s.metadata$action='DELETE' THEN 
DELETE
WHEN NOT matched and s.metadata$action='INSERT' THEN 
insert (id,name) values(s.id,s.name);
-- verify target table after merge
select * from customer_tgt;
-- insert again on source table
insert into customer_src values(5,'raj'),(6,'reshwanth'),(7,'sindhu');
-- verify streaming table after insert update is happened?
select * from stream_customer_src;

-- delete from customer
delete from customer_src where id =5;

select * from customer_tgt;
