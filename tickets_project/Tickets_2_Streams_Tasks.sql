-- Create STREAM TASK. 
-- Once there is new data trigger snowpipe, snowpipe will load data into stage table, 
-- stream task will periodically merge data from stage table to the target table.
-- * * * * * UTC Every minute. UTC time zone.
-- 0 2 * * * UTC Every night at 2 AM. UTC time zone.
-- 0 5,17 * * * UTC Twice daily, at 5 AM and 5 PM (at the top of the hour). UTC time zone.

use database tickets_db;

---************************************************
--- Creating Named Internal Stage 
---************************************************
use schema sales_stg;

CREATE or REPLACE STAGE tickets_db.sales_stg.tickets_stage;

list @tickets_stage;

---**************************************************
--- Creating TASK for internal Stage to Stage Tables
--- it will copy into stage tables from internal stage
---**************************************************
CREATE OR REPLACE TASK tickets_ingest_tasks
  warehouse =compute_wh
  SCHEDULE = '1 minute'
  AS
  EXECUTE IMMEDIATE
  $$ 
  BEGIN
    copy into events_stg  from @tickets_stage/events/ 
        on_error = skip_file                           
        pattern = '.*\.csv\.gz$'                       			  
        FILE_FORMAT = (type = csv field_delimiter = '|'
        skip_header = 1                                
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042'           
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')                     
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
        FORCE = TRUE;                                  
    copy into venue_stg  from @tickets_stage/venue/    
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '|' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
    copy into USERS_STG  from @tickets_stage/users/
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '|' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
    copy into LISTINGS_STG  from @tickets_stage/listings/
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '|' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
    copy into DATE_STG  from @tickets_stage/date/
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '|' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
    copy into CATEGORY_STG  from @tickets_stage/category/
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '|' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
    copy into SALES_STG  from @tickets_stage/sales/
        on_error = continue
        pattern = '.*\.csv\.gz$'
        FILE_FORMAT = (type = csv field_delimiter = '\t' 
        skip_header = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        EMPTY_FIELD_AS_NULL = TRUE 
        NULL_IF = ('NULL','null','')
        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
        )
        FORCE = TRUE;
  END;
  $$;                        




---********************************************************
--- Creating Streams for every Stage table to track changes
---*********************************************************

use schema sales;
-- upserts using merge in category table
 -- Create STREAM on venue_stg 
CREATE OR REPLACE STREAM venue_stg_stream ON TABLE tickets_db.sales_stg.venue_stg;



---********************************************************
--- Creating TASK for Venue Dim process
---*********************************************************

CREATE OR REPLACE TASK venue_dim_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('venue_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.venue_dim as tgt using tickets_db.sales_stg.venue_stg as src on tgt.venueid = src.venueid
when matched then
update
set
    tgt.venue_key = sha2(src.venueid,256),
    tgt.VENUENAME = src.VENUENAME,
    tgt.VENUECITY = src.VENUECITY,
    tgt.VENUESTATE = src.VENUESTATE,
    tgt.VENUESEATS = src.VENUESEATS,
    tgt.updated_date = current_date(),
    tgt.updated_by = 'etljob'
    when not matched then
insert
    (
        venue_key,
        venueid,
        VENUENAME,
        VENUECITY,
        VENUESTATE,
        VENUESEATS,
        created_date,
        created_by
    )
values(
        sha2(src.venueid,256),
        src.venueid,
        src.VENUENAME,
        src.VENUECITY,
        src.VENUESTATE,
        src.VENUESEATS,
        current_date(),
        'etljob'
    );
 
---********************************************************
--- Creating Streams for category_stg
---*********************************************************

CREATE OR REPLACE STREAM category_stg_stream ON TABLE tickets_db.sales_stg.category_stg;

---********************************************************
--- Creating TASK for Category Dim process
---*********************************************************
CREATE OR REPLACE TASK category_dim_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('category_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.category_dim as tgt using tickets_db.sales_stg.category_stg as src on tgt.catid = src.catid
when matched then
update
set
    tgt.category_key = sha2(src.catid,256),
    tgt.catgroup = src.catgroup,
    tgt.catname = src.catname,
    tgt.catdesc = src.catdesc,
    tgt.updated_date = current_date(),
    tgt.updated_by = 'etljob'
    when not matched then
insert
    (
        category_key,
        catid,
        catgroup,
        catname,
        catdesc,
        created_date,
        created_by
    )
values(
        sha2(src.catid,256),
        src.catid,
        src.catgroup,
        src.catname,
        src.catdesc,
        current_date(),
        'etljob'
    );
 
---********************************************************
--- Creating Streams for date_stg
---*********************************************************  
CREATE OR REPLACE STREAM date_stg_stream ON TABLE tickets_db.sales_stg.date_stg;

---********************************************************
--- Creating TASK for Date Dim process
---*********************************************************
CREATE OR REPLACE TASK date_dim_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('date_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.date_dim as tgt using tickets_db.sales_stg.date_stg as src on tgt.dateid = src.dateid
    when matched then
update
set
    tgt.date_key = sha2(src.dateid,256),
    tgt.dateid = src.dateid,
    tgt.caldate = src.caldate,
    tgt.day = src.day,
    tgt.week = src.week,
    tgt.month = src.month,
    tgt.qtr = src.qtr,
    tgt.year = src.year,
    tgt.holiday = src.holiday,
    tgt.updated_by = 'etljob',
    tgt.updated_date = current_date()
    when not matched then
insert
    (
        date_key,
        dateid,
        caldate,
        day,
        week,
        month,
        qtr,
        year,
        holiday,
        created_date,
        created_by
    )
values
    (
        sha2(src.dateid, 256),
        src.dateid,
        src.caldate,
        src.day,
        src.week,
        src.month,
        src.qtr,
        src.year,
        src.holiday,
        current_date,
        'etljob'
    );

---********************************************************
--- Creating Streams for users_stg
---*********************************************************  
CREATE OR REPLACE STREAM users_stg_stream ON TABLE tickets_db.sales_stg.users_stg;

---********************************************************
--- Creating TASK for Users Dim process
---*********************************************************
CREATE OR REPLACE TASK users_dim_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('users_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.users_dim as tgt using tickets_db.sales_stg.users_stg as src on tgt.userid = src.userid
    when matched then
update
set
    tgt.user_key = sha2(src.userid,256),
    tgt.userid = src.userid,
    tgt.username = src.username,
    tgt.FIRSTNAME = src.FIRSTNAME,
    tgt.LASTNAME = src.LASTNAME,
    tgt.CITY = src.CITY,
    tgt.STATE = src.STATE,
    tgt.EMAIL = src.EMAIL,
    tgt.PHONE = src.PHONE,
    tgt.LIKESPORTS = src.LIKESPORTS,
    tgt.LIKETHEATRE = src.LIKETHEATRE,
    tgt.LIKECONCERTS = src.LIKECONCERTS,
    tgt.LIKEJAZZ = src.LIKEJAZZ,
    tgt.LIKECLASSICAL = src.LIKECLASSICAL,
    tgt.LIKEOPERA = src.LIKEOPERA,
    tgt.LIKEROCK = src.LIKEROCK,
    tgt.LIKEVEGAS = src.LIKEVEGAS,
    tgt.LIKEBROADWAY = src.LIKEBROADWAY,
    tgt.LIKEMUSICALS = src.LIKEMUSICALS,
    tgt.updated_by = 'etljob',
    tgt.updated_date = current_date()
    when not matched then
insert
    (
        user_key,
        userid,
        username,
        FIRSTNAME,
        LASTNAME,
        CITY,
        STATE,
        EMAIL,
        PHONE,
        LIKESPORTS,
        LIKETHEATRE,
        LIKECONCERTS,
        LIKEJAZZ,
        LIKECLASSICAL,
        LIKEOPERA,
        LIKEROCK,
        LIKEVEGAS,
        LIKEBROADWAY,
        LIKEMUSICALS,
        created_date,
        created_by
    )
values
    (
        sha2(src.userid, 256),
        src.userid,
        src.username,
        src.FIRSTNAME,
        src.LASTNAME,
        src.CITY,
        src.STATE,
        src.EMAIL,
        src.PHONE,
        src.LIKESPORTS,
        src.LIKETHEATRE,
        src.LIKECONCERTS,
        src.LIKEJAZZ,
        src.LIKECLASSICAL,
        src.LIKEOPERA,
        src.LIKEROCK,
        src.LIKEVEGAS,
        src.LIKEBROADWAY,
        src.LIKEMUSICALS,
        current_date,
        'etljob'
    );
 
---********************************************************
--- Creating Streams for listings_stg
---*********************************************************  
CREATE OR REPLACE STREAM listings_stg_stream ON TABLE tickets_db.sales_stg.listings_stg;

---********************************************************
--- Creating TASK for Listings Dim process
---*********************************************************
CREATE OR REPLACE TASK listings_fact_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('listings_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.LISTINGS_FACT as tgt using (
        select
            stg.LISTID,
            u.user_key as SELLER_KEY,
            e.EVENT_KEY,
            d.DATE_KEY,
            stg.NUMTICKETS,
            stg.PRICEPERTICKET,
            stg.TOTALPRICE,
            stg.LISTTIME
        from
            tickets_db.sales_stg.LISTINGS_STG stg
            left join tickets_db.sales.users_dim as u on stg.sellerid = u.userid
            left join tickets_db.sales.events_fact as e on stg.eventid = e.eventid
            left join tickets_db.sales.date_dim as d on stg.dateid = d.dateid
    ) as src on tgt.LISTID = src.LISTID
    when matched then
update
set
    tgt.LIST_KEY = sha2(src.LISTID,256),
    tgt.EVENT_KEY = src.EVENT_KEY,
    tgt.SELLER_KEY = src.SELLER_KEY,
    tgt.DATE_KEY = src.DATE_KEY,
    tgt.NUMTICKETS = src.NUMTICKETS,
    tgt.PRICEPERTICKET = src.PRICEPERTICKET,
    tgt.TOTALPRICE = src.TOTALPRICE,
    tgt.LISTTIME = src.LISTTIME,
    tgt.updated_date = current_date(),
    tgt.updated_by = 'etljob'
    when not matched then
insert
    (
        LIST_KEY,
        EVENT_KEY,
        SELLER_KEY,
        DATE_KEY,
        LISTID,
        NUMTICKETS,
        PRICEPERTICKET,
        TOTALPRICE,
        LISTTIME,
        created_date,
        created_by
    )
values(
        sha2(src.LISTID,256),
        EVENT_KEY,
        SELLER_KEY,
        DATE_KEY,
        src.LISTID,
        src.NUMTICKETS,
        src.PRICEPERTICKET,
        src.TOTALPRICE,
        src.LISTTIME,
        current_date(),
        'etljob'
    );
    
    
    
---********************************************************
--- Creating Streams for events_stg
---*********************************************************  
CREATE OR REPLACE STREAM events_stg_stream ON TABLE tickets_db.sales_stg.events_stg;

---********************************************************
--- Creating TASK for Events fact process
---*********************************************************
CREATE OR REPLACE TASK events_fact_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('events_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.EVENTS_FACT as tgt using (
        select
            stg.EVENTID,
            v.VENUE_KEY,
            c.CATEGORY_KEY,
            d.DATE_KEY,
            stg.EVENTNAME,
            stg.STARTTIME
        from
            tickets_db.sales_stg.EVENTS_STG stg
            left join tickets_db.sales.venue_dim as v on stg.venueid = v.venueid
            left join tickets_db.sales.category_dim as c on stg.catid = c.catid
            left join tickets_db.sales.date_dim as d on stg.dateid = d.dateid
    ) as src on tgt.EVENTID = src.EVENTID
    when matched then
update
set
    tgt.EVENT_KEY = sha2(src.EVENTID,256),
    tgt.VENUE_KEY = src.VENUE_KEY,
    tgt.CATEGORY_KEY = src.CATEGORY_KEY,
    tgt.DATE_KEY = src.DATE_KEY,
    tgt.EVENTNAME = src.EVENTNAME,
    tgt.STARTTIME = src.STARTTIME,
    tgt.updated_date = current_date(),
    tgt.updated_by = 'etljob'
    when not matched then
insert
    (
        EVENT_KEY,
        VENUE_KEY,
        CATEGORY_KEY,
        DATE_KEY,
        EVENTID,
        EVENTNAME,
        STARTTIME,
        created_date,
        created_by
    )
values(
        sha2(src.EVENTID,256),
        VENUE_KEY,
        CATEGORY_KEY,
        DATE_KEY,
        src.EVENTID,
        src.EVENTNAME,
        src.STARTTIME,
        current_date(),
        'etljob'
    );
 
---********************************************************
--- Creating Streams for sales_stg
---*********************************************************    
CREATE OR REPLACE STREAM sales_stg_stream ON TABLE tickets_db.sales_stg.sales_stg;

---********************************************************
--- Creating TASK for Sales fact process
---*********************************************************

CREATE OR REPLACE TASK sales_fact_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 22 * * * Asia/Kolkata'
WHEN SYSTEM$STREAM_HAS_DATA('sales_stg_stream') -- Merage into target table by stream 
AS
merge into tickets_db.sales.SALES_FACT as tgt using (
        select
            stg.SALESID,
            l.LIST_KEY,
            s.user_key as SELLER_KEY,
            b.user_key as BUYER_KEY,
            e.EVENT_KEY,
            d.DATE_KEY,
            stg.QTYSOLD,
            stg.PRICEPAID,
            stg.COMMISSION,
            stg.SALETIME
        from
            tickets_db.sales_stg.SALES_STG stg
            left join tickets_db.sales.listings_fact as l on stg.listid = l.listid
            left join tickets_db.sales.events_fact as e on stg.eventid = e.eventid
            left join tickets_db.sales.users_dim as s on stg.sellerid = s.userid
            left join tickets_db.sales.users_dim as b on stg.buyerid = b.userid
            left join tickets_db.sales.date_dim as d on stg.dateid = d.dateid
    ) as src on tgt.SALESID = src.SALESID
    when matched then
update
set
    tgt.SALES_KEY = sha2(src.SALESID,256),
    tgt.LIST_KEY = src.LIST_KEY,
    tgt.SELLER_KEY = src.SELLER_KEY,
    tgt.BUYER_KEY = src.BUYER_KEY,
    tgt.EVENT_KEY = src.EVENT_KEY,
    tgt.DATE_KEY = src.DATE_KEY,
    tgt.QTYSOLD = src.QTYSOLD,
    tgt.PRICEPAID = src.PRICEPAID,
    tgt.COMMISSION = src.COMMISSION,
    tgt.SALETIME = src.SALETIME,
    tgt.updated_date = current_date(),
    tgt.updated_by = 'etljob'
    when not matched then
insert
    (
        SALES_KEY,
        LIST_KEY,
        SELLER_KEY,
        BUYER_KEY,
        EVENT_KEY,
        DATE_KEY,
        SALESID,
        QTYSOLD,
        PRICEPAID,
        COMMISSION,
        SALETIME,
        created_date,
        created_by
    )
values(
        sha2(src.SALESID,256),
        LIST_KEY,
        SELLER_KEY,
        BUYER_KEY,
        EVENT_KEY,
        DATE_KEY,
        src.SALESID,
        src.QTYSOLD,
        src.PRICEPAID,
        src.COMMISSION,
        src.SALETIME,
        current_date(),
        'etljob'
    );
 