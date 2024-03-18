



















--******************************************************************
-- Creating Database for TICKETS Data
-- Database Name : tickets_db
-- Target Schema Name : sales
-- Stage Schema Name : sales_stg
--******************************************************************


create or replace database  tickets_db;
use tickets_db;
create or replace schema sales_stg;
use schema sales_stg;

--******************************************************************
-- Creating  Venue Staging table : venue_stg   1
--******************************************************************

create or replace table tickets_db.sales_stg.venue_stg(
    venueid int not null,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate varchar(2),
    venueseats integer
);
--******************************************************************
-- Creating  Category Staging table : category_stg  2
--******************************************************************
create or replace table tickets_db.sales_stg.category_stg(
    catid int not null,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50)
);
--******************************************************************
-- Creating  User Staging table : users_stg   3
--******************************************************************

create or replace table tickets_db.sales_stg.users_stg(
    userid integer not null,
    username varchar(8),
    firstname varchar(30),
    lastname varchar(30),
    city varchar(30),
    state varchar(2),
    email varchar(100),
    phone varchar(14),
    likesports boolean,
    liketheatre boolean,
    likeconcerts boolean,
    likejazz boolean,
    likeclassical boolean,
    likeopera boolean,
    likerock boolean,
    likevegas boolean,
    likebroadway boolean,
    likemusicals boolean
);

--******************************************************************
-- Creating  Date Staging table : date_stg  4
--******************************************************************


create or replace table tickets_db.sales_stg.date_stg(
    dateid int not null,
    caldate date not null,
    day varchar(3) not null,
    week int not null,
    month varchar(5) not null,
    qtr varchar(5) not null,
    year int not null,
    holiday boolean
);
--******************************************************************
-- Creating  Events Staging table : events_stg  5
--******************************************************************

create or replace table tickets_db.sales_stg.events_stg(
    eventid int not null,
    venueid int not null,
    catid int not null,
    dateid int not null,
    eventname varchar(200),
    starttime timestamp
);
--******************************************************************
-- Creating  Listings Staging table : listings_stg
--******************************************************************

create or replace table tickets_db.sales_stg.listings_stg(
    listid int not null,
    sellerid int not null,
    eventid int not null,
    dateid smallint not null,
    numtickets int not null,
    priceperticket decimal(8, 2),
    totalprice decimal(8, 2),
    listtime timestamp
);
--******************************************************************
-- Creating  Sales Staging table : sales_stg
--******************************************************************

create or replace table tickets_db.sales_stg.sales_stg(
    salesid int not null,
    listid int not null,
    sellerid int not null,
    buyerid int not null,
    eventid int not null,
    dateid int not null,
    qtysold int not null,
    pricepaid decimal(8, 2),
    commission decimal(8, 2),
    saletime timestamp
);

--******************************************************************
--  Creating Target Schema
--  Target Schema Name : sales
--******************************************************************

create or replace schema   sales;
use schema sales;

--******************************************************************
--  Creating Venue Dimension Table : venue_dim
--******************************************************************

create or replace table tickets_db.sales.venue_dim(
    venue_key varchar(4000),
    venueid integer,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate varchar(2),
    venueseats integer,
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating category Dimension Table : category_dim
--******************************************************************

create or replace table tickets_db.sales.category_dim(
    category_key varchar(4000),
    catid int,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50),
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating Users Dimension Table : users_dim
--******************************************************************

create or replace table tickets_db.sales.users_dim(
    user_key varchar(4000),
    userid int,
    username varchar(8),
    firstname varchar(30),
    lastname varchar(30),
    city varchar(30),
    state varchar(2),
    email varchar(100),
    phone varchar(14),
    likesports boolean,
    liketheatre boolean,
    likeconcerts boolean,
    likejazz boolean,
    likeclassical boolean,
    likeopera boolean,
    likerock boolean,
    likevegas boolean,
    likebroadway boolean,
    likemusicals boolean,
    created_by varchar(50),
    created_date datetime,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating Date Dimension Table : date_dim
--******************************************************************

create or replace table tickets_db.sales.date_dim(
    date_key varchar(4000),
    dateid int,
    caldate date,
    day varchar(3),
    week int,
    month varchar(5),
    qtr varchar(5),
    year int,
    holiday boolean,
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating Events Fact Table : events_fact
--******************************************************************

create or replace table tickets_db.sales.events_fact(
    event_key varchar(4000),
    venue_key varchar(4000),
    category_key varchar(4000),
    date_key varchar(4000),
    eventid int,
    eventname varchar(200),
    starttime timestamp,
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating Listings Fact Table : listings_fact
--******************************************************************

create or replace table tickets_db.sales.listings_fact(
    list_key varchar(4000),
    listid int,
    seller_key varchar(4000),
    event_key varchar(4000),
    date_key varchar(4000),
    numtickets int,
    priceperticket decimal(8, 2),
    totalprice decimal(8, 2),
    listtime timestamp,
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

--******************************************************************
--  Creating Sales Fact Table : sales_fact
--******************************************************************

create or replace table tickets_db.sales.sales_fact(
    sales_key varchar(4000),
    list_key varchar(4000),
    seller_key varchar(4000),
    buyer_key varchar(4000),
    event_key varchar(4000),
    date_key varchar(4000),
    salesid int,
    qtysold integer,
    pricepaid decimal(8, 2),
    commission decimal(8, 2),
    saletime timestamp,
    created_by varchar(50),
    created_date timestamp,
    updated_by varchar(50),
    updated_date timestamp
);

