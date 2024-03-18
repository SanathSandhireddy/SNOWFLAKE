/*********************************************************
***  JSON Complex Data Types 
**********************************************************/

-- Waht is VARIANT Data Type?
-- A VARIANT can hold a value of any other data type, including an ARRAY or an OBJECT.

-- what is PARSE_JSON function and when its required?
-- VARIANT data type does not support string data. So, We need to convert string data into JSON Format using this funciton.

-- what is ARRAY Data type?
--ARRAY: similar to an array in other languages.
--ARRAY (can directly contain VARIANT, and thus indirectly contain any other data type, including itself).

-- what is OBJECT Data type?
-- OBJECT: similar to a JSON object, also called a “dictionary”, “hash”, or “map” in many languages. This contains key-value pairs.
--OBJECT (can directly contain VARIANT, and thus indirectly contain any other data type, including itself).

-- What is STRIP_OUTER_ARRAY option while loading JSON Data?
-- STRIP_OUTER_ARRAY, Removes the outer set of square brackets [ ] when loading the data into table.

-- what is ARRAY_CONSTRUCT?
-- ARRAY_CONSTRUCT Returns an array constructed from zero, one, or more inputs.
-- SELECT ARRAY_CONSTRUCT(10, 20, 30);
-- SELECT ARRAY_CONSTRUCT(null, 'hello', 3::double, 4, 5);

--  what is OBJECT_CONSTRUCT?
-- OBJECT_CONSTRUCT Returns an OBJECT constructed from the arguments.
-- SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null);


-- What is LATERAL?
-- In a FROM clause, the LATERAL keyword allows an inline view to reference columns from a table expression that precedes that inline view.


/*********************************************************
***  --using LATERAL 
**********************************************************/

-- A lateral join behaves more like a correlated subquery than like most JOINs.
CREATE TABLE departments (department_id INTEGER, name VARCHAR);
CREATE TABLE employees (employee_ID INTEGER, last_name VARCHAR, 
                        department_ID INTEGER, project_names ARRAY);


INSERT INTO departments (department_ID, name) VALUES 
    (1, 'Engineering'), 
    (2, 'Support');
INSERT INTO employees (employee_ID, last_name, department_ID) VALUES 
    (101, 'Richards', 1),
    (102, 'Paulson',  1),
    (103, 'Johnson',  2);


SELECT * 
    FROM departments AS d, LATERAL (SELECT * FROM employees AS e WHERE e.department_ID = d.department_ID) AS iv2
    ORDER BY employee_ID;


/*********************************************************
***  --using LATERAL with FLATTEN()
**********************************************************/



UPDATE employees SET project_names = ARRAY_CONSTRUCT('Materialized Views', 'UDFs') 
    WHERE employee_ID = 101;
UPDATE employees SET project_names = ARRAY_CONSTRUCT('Materialized Views', 'Lateral Joins')
    WHERE employee_ID = 102;

select * from employees;

-- converting into key-value pair data from columns and rows using object_construct
select object_construct(*) from employees;


SELECT emp.employee_ID, emp.last_name, index, value AS project_name
    FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names
    ORDER BY employee_ID;


/*********************************************************
***  PARSE_JSON Function usage
**********************************************************/

CREATE OR REPLACE TABLE car_sales
( 
  src variant
)
AS
SELECT PARSE_JSON(column1) AS src
FROM VALUES
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Valley View Auto Sales",
    "salesperson" : {
      "id": "55",
      "name": "Frank Beasley"
    },
    "customer" : [
      {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
    ],
    "vehicle" : [
      {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
    ]
}'),
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Tindel Toyota",
    "salesperson" : {
      "id": "274",
      "name": "Greg Northrup"
    },
    "customer" : [
      {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
    ],
    "vehicle" : [
      {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}  
    ]
}') v;


select * from car_sales;


--Extracting Values by Path Using the GET_PATH Function
SELECT GET_PATH(src, 'vehicle[0]:make') FROM car_sales;
-- Another way to extract data using hierarchy. both will return the same result,
SELECT src:vehicle[0].make FROM car_sales;


/*********************************************************
***  AWS S3 Stage + Process json data
**********************************************************/


create or replace table persons as
    select column1 as id, parse_json(column2) as c
 from values
   (12712555,
   '{ name:  { first: "John", last: "Smith"},
     contact: [
     { business:[
       { type: "phone", content:"555-1234" },
       { type: "email", content:"j.smith@company.com" } ] } ] }'),
   (98127771,
   '{ name:  { first: "Jane", last: "Doe"},
     contact: [
     { business:[
       { type: "phone", content:"555-1236" },
       { type: "email", content:"j.doe@company.com" } ] } ] }') v;

 -- Note the multiple instances of LATERAL FLATTEN in the FROM clause of the following query.
 -- Each LATERAL view is based on the previous one to refer to elements in
 -- multiple levels of arrays.

 select *  FROM persons;

 SELECT id as "ID",
   f.value AS "Contact",
   f1.value:type AS "Type",
   f1.value:content AS "Details"
 FROM persons p,
   lateral flatten(input => p.c, path => 'contact') f,
   lateral flatten(input => f.value:business) f1;




create or replace stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

create or replace table json_weather_data (v variant);

copy into json_weather_data
from @nyc_weather
file_format = (type=json);



create or replace view json_weather_data_view as
select
    v:time::timestamp as observation_time,
    v:city.id::int as city_id,
    v:city.name::string as city_name,
    v:city.country::string as country,
    v:city.coord.lat::float as city_lat,
    v:city.coord.lon::float as city_lon,
    v:clouds.all::int as clouds,
    (v:main.temp::float)-273.15 as temp_avg,
    (v:main.temp_min::float)-273.15 as temp_min,
    (v:main.temp_max::float)-273.15 as temp_max,
    v:weather[0].main::string as weather,
    v:weather[0].description::string as weather_desc,
    v:weather[0].icon::string as weather_icon,
    v:wind.deg::float as wind_dir,
    v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

select * from json_weather_data_view;




/**************************************
** unloading json into tablestage
****************************************/


-- Create a table
CREATE OR REPLACE TABLE mytable (
 id number(8) NOT NULL,
 first_name varchar(255) default NULL,
 last_name varchar(255) default NULL,
 city varchar(255),
 state varchar(255)
);
-- Populate the table with data
INSERT INTO mytable (id,first_name,last_name,city,state) 
 VALUES 
 (1,'Ryan','Dalton','Salt Lake City','UT'),
 (2,'Upton','Conway','Birmingham','AL'),
 (3,'Kibo','Horton','Columbus','GA');
-- Unload the data to a file in the staging location for the table
-- Note: You can unload data to any staging location
COPY INTO @%mytable
 FROM (SELECT OBJECT_CONSTRUCT('id', id, 'first_name', first_name, 'last_name', last_name, 'city', city, 'state', state) FROM mytable)
 FILE_FORMAT = (TYPE = JSON);
-- The COPY INTO location statement creates a file named data_0_0_0.json.gz in the staging location.
SELECT * FROM @%mytable;
-- The file contains the following data:

list @%mytable;

