create or replace  database snow_project;
use snow_project;
create or replace schema snow_project.file_format;
create or replace schema snow_project.pipe_schema;
create or replace schema snow_project.pipe_table_schema;
create  or replace schema snow_project.pro_stages;
create or replace table snow_project.pipe_table_schema.pipe_table (row_column variant);
create or replace file format snow_project.file_format.json_format 
type=json;

CREATE OR REPLACE STAGE snow_project.pro_stages.my_prostage
URL='s3://krishregex'
CREDENTIALS = (AWS_KEY_ID = ''
      AWS_SECRET_KEY = ''
  );

describe stage snow_project.pro_stages.my_prostage;
list @snow_project.pro_stages.my_prostage;





CREATE OR REPLACE TABLE snow_project.pipe_table_schema.pipe_table (
    row_column VARIANT,
    file_name STRING,
    load_time TIMESTAMP
);


CREATE OR REPLACE PIPE snow_project.pipe_schema.pro_pipe
AUTO_INGEST = TRUE
AS
COPY INTO snow_project.pipe_table_schema.pipe_table
(row_column, file_name, load_time)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED
    FROM @snow_project.pro_stages.my_prostage
)
FILE_FORMAT = snow_project.file_format.json_format;


select * from snow_project.pipe_table_schema.pipe_table;



desc pipe snow_project.pipe_schema.pro_pipe;



select * from snow_project.pipe_table_schema.pipe_table;


describe table snow_project.pipe_table_schema.pipe_table;
select
  row_column:order_id::int      as order_id,
  row_column:order_date::date   as order_date,
  row_column:amount::number     as amount
from snow_project.pipe_table_schema.pipe_table;


CREATE OR REPLACE TABLE silver_table AS
SELECT
    -- Cast the timestamp string
    
    TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string) AS event_ts,

    -- Derived calendar columns
    YEAR(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))     AS year,
    MONTH(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))    AS month,
    QUARTER(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS quarter,
    DAYNAME(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS week_day_name
FROM snow_project.pipe_table_schema.pipe_table;


select * from silver_table;



-- task reat

CREATE OR REPLACE TABLE silver_table (
    event_ts TIMESTAMP,
    year INT,
    month INT,
    quarter INT,
    week_day_name STRING
);

INSERT INTO silver_table
SELECT
    TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string) AS event_ts,
    YEAR(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))     AS year,
    MONTH(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))    AS month,
    QUARTER(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS quarter,
    DAYNAME(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS week_day_name
FROM snow_project.pipe_table_schema.pipe_table;


--- task CREATE OR REPLACE TASK silver_task
CREATE OR REPLACE TASK silver_task
WAREHOUSE = compute_wh
SCHEDULE = '1 minute'  -- runs every minute
AS
INSERT INTO silver_table
SELECT
    TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string) AS event_ts,
    YEAR(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))     AS year,
    MONTH(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))    AS month,
    QUARTER(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS quarter,
    DAYNAME(TO_TIMESTAMP(ROW_COLUMN:LOAD_TIMESTAMP::string))  AS week_day_name
FROM snow_project.pipe_table_schema.pipe_table;

ALTER TASK silver_task RESUME;

ALTER TASK silver_task SUSPEND;

select * from silver_table;

ALTER PIPE snow_project.pipe_schema.pro_pipe REFRESH;

LIST @snow_project.pro_stages.my_prostage;


SELECT COUNT(*) 
FROM snow_project.pipe_table_schema.pipe_table;

SELECT * 
FROM snow_project.pipe_table_schema.pipe_table;


-- silver leval
drop table snow_project.pipe_table_schema.silver_table;

CREATE OR REPLACE TABLE snow_project.pipe_table_schema.silver_table (
    cid STRING,
    cname STRING,
    eid STRING,
    pno STRING,
    city STRING,
    state STRING,
    country STRING,
    CusSeg STRING,
    CusSta STRING,
    CreRat STRING,
    EffStaDT STRING,
    EffEndDt STRING,
    Iscurr STRING,
    SouSys STRING,
    LoDa STRING,
    LoTi STRING,
    ReHa STRING,
    year INT,
    month INT,
    quarter INT,
    week_day_name STRING,
    file_name STRING,
    load_time TIMESTAMP
);

drop stream snow_project.pipe_table_schema.pipe_stream;

CREATE OR REPLACE STREAM snow_project.pipe_table_schema.pipe_stream
ON TABLE snow_project.pipe_table_schema.pipe_table
APPEND_ONLY = TRUE;


CREATE OR REPLACE TASK snow_project.pipe_table_schema.silver_task
WAREHOUSE = compute_wh
SCHEDULE = '1 minute'
AS
INSERT INTO snow_project.pipe_table_schema.silver_table
SELECT
    row_column:CUSTOMER_ID::string,
    row_column:CUSTOMER_NAME::string,
    row_column:EMAIL_ID::string,
    row_column:PHONE_NUMBER::string,
    row_column:CITY::string,
    row_column:STATE::string,
    row_column:COUNTRY::string,
    row_column:CUSTOMER_SEGMENT::string,
    row_column:CUSTOMER_STATUS::string,
    row_column:CREDIT_RATING::string,
    row_column:EFFECTIVE_START_DT::string,
    row_column:EFFECTIVE_END_DT::string,
    row_column:IS_CURRENT::string,
    row_column:SOURCE_SYSTEM::string,
    row_column:LOAD_DATE::string,
    row_column:LOAD_TIMESTAMP::string,
    row_column:RECORD_HASH::string,
    YEAR(row_column:LOAD_TIMESTAMP::TIMESTAMP),
    MONTH(row_column:LOAD_TIMESTAMP::TIMESTAMP),
    QUARTER(row_column:LOAD_TIMESTAMP::TIMESTAMP),
    DAYNAME(row_column:LOAD_TIMESTAMP::TIMESTAMP),
    file_name,
    load_time
FROM snow_project.pipe_table_schema.pipe_stream;

ALTER TASK snow_project.pipe_table_schema.silver_task RESUME;

SHOW TASKS LIKE '%silver_task%';




select * from snow_project.pipe_table_schema.silver_table;

SELECT * 
FROM snow_project.pipe_table_schema.pipe_stream;

select * from snow_project.pipe_table_schema.pipe_stream;



SELECT SYSTEM$PIPE_STATUS('snow_project.pipe_schema.pro_pipe');

SHOW TASKS;

SELECT COUNT(*) FROM snow_project.pipe_table_schema.pipe_table;


CREATE OR REPLACE TABLE snow_project.pipe_table_schema.dim_customer (
    cid STRING,
    cname STRING,
    eid STRING,
    pno STRING,
    city STRING,
    state STRING,
    country STRING,
    CusSeg STRING,
    CusSta STRING,
    CreRat STRING,

    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    is_current STRING,

    record_hash STRING
);

CREATE OR REPLACE TASK snow_project.pipe_table_schema.task_scd2_customer
WAREHOUSE = compute_wh
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('snow_project.pipe_table_schema.pipe_stream')
AS

MERGE INTO snow_project.pipe_table_schema.dim_customer tgt
USING (

    SELECT
        row_column:CUSTOMER_ID::string cid,
        row_column:CUSTOMER_NAME::string cname,
        row_column:EMAIL_ID::string eid,
        row_column:PHONE_NUMBER::string pno,
        row_column:CITY::string city,
        row_column:STATE::string state,
        row_column:COUNTRY::string country,
        row_column:CUSTOMER_SEGMENT::string CusSeg,
        row_column:CUSTOMER_STATUS::string CusSta,
        row_column:CREDIT_RATING::string CreRat,
        row_column:RECORD_HASH::string record_hash,
        CURRENT_TIMESTAMP() ts
    FROM snow_project.pipe_table_schema.pipe_stream

) src

ON tgt.cid = src.cid
AND tgt.is_current = 'Y'

WHEN MATCHED
AND tgt.record_hash <> src.record_hash
THEN UPDATE SET
    tgt.is_current = 'N',
    tgt.effective_end_dt = src.ts

WHEN NOT MATCHED THEN
INSERT (
    cid,cname,eid,pno,city,state,country,CusSeg,CusSta,CreRat,
    effective_start_dt,effective_end_dt,is_current,record_hash
)
VALUES (
    src.cid,src.cname,src.eid,src.pno,src.city,src.state,src.country,
    src.CusSeg,src.CusSta,src.CreRat,
    src.ts,NULL,'Y',src.record_hash
);

ALTER TASK snow_project.pipe_table_schema.task_scd2_customer RESUME;

SHOW TASKS LIKE '%scd2%';

SELECT cid,is_current,effective_start_dt,effective_end_dt
FROM snow_project.pipe_table_schema.dim_customer
ORDER BY cid,effective_start_dt;


SELECT SYSTEM$PIPE_STATUS('snow_project.pipe_schema.pro_pipe');


SELECT row_column, file_name
FROM snow_project.pipe_table_schema.pipe_table
LIMIT 5;

SELECT *
FROM snow_project.pipe_table_schema.pipe_stream;

SELECT *
FROM snow_project.pipe_table_schema.dim_customer
ORDER BY cid,effective_start_dt;
