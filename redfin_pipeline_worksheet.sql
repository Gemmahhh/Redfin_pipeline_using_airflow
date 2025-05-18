-- First, let us create the database 
CREATE DATABASE redfin_database;

-- Now, its time to create the schema 
CREATE SCHEMA redfin_schema;

DROP TABLE redfin_table
-- Its now time to create the table 
CREATE OR REPLACE TABLE redfin_database.redfin_schema.redfin_table (
    PERIOD_BEGIN DATE,
    PERIOD_END DATE,
    CITY VARCHAR(255),
    STATE VARCHAR(50),
    REGION VARCHAR(255),
    PROPERTY_TYPE VARCHAR(100),
    IS_SEASONALLY_ADJUSTED BOOLEAN,
    MEDIAN_SALE_PRICE FLOAT,
    MEDIAN_SALE_PRICE_MOM FLOAT,
    MEDIAN_SALE_PRICE_YOY FLOAT,
    MEDIAN_LIST_PRICE FLOAT,
    MEDIAN_PPSF FLOAT,
    HOMES_SOLD INTEGER,
    HOMES_SOLD_MOM FLOAT,
    HOMES_SOLD_YOY FLOAT,
    NEW_LISTINGS FLOAT,
    INVENTORY FLOAT,
    MONTHS_OF_SUPPLY FLOAT,
    MEDIAN_DOM FLOAT,
    AVG_SALE_TO_LIST FLOAT,
    SOLD_ABOVE_LIST FLOAT
);

-- lets view the empty table 
select * from redfin_table

-- Now, its time to specify the format of the file and this is done by creating a file format 
-- this file format is to be used when inserting the data from s3 to snowflake 
-- the file format helps snowflake know the format of the data that is being ingested. 
-- I'll be creating a schema for this file format just to keep things organized. 
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format redfin_database.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1


ALTER FILE FORMAT redfin_database.file_format_schema.format_csv
SET FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = '\\';
    

-- we need an external stage to be able to point snowflake to our aws s3. 
-- lets create a schema for this to so we can keep everything organized
CREATE SCHEMA external_stage_schema

CREATE OR REPLACE STAGE redfin_database.external_stage_schema.redfin_ext_stage_yml
    url = "s3://redfin-pipeline-transformed-bucket/"
    -- during production, the access and scret keys would be stored on aws and not hardcoded this way
   credentials = (aws_key_id= '********'
    aws_secret_key='************')
    FILE_FORMAT= redfin_database.file_format_schema.format_csv

-- now I want to list out all of my stages
list @redfin_database.external_stage_schema.redfin_ext_stage_yml


-- Its finally time to create snowpipe
-- Snowpipe is used to enable loading data from files as soon as they are available in a stage
-- This helps us not to run the COPY COMMAND manually
-- Now, lets crate a schema for snowpipe
CREATE OR REPLACE SCHEMA redfin_database.snowpipe_schema

CREATE OR REPLACE PIPE redfin_database.snowpipe_schema.redfin_snowpipe
    auto_ingest = True
    AS 
    COPY INTO redfin_database.redfin_schema.redfin_table
    FROM @redfin_database.external_stage_schema.redfin_ext_stage_yml

-- I want to get the notification_channel
DESC PIPE redfin_database.snowpipe_schema.redfin_snowpipe

-- debugging the issue with ingesting the data from s3 bucket. 
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
FROM @redfin_database.external_stage_schema.redfin_ext_stage_yml/redfin_data_10052025191914.csv
(FILE_FORMAT => redfin_database.file_format_schema.format_csv);



-- I realized that one of the field was enclosed with "" and this was causing an issue with data ingestion. 
-- So I updated the file format to be able to accomodate situations like this. 
-- After doing that, I reran the entire pipeline and the data got ingestd successfully into the snowflake table 

-- Here is a sample of the ingested data. 
select * from redfin_database.redfin_schema.redfin_table limit 20