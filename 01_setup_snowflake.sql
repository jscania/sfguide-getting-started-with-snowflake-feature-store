/*-----------------------------------------------------------------------------
Hands-On Lab: Advanced Guide to Snowflake Feature Store - Data Engineering with Snowpark
Script:       01_setup_snowflake.sql
Author:       Jacopo Maragna
Last Updated: 1/24/2025
Description: This SQL script is used to setup the required database objects including the source data for this use-case.
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Create the account level objects
-- ----------------------------------------------------------------------------
-- Roles
-- Use ULTRASONIC_ROLE for now but consider adding more access granularity in the future.
USE ROLE ULTRASONIC_ROLE;

-- Warehouse
CREATE OR REPLACE WAREHOUSE TPCXAI_SF0001_QUICKSTART_WH WAREHOUSE_SIZE = XSMALL;
GRANT OWNERSHIP ON WAREHOUSE TPCXAI_SF0001_QUICKSTART_WH TO ROLE ULTRASONIC_ROLE;

-- Database - BASE
-- It will store all our database artifacts and raw source data.
CREATE OR REPLACE DATABASE TPCXAI_SF0001_QUICKSTART;
GRANT OWNERSHIP ON DATABASE TPCXAI_SF0001_QUICKSTART TO ROLE ULTRASONIC_ROLE;  -- Ownership is the highest level of privilege in Snowflake.
-- GRANT ALL ON DATABASE TPCXAI_SF0001_QUICKSTART TO ROLE ULTRASONIC_ROLE;
-- GRANT ALL ON ALL SCHEMAS IN DATABASE TPCXAI_SF0001_QUICKSTART TO ROLE ULTRASONIC_ROLE;
-- GRANT ALL ON FUTURE SCHEMAS IN DATABASE TPCXAI_SF0001_QUICKSTART TO ROLE ULTRASONIC_ROLE;

-- Database - INC
-- It will be setup to simulate ingesting of raw source data in an incrementing fashion.
CREATE OR REPLACE DATABASE TPCXAI_SF0001_QUICKSTART_INC;
GRANT OWNERSHIP ON DATABASE TPCXAI_SF0001_QUICKSTART_INC TO ROLE ULTRASONIC_ROLE;


-- ----------------------------------------------------------------------------
-- Step #2: Create the Database level objects
-- ----------------------------------------------------------------------------

-- Database - BASE
USE ROLE ULTRASONIC_ROLE;
USE WAREHOUSE TPCXAI_SF0001_QUICKSTART_WH;
USE DATABASE TPCXAI_SF0001_QUICKSTART;

-- Database - BASE - Schemas
CREATE OR REPLACE SCHEMA TRAINING;
CREATE OR REPLACE SCHEMA SCORING;
CREATE OR REPLACE SCHEMA SERVING;
CREATE OR REPLACE SCHEMA CONFIG;

-- Database - BASE - External TPCXAI Objects
USE SCHEMA CONFIG;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE TPCXAI_STAGE
    URL = 's3://sfquickstarts/getting_started_with_snowflake_feature_store/'
;

-- Database - INC
USE ROLE ULTRASONIC_ROLE;
USE WAREHOUSE TPCXAI_SF0001_QUICKSTART_WH;
USE DATABASE TPCXAI_SF0001_QUICKSTART_INC;

-- Database - INC - Schemas
CREATE OR REPLACE SCHEMA TRAINING;
CREATE OR REPLACE SCHEMA SCORING;
CREATE OR REPLACE SCHEMA SERVING;




