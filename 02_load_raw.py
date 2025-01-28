# ------------------------------------------------------------------------------
# Hands-On Lab: Advanced Guide to Snowflake Feature Store - Data Engineering with Snowpark
# Script:       02_load_raw.py
# Author:       Jacopo Maragna
# Last Updated: 1/24/2025
# Description: This Python script will retrive data from S3 and populate our database tables
# ------------------------------------------------------------------------------

from snowflake.snowpark import Session

from useful_fns import run_sql


ROLE = "ULTRASONIC_ROLE"
WAREHOUSE = "TPCXAI_SF0001_QUICKSTART_WH"
SCALE_FACTOR = "SF0001"
DATABASE = f"TPCXAI_{SCALE_FACTOR}_QUICKSTART"
TPCXAI_EXTERNAL_STAGE = "TPCXAI_STAGE"
TPCXAI_EXTERNAL_FILE_FORMAT = "PARQUET_FORMAT"
TABLES = ["CUSTOMER", "ORDERS", "LINEITEM", "ORDER_RETURNS"]
TABLE_DICT = {
    "training": {"schema": "TRAINING", "tables": TABLES},
    "serving": {"schema": "SERVING", "tables": TABLES},
    "scoring": {"schema": "SCORING", "tables": TABLES},
}

# SNOWFLAKE ADVANTAGE: Schema detection
# SNOWFLAKE ADVANTAGE: Data ingestion with COPY
# SNOWFLAKE ADVANTAGE: Snowflake Tables (not file-based)
# SNOWFLAKE ADVANTAGE: Warehouse elasticity (dynamic scaling)


def load_raw_table(session, date_diff_to_source, tname=None, schema=None):
    session.use_schema(schema)
    # S3 source
    location = f"@{DATABASE}.EXTERNAL.{TPCXAI_EXTERNAL_STAGE}/{schema}/{tname}"

    # CUSTOMER
    if tname == "CUSTOMER":
        run_sql(
            f"""CREATE OR REPLACE TABLE {DATABASE}.{schema}.{tname}
                    (C_CUSTOMER_SK INTEGER,
                    C_CUSTOMER_ID VARCHAR,
                    C_CURRENT_ADDR_SK INTEGER,
                    C_FIRST_NAME VARCHAR,
                    C_LAST_NAME VARCHAR,
                    C_PREFERRED_CUST_FLAG VARCHAR,
                    C_BIRTH_DAY INTEGER,
                    C_BIRTH_MONTH INTEGER,
                    C_BIRTH_YEAR INTEGER,
                    C_BIRTH_COUNTRY VARCHAR,
                    C_LOGIN VARCHAR,
                    C_EMAIL_ADDRESS VARCHAR,
                    C_CLUSTER_ID INTEGER
                    ) CLUSTER BY (C_CUSTOMER_SK);
            """,
            session,
        )
        run_sql(
            f"""COPY INTO {DATABASE}.{schema}.{tname} 
                FROM 
                    (select $1:C_CUSTOMER_SK::INTEGER,
                            $1:C_CUSTOMER_ID::VARCHAR,
                            $1:C_CURRENT_ADDR_SK::INTEGER,
                            $1:C_FIRST_NAME::VARCHAR,
                            $1:C_LAST_NAME::VARCHAR,
                            $1:C_PREFERRED_CUST_FLAG::VARCHAR,
                            $1:C_BIRTH_DAY::INTEGER,
                            $1:C_BIRTH_MONTH::INTEGER,
                            $1:C_BIRTH_YEAR::INTEGER,
                            $1:C_BIRTH_COUNTRY::VARCHAR,
                            $1:C_LOGIN::VARCHAR,
                            $1:C_EMAIL_ADDRESS::VARCHAR,                        
                            $1:C_CLUSTER_ID::INTEGER
                    from {location})
                FILE_FORMAT = {DATABASE}.EXTERNAL.{TPCXAI_EXTERNAL_FILE_FORMAT};
            """,
            session,
        )
    # ORDERS
    elif tname == "ORDERS":
        run_sql(
            f"""CREATE OR REPLACE TABLE {DATABASE}.{schema}.{tname}
                (O_ORDER_ID INTEGER,
                O_CUSTOMER_SK INTEGER,
                ORDER_TS TIMESTAMP,
                WEEKDAY VARCHAR,
                ORDER_DATE DATE,
                STORE INTEGER,
                TRIP_TYPE INTEGER
                ) CLUSTER BY (O_ORDER_ID, ORDER_DATE);
            """,
            session,
        )
        run_sql(
            f"""COPY INTO {DATABASE}.{schema}.{tname} 
                FROM 
                    (select $1:O_ORDER_ID::INTEGER,
                            $1:O_CUSTOMER_SK::INTEGER,
                            timestampadd('MINS', UNIFORM( -1440 , 0 , random() ) ,timestampadd('days',   {date_diff_to_source}, $1:"DATE"::DATE)) ORDER_TS,
                            decode(extract(dayofweek from ORDER_TS), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', 4, 'Thursday',  5, 'Friday',  6, 'Saturday',  0, 'Sunday') WEEKDAY,
                            TO_DATE(ORDER_TS) ORDER_DATE,
                            $1:STORE::INTEGER,
                            $1:TRIP_TYPE::INTEGER
                    from {location})
                FILE_FORMAT = {DATABASE}.EXTERNAL.{TPCXAI_EXTERNAL_FILE_FORMAT};
            """,
            session,
        )
    # LINEITEM
    elif tname == "LINEITEM":
        run_sql(
            f"""CREATE OR REPLACE TABLE {DATABASE}.{schema}.{tname}
                (LI_ORDER_ID INTEGER,
                LI_PRODUCT_ID INTEGER,
                QUANTITY INTEGER,
                PRICE DECIMAL(8,2)
                ) CLUSTER BY (LI_PRODUCT_ID, LI_ORDER_ID);
            """,
            session,
        )
        run_sql(
            f"""COPY INTO {DATABASE}.{schema}.{tname} 
                FROM 
                    (select $1:LI_ORDER_ID::INTEGER,
                            $1:LI_PRODUCT_ID::INTEGER,
                            $1:QUANTITY::INTEGER,
                            $1:PRICE::DECIMAL(8,2)
                    from {location})
                FILE_FORMAT = {DATABASE}.EXTERNAL.{TPCXAI_EXTERNAL_FILE_FORMAT};
            """,
            session,
        )
    # ORDER_RETURNS
    elif tname == "ORDER_RETURNS":
        run_sql(
            f"""CREATE OR REPLACE TABLE {DATABASE}.{schema}.{tname}
                (OR_ORDER_ID INTEGER,
                OR_PRODUCT_ID INTEGER,
                OR_RETURN_QUANTITY INTEGER
                ) CLUSTER BY (OR_PRODUCT_ID, OR_ORDER_ID);
            """,
            session,
        )
        run_sql(
            f"""COPY INTO {DATABASE}.{schema}.{tname} 
                FROM 
                    (select $1:OR_ORDER_ID::INTEGER,
                            $1:OR_PRODUCT_ID::INTEGER,
                            $1:OR_RETURN_QUANTITY::INTEGER
                    from {location})
                FILE_FORMAT = {DATABASE}.EXTERNAL.{TPCXAI_EXTERNAL_FILE_FORMAT};
            """,
            session,
        )


def load_all_raw_tables(session):
    # Calculate the DATE point difference between the source data and todays date.
    # This reference point will be used to select a subset of the data for pre-loading, and the remainder will be incrementally ingested via a scheduled task.
    date_diff_to_source = session.sql(
        """select timestampdiff('days',  '2013-04-01', CURRENT_DATE() )::VARCHAR date_diff_to_source"""
    ).collect()[0][0]
    print(
        "Difference in Days between source data and current date :", date_diff_to_source
    )

    _ = session.sql(
        f"ALTER WAREHOUSE {WAREHOUSE} SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE"
    ).collect()

    for data in TABLE_DICT.values():
        tnames = data["tables"]
        schema = data["schema"]
        for tname in tnames:
            print("Loading {}".format(tname))
            load_raw_table(session, date_diff_to_source, tname=tname, schema=schema)

    _ = session.sql(
        f"ALTER WAREHOUSE {WAREHOUSE} SET WAREHOUSE_SIZE = XSMALL"
    ).collect()


# For local debugging
# Make sure to override the default connection name with an environment variable as follows
# export SNOWFLAKE_DEFAULT_CONNECTION_NAME="tk34300.eu-west-1"
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        session.use_role(ROLE)
        session.use_warehouse(WAREHOUSE)
        session.use_database(DATABASE)
        load_all_raw_tables(session)
