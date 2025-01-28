import json

from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.ml.feature_store import FeatureView, Entity

from useful_fns import formatSQL, create_FeatureStore, init_snowflake
from feature_engineering_fns import uc01_load_data, uc01_pre_process


def get_dataframes(session, tpcxai_database, tpcxai_schema):
    # Tables
    customer_tbl = ".".join([tpcxai_database, tpcxai_schema, "CUSTOMER"])
    line_item_tbl = ".".join([tpcxai_database, tpcxai_schema, "LINEITEM"])
    order_tbl = ".".join([tpcxai_database, tpcxai_schema, "ORDERS"])
    order_returns_tbl = ".".join([tpcxai_database, tpcxai_schema, "ORDER_RETURNS"])

    # Snowpark Dataframe
    customer_sdf = session.table(customer_tbl)
    line_item_sdf = session.table(line_item_tbl)
    order_sdf = session.table(order_tbl)
    order_returns_sdf = session.table(order_returns_tbl)

    # Row Counts
    print(f"""\nTABLE ROW_COUNTS IN {tpcxai_schema}""")
    print(customer_tbl, customer_sdf.count())
    print(line_item_tbl, line_item_sdf.count())
    print(order_tbl, order_sdf.count())
    print(order_returns_tbl, order_returns_sdf.count())

    return [customer_sdf, line_item_sdf, order_sdf, order_returns_sdf]


def create_customer_entity(fs):
    if "CUSTOMER" not in json.loads(
        fs.list_entities().select(F.to_json(F.array_agg("NAME", True))).collect()[0][0]
    ):
        customer_entity = Entity(
            name="CUSTOMER",
            join_keys=["O_CUSTOMER_SK"],
            desc="Primary Key for CUSTOMER",
        )
        fs.register_entity(customer_entity)
    else:
        customer_entity = fs.get_entity("CUSTOMER")

    print(fs.list_entities().show())

    return customer_entity


def load_raw_data(order_sdf, line_item_sdf, order_returns_sdf):
    raw_data = uc01_load_data(order_sdf, line_item_sdf, order_returns_sdf)

    return raw_data


def preprocess_data(order_sdf, line_item_sdf, order_returns_sdf):
    raw_data = load_raw_data(order_sdf, line_item_sdf, order_returns_sdf)

    preprocessed_data = uc01_pre_process(raw_data)

    # Format the SQL for the Snowpark Dataframe
    ppd_sql = formatSQL(preprocessed_data.queries["queries"][0], True)

    return [preprocessed_data, ppd_sql]


def create_feature_view(fs, customer_entity, ppd_sql):
    # Define descriptions for the FeatureView's Features.  These will be added as comments to the database object
    preprocess_features_desc = {
        "FREQUENCY": "Average yearly order frequency",
        "RETURN_RATIO": "Average of, Per Order Returns Ratio.  Per order returns ratio : total returns value / total order value",
    }

    ppd_fv_name = "FV_UC01_PREPROCESS"
    ppd_fv_version = "V_1"

    try:
        # If FeatureView already exists just return the reference to it
        fv_uc01_preprocess = fs.get_feature_view(
            name=ppd_fv_name, version=ppd_fv_version
        )
    except:
        # Create the FeatureView instance
        fv_uc01_preprocess_instance = FeatureView(
            name=ppd_fv_name,
            entities=[customer_entity],
            # feature_df=preprocessed_data,      # <- We can use the snowpark dataframe as-is from our Python
            feature_df=session.sql(
                ppd_sql
            ),  # <- Or we can use SQL, in this case linted from the dataframe generated SQL to make more human readable
            timestamp_col="LATEST_ORDER_DATE",
            # refresh_freq="60 minute",  # <- specifying optional refresh_freq creates FeatureView as Dynamic Table, else created as View.
            desc="Features to support Use Case 01",
        ).attach_feature_desc(preprocess_features_desc)

        # Register the FeatureView instance.  Creates  object in Snowflake
        fv_uc01_preprocess = fs.register_feature_view(
            feature_view=fv_uc01_preprocess_instance, version=ppd_fv_version, block=True
        )
        print(f"Feature View : {ppd_fv_name}_{ppd_fv_version} created")
    else:
        print(f"Feature View : {ppd_fv_name}_{ppd_fv_version} already created")
    finally:
        fs.list_feature_views().show(20)

    return fv_uc01_preprocess


# Make sure to override the default connection name with an environment variable as follows
# export SNOWFLAKE_DEFAULT_CONNECTION_NAME="tk34300.eu-west-1"
if __name__ == "__main__":
    # Scale Factor
    scale_factor = "SF0001"

    # Roles
    fs_qs_role = "ULTRASONIC_ROLE"

    # Database
    tpcxai_database = f"TPCXAI_{scale_factor}_QUICKSTART"

    # Schemas
    tpcxai_training_schema = "TRAINING"
    tpcxai_scoring_schema = "SCORING"
    tpcxai_serving_schema = "SERVING"

    # Init Snowflake
    session, warehouse_env = init_snowflake(
        scale_factor, tpcxai_database, tpcxai_training_schema, fs_qs_role
    )

    # Get Feature Store
    fs = create_FeatureStore(
        session,
        tpcxai_database,
        f"""_{tpcxai_training_schema}_FEATURE_STORE""",
        warehouse_env,
    )

    # Get Dataframes
    customer_sdf, line_item_sdf, order_sdf, order_returns_sdf = get_dataframes(
        session, tpcxai_database, tpcxai_training_schema
    )

    # Create customer entity
    customer_entity = create_customer_entity(fs)

    # Get preprocessed data
    preprocessed_data, ppd_sql = preprocess_data(
        order_sdf, line_item_sdf, order_returns_sdf
    )

    # Create Feature View
    fv_uc01_preprocess = create_feature_view(fs, customer_entity, ppd_sql)

    print(fv_uc01_preprocess.feature_df.show())
    print("Feature engineering succesfully completed !")
