import snowflake.snowpark.functions as F
from snowflake.ml.modeling.pipeline import Pipeline as sml_Pipeline
from snowflake.ml.modeling.preprocessing import MinMaxScaler as sml_MinMaxScaler
from snowflake.ml.modeling.cluster import KMeans as sml_KMeans

from useful_fns import (
    init_snowflake,
    check_and_update,
    create_ModelRegistry,
    create_FeatureStore,
)


def create_spine(fv_uc01_preprocess):
    spine_sdf = fv_uc01_preprocess.feature_df.group_by("O_CUSTOMER_SK").agg(
        F.max("LATEST_ORDER_DATE").as_("ASOF_DATE")
    )  # .limit(10)
    print(spine_sdf.sort("O_CUSTOMER_SK").show(5))

    return spine_sdf


def generate_dataset(fs, spine_sdf, fv_uc01_preprocess):
    # Generate_Dataset
    training_dataset = fs.generate_dataset(
        name="UC01_TRAINING",
        spine_df=spine_sdf,
        features=[fv_uc01_preprocess],
        spine_timestamp_col="ASOF_DATE",
    )

    # Create a snowpark dataframe reference from the Dataset
    training_dataset_sdf = training_dataset.read.to_snowpark_dataframe()
    # Display some sample data
    training_dataset_sdf.sort("O_CUSTOMER_SK").show(5)

    training_dataset_sdf.to_pandas()

    print(training_dataset.list_versions())
    print(training_dataset.selected_version)
    print(training_dataset.fully_qualified_name)

    return training_dataset_sdf


## MODEL PIPELINE
## - Model Specific Transforms
## - Model Fitting Function (Kmeans)
def uc01_train(featurevector, num_clusters):
    mms_input_cols = ["RETURN_RATIO", "FREQUENCY"]
    km_input_cols = mms_output_cols = ["RETURN_RATIO_MMS", "FREQUENCY_MMS"]
    km_output_cols = "CLUSTER"
    km4_purchases = sml_Pipeline(
        steps=[
            (
                "MMS",
                sml_MinMaxScaler(
                    clip=True,
                    input_cols=mms_input_cols,
                    output_cols=mms_output_cols,
                ),
            ),
            (
                "KM",
                sml_KMeans(
                    n_clusters=num_clusters,
                    init="k-means++",
                    max_iter=300,
                    n_init=10,
                    random_state=0,
                    input_cols=km_input_cols,
                    output_cols=km_output_cols,
                ),
            ),
        ]
    )
    km4_purchases.fit(featurevector.select(mms_input_cols))
    return {"MODEL": km4_purchases}


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

    # Feature view
    ppd_fv_name = "FV_UC01_PREPROCESS"
    ppd_fv_version = "V_1"

    # Init Snowflake
    session, warehouse_env = init_snowflake(
        scale_factor, tpcxai_database, tpcxai_training_schema, fs_qs_role
    )

    # Create/Reference Snowflake Model Registry - Common across Environments
    mr = create_ModelRegistry(session, tpcxai_database, "_MODEL_REGISTRY")

    # Get Feature Store
    fs = create_FeatureStore(
        session,
        tpcxai_database,
        f"""_{tpcxai_training_schema}_FEATURE_STORE""",
        warehouse_env,
    )

    # Retrieve a Feature View instance for use within Python
    fv_uc01_preprocess = fs.get_feature_view(ppd_fv_name, ppd_fv_version)

    # Create Spine
    spine_sdf = create_spine(fv_uc01_preprocess)

    # Generate training dataset
    training_dataset_sdf = generate_dataset(fs, spine_sdf, fv_uc01_preprocess)

    # Fit the KMeans Model
    model_name = "UC01_SNOWFLAKEML_KMEANS_MODEL"
    num_clusters = 5

    train_result = uc01_train(training_dataset_sdf, num_clusters)

    # Check for the latest version of this model in registry, and increment version
    mr_df = mr.show_models()
    model_version = check_and_update(mr_df, model_name)
    print("model version:\t", model_version)
    # Save the Model to the Model Registry
    mv_kmeans = mr.log_model(
        model=train_result["MODEL"],
        model_name=model_name,
        version_name=model_version,
        comment="TPCXAI USE CASE 01 - KMEANS - CUSTOMER PURCHASE CLUSTERS",
    )

    print(mr.show_models())

    # Get and set default for latest version of the model
    m = mr.get_model(model_name)
    latest_version = m.show_versions().iloc[-1]["name"]
    mv = m.version(latest_version)
    m.default = latest_version

    print("Model training succesfully completed !")
