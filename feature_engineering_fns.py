# FEATURE ENGINEERING FUNCTIONS

from datetime import date, datetime
from decimal import Decimal
# SNOWFLAKE
# Snowpark
from snowflake.snowpark import Session, DataFrame, Window, WindowSpec
# from snowflake.snowpark import Analytics
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
from snowflake.snowpark.version import VERSION

def uc01_load_data(order_data: DataFrame, lineitem_data: DataFrame, order_returns_data: DataFrame) -> DataFrame:
    """
    Merges order, linetime and order_returns data and replaces Nulls/None with appropriate default values.
    order_data         : A dataframe referencing the "ORDER" table in the relevant schema
    lineitem_data      : A dataframe referencing the LINEITEM table in the relevant schema
    order_returns_data : A dataframe referencing the ORDER_RETURNS table in the relevant schema

    Returns            : Merged/cleansed dataframe with required columns
    """
    # Default replacement values for Null dates and decimal types
    epoch_dt = date(year=1970, month=1, day=1)
    decimal_zero = Decimal('0.0')
    
    # Merge three dataframes
    raw_data =  lineitem_data.join(
        order_returns_data,
        (lineitem_data["LI_ORDER_ID"] == order_returns_data["OR_ORDER_ID"]) &
        (lineitem_data["LI_PRODUCT_ID"] == order_returns_data["OR_PRODUCT_ID"]),
        "left") \
        .join(
            order_data,
            order_returns_data["OR_ORDER_ID"] == order_data["O_ORDER_ID"],
            "inner") \
        .select(   "O_ORDER_ID",  "O_CUSTOMER_SK", "ORDER_TS", "WEEKDAY", "ORDER_DATE", "LI_PRODUCT_ID", "PRICE", "QUANTITY", "OR_RETURN_QUANTITY") \
        .fillna({ "O_ORDER_ID": 0, "O_CUSTOMER_SK": 0, "ORDER_DATE": epoch_dt, "PRICE": decimal_zero, "QUANTITY": 0, "OR_RETURN_QUANTITY": 0 })

    return raw_data[['O_ORDER_ID', 'O_CUSTOMER_SK', 'ORDER_DATE', 'LI_PRODUCT_ID', 'PRICE', 'QUANTITY', 'OR_RETURN_QUANTITY']]

def uc01_pre_process(data: DataFrame) -> DataFrame:
    """
    Performs model-agnostic Feature-Engineering to prepare data for Use Case 01 model for Customer Entity level features
    data         : A dataframe containing the merged/cleansed data from Order, Lineitem and Order_returns tables
    result       : Customer level behavioural features
    """
    # Calculate INVOICE_YEAR, ROW_PRICE and RETURN_ROW_PRICE
    data = data.with_columns(["INVOICE_YEAR",  "ROW_PRICE",  "RETURN_ROW_PRICE" ]
                            ,[ F.year(data["ORDER_DATE"]),  data["QUANTITY"] * data["PRICE"],  data["OR_RETURN_QUANTITY"] * data["PRICE"]] )

    # Generate Customer/Order level features : total-price, total-return-price, year of first order last-order-date
    groups = data.groupBy("O_CUSTOMER_SK", "O_ORDER_ID").agg(
        F.sum(F.col("ROW_PRICE")).alias("ROW_PRICE"),
        F.sum(F.col("RETURN_ROW_PRICE")).alias("RETURN_ROW_PRICE"),
        F.min(F.col("INVOICE_YEAR")).alias("INVOICE_YEAR"),
        F.max(F.col("ORDER_DATE")).alias("LATEST_ORDER_DATE"))

    # Calculate price RETURN RATIO per Customer
    groups = groups.withColumn("RATIO", groups["RETURN_ROW_PRICE"] / groups["ROW_PRICE"])
    ratio = groups.groupBy("O_CUSTOMER_SK").agg(F.avg(F.col("RATIO")).cast(T.FloatType()).alias("RETURN_RATIO"), 
                                                F.max(F.col("LATEST_ORDER_DATE")).alias("LATEST_ORDER_DATE")
                                                )

    # Calculate average annual shopping FREQUENCY 
    frequency_groups = groups.groupBy("O_CUSTOMER_SK", "INVOICE_YEAR").agg(F.count(F.col("O_ORDER_ID")).cast(T.FloatType()).alias("FREQUENCY"))
    frequency = frequency_groups.groupBy("O_CUSTOMER_SK").agg(F.avg(F.col("FREQUENCY")).alias("FREQUENCY"))

    # Merge FREQUENCY and RETURN_RATIO
    result = frequency.join(ratio, on="O_CUSTOMER_SK")

    return result