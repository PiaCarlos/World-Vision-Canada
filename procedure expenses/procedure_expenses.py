import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.snowpark.functions import call_udf, lit, col, coalesce, sql_expr

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, substring, concat, lit, udf, sum as sum_, when, max as max_
from snowflake.snowpark.types import StringType
from snowflake.snowpark.window import Window
from datetime import datetime
from snowflake.snowpark.functions import row_number

def main(session: snowpark.Session): 

    # find out if procedure started
    print('procedure has started succesfully')

    # Define the query to fetch the data
    query = "SELECT * FROM PROD.PUBLISH.VW_EXPENSE WHERE t4_funding_source LIKE '%CANO%'"
    # Execute the query using Snowpark DataFrame
    snowpark_df = session.sql(query)

    # Transform Fiscal Year (e.g. FY2025 -> FY25)
    snowpark_df = snowpark_df.withColumn(
        "FISCAL_YEAR_NEW",
        concat(lit("FY"), substring(col("FISCAL_YEAR"), -2, 2).cast("string"))
    )

    # Define a UDF for transformation
    def transform_to_IVS_Objective_Code(output):
        output_str = str(output) if output is not None else ""
        parts = output_str.split(".")
        while len(parts) < 4:
            parts.insert(0, "00")
        parts = [part.zfill(2) for part in parts]
        return ".".join(parts)

    # Register UDF ( transform output to Ivs_objective_code e.g 1.2 -> 00.00.01.02)
    transform_to_IVS_Objective_Code_udf = udf(
        transform_to_IVS_Objective_Code,
        return_type=StringType(),
        input_types=[StringType()]
    )

    # Apply the UDF  (also cast OUTPUT to string for safety)
    snowpark_df = snowpark_df.withColumn(
        "IVS_OBJECTIVE_CODE",
        transform_to_IVS_Objective_Code_udf(col("OUTPUT").cast("string"))
    )

    # Fetch the lookup table data
    lookup_query = "SELECT * FROM DEV.PUBLISH.VW_active"
    lookup_df = session.sql(lookup_query)

    # Join on fiscal year and A9 project number
    joined_df = snowpark_df.join(
    lookup_df,
    (col("FISCAL_YEAR_NEW") == lookup_df["PROJECT_ACTIVE_FY"]) &
    (col("A9_PROJECT_NUMBER").cast("string") == substring(lookup_df["PROJECT_CODE"], 9, 6)),
    how="left" )

    # Window specification
    window_spec = Window.partitionBy("A9_PROJECT_NUMBER", "OUTPUT", "FISCAL_YEAR")
    current_fiscal_year = f"FY{datetime.now().year}"

    # Calculate maximum EXTRACT_PERIOD and SUM_YTD_ACTUAL with conditions
    max_extract_period_df = joined_df.withColumn(
        "MAX_EXTRACT_PERIOD",
        max_("EXTRACT_PERIOD").over(window_spec)
    )

    condition_col = when(
        col("FISCAL_YEAR") == current_fiscal_year,
        when(col("EXTRACT_PERIOD") == col("MAX_EXTRACT_PERIOD"), col("YTD_ACTUAL")).otherwise(0)
    ).otherwise(
        when(col("EXTRACT_PERIOD").cast("string").endswith("012"), col("YTD_ACTUAL")).otherwise(0)
    )

    # Add the SUM_YTD_ACTUAL column
    final_df = max_extract_period_df.withColumn("SUM_YTD_ACTUAL", sum_(condition_col).over(window_spec))

    # Select and rename columns
    final_df = final_df.select(
        col("A9_PROJECT_NUMBER"),
        col("t4_funding_source"),
        col("FISCAL_YEAR"),
        col("FISCAL_YEAR_NEW"),
        col("OUTPUT"),
        col("IVS_OBJECTIVE_CODE"),
        col("OUTPUT_DESCRIPTION"),
        col("OFFICE_NAME"),
        # new colums added
        col("T6_LOGFRAME_CODE"),
        col("T9_ALLOCATION_TYPE_DESCRIPTION"),
        col("T9_ALLOCATION_TYPE"), 
        col("ACCOUNT_ROLLUP"),
        col("ACCOUNT_ROLLUP_DESCRIPTION"),
        col("ACCOUNT_CODE"),
        col("ACCOUNT_NAME"),  
        col("YTD_ACTUAL").alias("Sum of YTD_ACTUAL"),
        col("YTD_BUDGET").alias("Sum of YTD_BUDGET"),
        col("EXTRACT_PERIOD").alias("ExtractPeriod"),
        col("SUM_YTD_ACTUAL").alias("TOTAL_YTD_ACTUAL")
    )

    # Add timestamps
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_df = final_df.withColumn("INSERT_DATE", lit(ts)).withColumn("UPDATE_DATE", lit(ts))


    # add a unique code for each row
    # grab last 3 digits of extract period to get the month
    month = substring(col("ExtractPeriod").cast("string"), -2, 2)
    t6_code = substring(col("T6_LOGFRAME_CODE").cast("string"), -2, 2)
    
    # ad column for the month
    final_df = final_df.withColumn("month", month)
    # Partition total YTD for each project/month
    projectMonth_win = Window.partition_by(col("A9_PROJECT_NUMBER"), col("FISCAL_YEAR_NEW"), col("OUTPUT"),
    col("month"))
    # add colum for each month
    final_df = final_df.withColumn(
    "PROJECT_MONTH_TOTAL_YTD",
    sum_(col("Sum of YTD_ACTUAL")).over(projectMonth_win)
    )

    # drop month colums 
    final_df = final_df.drop("month")

    # organise the columns
    column = ["PROJECT_MONTH_TOTAL_YTD"]
    stop_column = "TOTAL_YTD_ACTUAL"
    others = [c for c in final_df.columns if c not in column]
    index = others.index(stop_column)
    new_order = others[:index] + column + others[index:]

    # change order
    final_df = final_df.select(*[col(c) for c in new_order])

    # table names
    table_name = "LOGFRAME_EXPENSE"
    test_table_name = "EXPENSE_TESTING"
    dev  = "DEV"
    prod = "PROD"

    # write the Test Dataframe to the Snowflake table
    final_df.write.mode("overwrite").save_as_table(f"{dev}.ANALYTICS.{test_table_name}")
    print("test table created or modified succesfully")

    return f"Procedure completed. {final_df.count()} rows inserted into {test_table_name}."
