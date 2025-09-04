
from snowflake.snowpark.functions import col
import pandas as pd
import numpy as np
from snowflake.snowpark.functions import upper

# remove overlap codes that contain unique and WFP/GIK projects -> they will later be used to change the projects names for WFP and GIK or stay as unique
def df_filter(session, df, df_translation):

    # filter by unique
    # take those that contain unique
    projects_filtered = (df.filter(upper(col("WVC_FUNDED_AP")).like("%UNIQUE%")).select("PROJECT_ID"))
    # grab these projects and remove them from df_pandas
    df_unique = df.join(projects_filtered, on="PROJECT_ID", how="left_semi")
    df_without_unique = df.join(projects_filtered, on="PROJECT_ID", how="left_anti")
    
    
    # filter by gik or wfp
    # join df_non_unique with the table of project codes and df_translation
    df_with_label = df_without_unique.join(df_translation.select("PROJECT_TYPE", "localized_label"),on="PROJECT_TYPE",how="left")
    # if label is not null, it's a gik or a wfp. 
    df_GIK_WFP = df_with_label.filter(col("localized_label").isNotNull())

    # if label is null -> then it's not unique nor gik/wfp. the remaining
    df_remaining = df_with_label.filter(col("localized_label").isNull())

    return df_unique, df_GIK_WFP, df_remaining
