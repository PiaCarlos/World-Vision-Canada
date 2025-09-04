from distro import like
import pandas as pd
import numpy as np
from snowflake.snowpark.functions import col
import snowflake.snowpark as snowpark
from collections import defaultdict
import networkx
from gen_excel_functions import *
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import upper
from gen_excel_functions import tie, export_excel
from gen_graph import *
from gen_filtering import df_filter
import pytz
from datetime import datetime

# extracing relevant data from snowflake
def read_snowflake(session):  

    # read from dim, all actie projects 
    df_dim = (session.table("DEV.PUBLISH.DIM_P").filter(col("ACTIVE_IND") == "Y").select(
          col("COUNTRY_NAME"),
          col("COUNTRY"),
          col("PROJECT_ID"),
          col("PROJECT_NAME"),
          col("IVS_PROJECT_CODE"),
          col("WVC_FUNDED_AP"),
          col("PROJECT_TYPE")
        )
    )

    # read from dpmsprojectlocations, all locations from projects. if a project doesn't have a location, those colums will be none. 
    df_loc = session.table("DEV.PUBLISH.DIM_DPMS").filter(col("ACTIVE_IND") == "Y").select(
        col("PROJECT_ID"),
        col("WVC_DPMS_SUBREGION_ID_NAME"),
        col("WVC_DPMS_SUB_REGIONID"),
        col("CITY")
        )
    
    # useful to translate country id to country code
    df_country_id = session.table("DEV.REFERENCE.VW").select(
        col("COUNTRY_CODE"),
        col("COUNTRY_HKEY"),
        col("COUNTRY"),
        col("COUNTRY_ID")
        )

    # then, grab the codes of WFP and GIK
    # update to a publish schema whenever possible
    df_translation = (
    session
      .table("DEV.raw.raw_dpms")
      .filter(col("entity_name") == "crc5f_projectprofiles")
      .filter(col("option_set_name") == "cr141_projecttype")
      .filter(col("localized_label").isin(["WFP", "GIK"]))
      .select(col("option_1").alias("PROJECT_TYPE"), col("localized_label"))
    )


    
    # join them 
    df = (df_dim.join(df_loc, on="PROJECT_ID", how="left"))

    return df, df_country_id , df_translation


# assign overlap codes for unique projects 
def gen_code_unique(df_unique_pd):
    df = df_unique_pd.copy()
    df['OVERLAP_CODE'] = df['WVC_FUNDED_AP']
    return df

# assign overlap codes for gik and wfp projects
def gen_code_wfp_gik(df_wfp_gik_pd):    
    df = df_wfp_gik_pd.copy()
    df['OVERLAP_CODE'] = df['LOCALIZED_LABEL'].where(df['LOCALIZED_LABEL'].isin(["WFP","GIK"]), other=None)
    return df

# assign overlap codes to remaining projects 
def gen_code_remaining(df_remaining_pd, project_to_overlap):
    df = df_remaining_pd.copy()
    df['OVERLAP_CODE'] = df['PROJECT_ID'].map(project_to_overlap)
    return df


def truncate_and_populate(session, df, table_name):
    session.sql(f'truncate table if exists ANALYTICS.{table_name};').collect()
    session.write_pandas(df = df, schema = 'ANALYTICS', table_name = table_name, overwrite=False, auto_create_table=False)
    return
 

def get_current_time():
    return datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
 
def timestamp_dataframe(df):    
    nowtime = get_current_time()
    df['INSERT_DATE'] = nowtime
    df['UPDATE_DATE'] = nowtime
    return df


# Main function which will be executed in execute_script or other executable. 
def main(session: snowpark.Session):
    
    print('Read data from snowflake')
    df, df_country_id , df_translation  = read_snowflake(session)

    print('filter out unique and gik/wfp projects')
    df_unique, df_GIK_WFP, df_remaining =  df_filter(session, df , df_translation)

    print('transforming the data frames to pandas')
    df_unique_pd = df_unique.to_pandas()
    df_GIK_WFP_pd = df_GIK_WFP.to_pandas()
    df_remaining_pd = df_remaining.to_pandas()
    df_country_id = df_country_id.to_pandas()

    print('Create sub_region and no_sub_region dictionaries')
    dict_with_sub, dict_without_sub = create_dictionnary(df_remaining_pd)

    print('Create graph with projects')
    graph = create_graph(df_remaining_pd, dict_with_sub)

    print('Generate overlap codes')
    project_to_overlap = generate_OverCode(graph, df_country_id, df_remaining_pd, dict_without_sub)

    print('assign all overlap codes')
    df_remaining_pd = gen_code_remaining(df_remaining_pd, project_to_overlap)
    df_unique_pd = gen_code_unique(df_unique_pd)
    df_GIK_WFP_pd = gen_code_wfp_gik(df_GIK_WFP_pd)

    print('tie them together ')
    df = tie(df_unique_pd, df_GIK_WFP_pd, df_remaining_pd)

    print('get the datetime')
    timestamp_dataframe(df)

    print('updating table in snowflake')
    truncate_and_populate(session, df, "ANLT_IA_OVERLAP_CODES")
 
    print("Operation was successful")

