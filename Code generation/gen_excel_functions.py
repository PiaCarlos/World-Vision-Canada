import pandas as pd
import numpy as np


# Convert in excel for testing
def write_excel_test(df_pandas):
    df_pandas.to_excel("./data/project_locations.xlsx", index=False)
    print("the data was exported to an excel file correctly")

# don't count projects that have a overlap code that contains unique - excel version
def filter_unique_excel(df_pandas):
    """ I'm testing with excel, but we should use this
    df_OverCode = session.table("IVS_DPMS_PROD.PUBLISH.DIM_PROJECT").filter(col("ACTIVE_IND") == "Y").select(
        col("WVC_FUNDED_AP"),
        col("PROJECT_ID"),
        col("PROJECT_NAME"),
        )
    """
    # If there is an old excel, read it 
    try:
        old_code = pd.read_excel("./data/project_locations_city_overlap.xlsx", usecols=['PROJECT_ID','OVERLAP_CODE'])
    except FileNotFoundError:
        # there is no excel file 
        print("excel file not found, so unique not ignored")
        return df_pandas
    
    # filter for codes that contain the word : unique
    mask = old_code['OVERLAP_CODE'].str.contains('unique', case=False, na=False)
    unique_projects = set(old_code.loc[mask, 'PROJECT_ID'])
    df_pandas = df_pandas[~df_pandas["PROJECT_ID"].isin(unique_projects).copy()]
    
    return df_pandas

# to export to 3 different excel for each sub df. 
def test_excel_separated(df_unique, df_GIK_WFP, df_remaining_pd, project_to_overlap, df):
    print('testing_print unique ')
    df_unique_pd = df_unique.to_pandas()
    df_unique_pd["TEST_GROUP"] = "UNIQUE"
    df_unique_pd.to_excel("./data/tests/unique_projects.xlsx", index=False)

    print('testing_print unique ')
    df_wfp_gik_loc = df_GIK_WFP.join(df, on="PROJECT_ID", how="left")
    df_wfp_gik_pd = df_wfp_gik_loc.to_pandas()
    # tag with their label (WFP or GIK) if you like:
    df_wfp_gik_pd["TEST_GROUP"] = df_wfp_gik_pd["PROJECT_TYPE"]
    df_wfp_gik_pd.to_excel("./data/tests/wfp_gik_projects.xlsx", index=False)

    print("testing printing remaining - so normal printing of overlap codes")
    df_remaining_pd['OVERLAP_CODE'] = df_remaining_pd['PROJECT_ID'].map(project_to_overlap)
    # export to excel 
    df_remaining_pd.to_excel("./data/tests/overlap_n_testing.xlsx", index=False)
    print("Excel file with overlap was succesful")


# others excels are outdated, this one perfectly tests it and loads into an excel file. 
def tie(df_unique_pd, df_GIK_WFP_pd, df_remaining_pd):
    # choosing colums
    cols = [
        "COUNTRY_NAME",
        "PROJECT_ID",
        "IVS_PROJECT_CODE",
        "PROJECT_NAME",
        "WVC_DPMS_SUBREGION_ID_NAME",
        "CITY",
        "OVERLAP_CODE"
    ]

    # remove other colums 
    df_unique_pd = df_unique_pd.reindex(columns=cols)
    df_GIK_WFP_pd = df_GIK_WFP_pd.reindex(columns=cols)
    df_remaining_pd = df_remaining_pd.reindex(columns=cols)

    # tie them 
    df_pd = pd.concat([df_unique_pd, df_GIK_WFP_pd, df_remaining_pd], ignore_index=True)

    # eliminate duplicates 
    df_pd = df_pd.drop_duplicates(subset=['PROJECT_ID'], keep='first')

    return df_pd


def export_excel(df):
     df.to_excel("./data/overlap_locations_final.xlsx", index=False)