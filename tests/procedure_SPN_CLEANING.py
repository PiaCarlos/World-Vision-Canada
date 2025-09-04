import pandas as pd
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.snowpark.functions import col
import snowflake.snowpark as snowpark
import sys
import math
import pytz


# read data from snowflake and the 4 excel files
def read_snowflake(session):  

    # read from project result data the result data that is in DPMS
    df_DPMS = (session.table("PROD.PUBLISH.VW").filter(col("PERIOD") == "FY25").filter(col("PROJECT_CODE").like("%SPN%"))
               .filter(col("NUMERATOR") > 0).select(
        'INDICATOR_CODE_NAME',
        'PROJECT_CODE',
        'FREQUENCY',
        'RESULT_NAME',
        'NUMERATOR',
        'DENOMINATOR'
    ))
    
    # read the excel files that are in data
    df_source = pd.read_csv('./data/CANO FY25 Midterm Data_source_step7A_MonitoringPlan(NEWDATA).csv', encoding='latin1',  usecols=["WVC_Code","IndicatorCode" , "IVS_ProjectCode", "FyAchieved", "FyAchievedDenominator", "FyTarget", "FyTargetDenominator" ,"LastAchievedModifiedDate"])
    
    df_actuals_test = pd.read_csv('./data/UpdateTOTALS 29_Jul_25 1-49-20 PM(UpdateTOTALS).csv', encoding='latin1')
    df_actuals_rest = pd.read_csv('./data/UpdateTOTALS 29_Jul_25 2-32-14 PM(UpdateTOTALS).csv', encoding='latin1')
    df_target_value = pd.read_csv('./data/Update_Targets 29_Jul_25 4-01-33 PM(Update_Targets).csv', encoding='latin1', usecols=["Indicator Code (Indicator Statement) (Indicator)", "Name", "Logic Model (Monitoring Plan) (Monitoring Plan)", "Total Numerator", "Total Denominator"])
    
    # merge both actual data frames 
    df_actuals = pd.concat([df_actuals_test, df_actuals_rest], ignore_index=True)


    return df_source, df_actuals, df_target_value, df_DPMS

def renaming_colums(df_source, df_target_value, df_actuals,   df_DPMS):
    
    #  ok so I need to grab the "ProjectCode"  and "IVS_ProjectCode" in source to compare to the target value using "INDICATOR CODE" and "Logic Model (Monitoring Plan) (Monitoring Plan)" to compare it to the output in dpms using     
    # "INDICATOR_CODE_NAME" and "PROJECT_CODE"
    # also need to change numerators/denomminators names 

    # rename colums 
    df_source = df_source.rename(columns={ 
        'WVC_Code': 'INDICATOR_CODE_NAME',
        'IVS_ProjectCode' : 'PROJECT_CODE',
        'FyAchieved' : 'archieved_numerator_s',
        'FyAchievedDenominator' : 'archieved_denominator_s',
        'FyTarget' : 'target_numerator_s',
        'FyTargetDenominator' : 'target_denominator_s'
    })

    
    df_target_value = df_target_value.rename(columns={ 
        'Indicator Code (Indicator Statement) (Indicator)': 'INDICATOR_CODE_NAME',
        'Logic Model (Monitoring Plan) (Monitoring Plan)' : 'PROJECT_CODE',
        'Total Numerator' : 'Numerator_target',
        'Total Denominator' : 'Denominator_target',
        'Name' : 'RESULT_NAME'
    })

    df_actuals = df_actuals.rename(columns={ 
        'Indicator Code (Indicator Statement) (Indicator)': 'INDICATOR_CODE_NAME',
        'Logic Model (Monitoring Plan) (Monitoring Plan)' : 'PROJECT_CODE',
        'Total Numerator' : 'Numerator_target',
        'Total Denominator' : 'Denominator_target',
        'Name' : 'RESULT_NAME'
    })


    return df_source, df_target_value, df_actuals, df_DPMS


def remove_different_IndicatorCode(df_source):
    df = df_source.copy()

    # lowercase and remove spaces to not cause issues
    df["IndicatorCode"] = (df["IndicatorCode"].astype("string").str.strip().str.upper())


    # find the cases where a indicator code name has different indicator codes. 
    mask = (df.groupby(["PROJECT_CODE", "INDICATOR_CODE_NAME"])["IndicatorCode"].transform(lambda s: s.dropna().nunique()) > 1)


    # take only ones that have different indicatorCodes
    df_diff = df.loc[mask]
    df_cleaned = df.loc[~mask]

    # make an excel of df_diff
    df_diff.to_excel("./data/tests/different_indicator_code.xlsx", index=False)

    return df_cleaned



def compare_source_dpms(df_source, df_DPMS):

    # get the last modified time as a column 
    df_source['LastAchievedModifiedDate'] = pd.to_datetime(df_source['LastAchievedModifiedDate'], errors='coerce')

    # put the most recent first then delete the others, so we take the most recent row in source
    df_source = df_source.sort_values('LastAchievedModifiedDate', ascending=False)

    # take the first number greater than 0 for numerator in archieved or target
    mask_bigger_0 = (df_source.get('archieved_numerator_s', 0).fillna(0) > 0) | (df_source.get('target_numerator_s', 0).fillna(0) > 0)
    df_source = df_source[mask_bigger_0].drop_duplicates(['INDICATOR_CODE_NAME', 'PROJECT_CODE'], keep='first')

    # agregate numerator/denominator of DPMS. so male/female = total
    df_DPMS = ( df_DPMS.groupby(['INDICATOR_CODE_NAME', 'PROJECT_CODE', 'FREQUENCY'], as_index=False).agg({
        'NUMERATOR': 'sum',
        'DENOMINATOR': 'sum'
    }))

    
    # merge source and dpms
    cols = [
        'INDICATOR_CODE_NAME','PROJECT_CODE',
        'archieved_numerator_s','archieved_denominator_s',
        'target_numerator_s','target_denominator_s'
    ]

    cols_exist = [col for col in cols if col in df_source.columns]
    df_compare = df_DPMS.merge(df_source[cols_exist], on=['INDICATOR_CODE_NAME', 'PROJECT_CODE'], how='left')

    # look in the frequency to know when when to take the numerator/denominator from archieved or target
    numerator_source = []
    denominator_source = []

    for _, row in df_compare.iterrows():
        # so it's a target row
        if row['FREQUENCY'].strip().lower() == 'annual/target report':
            numerator_source.append(row['target_numerator_s'])
            denominator_source.append(row['target_denominator_s'])
        # so it's a archieved row
        else:
            numerator_source.append(row['archieved_numerator_s'])
            denominator_source.append(row['archieved_denominator_s'])

    df_compare['Numerator_source'] = numerator_source
    df_compare['Denominator_source'] = denominator_source

    # create new colums of their difference
    df_compare['diff_num_s_dpms'] = df_compare['Numerator_source'] - df_compare['NUMERATOR']
    df_compare['diff_den_s_dpms'] = df_compare['Denominator_source'] - df_compare['DENOMINATOR']

    top_num_s, top_den_s = spot_check(df_compare, 50, "diff_num_s_dpms", "diff_den_s_dpms")

    return top_num_s, top_den_s




def compare_target_dpms(df_target_value, df_DPMS):

    # take the first number greater than 0 for numerator
    mask_bigger_0 = df_target_value['Numerator_target'] > 0
    df_target_value = df_target_value[mask_bigger_0].drop_duplicates(['INDICATOR_CODE_NAME', 'PROJECT_CODE', 'RESULT_NAME'], keep='first')

    # need to get the code correctly
    df_target_value["PROJECT_CODE"] = df_target_value["PROJECT_CODE"].str.split("-", n=4).str[:4].str.join("-")

    # agregate numerator/denominator of DPMS. so male/female = total
    df_DPMS = ( df_DPMS.groupby(['INDICATOR_CODE_NAME', 'PROJECT_CODE', 'RESULT_NAME'], as_index=False).agg({
        'NUMERATOR': 'sum',
        'DENOMINATOR': 'sum'
    }))

    # merge source and dpms
    df_compare = df_target_value.merge(df_DPMS, on=['INDICATOR_CODE_NAME', 'PROJECT_CODE', 'RESULT_NAME'], how='outer')

    # create new colums of their difference
    df_compare['diff_num_t_dpms'] = df_compare['Numerator_target'] - df_compare['NUMERATOR']
    df_compare['diff_den_t_dpms'] = df_compare['Denominator_target'] - df_compare['DENOMINATOR']

    top_num_t, top_den_t = spot_check(df_compare, 50, "diff_num_t_dpms", "diff_den_t_dpms")

    return top_num_t, top_den_t



def compare_source_target(df_source, df_target_value):

     # get the last modified time as a column 
    df_source['LastAchievedModifiedDate'] = pd.to_datetime(df_source['LastAchievedModifiedDate'], errors='coerce')

    # put the most recent first then delete the others, so we take the most recent row in source
    df_source = df_source.sort_values('LastAchievedModifiedDate', ascending=False)

    # take the first number greater than 0 for numerator in archieved or target
    mask_bigger_0 = (df_source.get('archieved_numerator_s', 0).fillna(0) > 0) | (df_source.get('target_numerator_s', 0).fillna(0) > 0)
    df_source = df_source[mask_bigger_0].drop_duplicates(['INDICATOR_CODE_NAME', 'PROJECT_CODE'], keep='first')

    # need to get the code correctly
    df_target_value["PROJECT_CODE"] = df_target_value["PROJECT_CODE"].str.split("-", n=4).str[:4].str.join("-")

    # merge source and dpms
    df_compare = df_source.merge(df_target_value, on=['INDICATOR_CODE_NAME', 'PROJECT_CODE'], how='outer')

    # create new colums of their difference
    df_compare['diff_num_st_dpms'] = df_compare['target_numerator_s'] - df_compare['Numerator_target']
    df_compare['diff_den_st_dpms'] = df_compare['target_denominator_s'] - df_compare['Denominator_target']

    top_num_st, top_den_st = spot_check(df_compare, 50, "diff_num_st_dpms", "diff_den_st_dpms")

    return top_num_st, top_den_st



def spot_check(df, n, num_diff, den_diff): # to get a sanple of n 
    # spot checks  
    spot_random = df.sample(min(n, len(df)),)

    # tests for numerator
    # get top n rows with biggest difference between numerator in source vs target
    top_num =  df.assign(abs_diff=df[num_diff].abs()).sort_values("abs_diff", ascending=False, na_position="last").head(n)
    # tests for denominator
    top_den=  df.assign(abs_diff=df[den_diff].abs()).sort_values("abs_diff", ascending=False, na_position="last").head(n)

    return top_num, top_den


# verify what colums are in the result data
def rd_cols(df_rd):
    cols = df_rd.schema.names
    print("result data colums: \n")
    print(cols)
    return cols


def write_excel(top_num, top_den, num_name, den_name):
    
    # making an excel of top 50 biggest difference numerator
     top_num.to_excel(num_name, index=False)
     # making an excel of top 50 biggest difference denominator 
     top_den.to_excel(den_name, index=False)
    

# Main function to execute the stored procedure.
def main(session: snowpark.Session):
   

    print('Read Data from Snowflake')
    df_source, df_actuals, df_target_value, df_DPMS = read_snowflake(session)

    # see colums in DPMS
    #rd_cols(df_DPMS)


    print('transform pandas')
    df_DPMS_pd = df_DPMS.to_pandas()

    print('rename colums')
    df_source, df_target_value, df_actuals, df_DPMS_pd = renaming_colums(df_source, df_target_value, df_actuals, df_DPMS_pd)

    print('remove the rows that have the same indicator code name but different indicator code ')
    df_source = remove_different_IndicatorCode(df_source)

    print('creating the data frames for the spot checks comparing source and DPMS')
    top_num_s, top_den_s = compare_source_dpms(df_source, df_DPMS_pd)
    
    print("Create the excel files for source v DPMS")
    write_excel(top_num_s, top_den_s, "./data/tests/spot_top50_sn_DPMS.xlsx", "./data/tests/spot_top50_sd_DPMS.xlsx")

    print('creating the data frames for the spot checks comparing target and DPMS')
    top_num_t, top_den_t = compare_target_dpms(df_target_value, df_DPMS_pd)

    print("Create the excel files for target v DPMS")
    write_excel(top_num_t, top_den_t, "./data/tests/spot_top50_tn_DPMS.xlsx", "./data/tests/spot_top50_td_DPMS.xlsx")

    print('creating the data frames for the spot checks comparing actual and DPMS')
    top_num_a, top_den_a = compare_target_dpms(df_actuals, df_DPMS_pd)
    write_excel(top_num_a, top_den_a, "./data/tests/spot_top50_an_DPMS.xlsx", "./data/tests/spot_top50_ad_DPMS.xlsx")


    print('creating the data frames for the spot checks comparing source and target')
    top_num_st, top_den_st = compare_source_target(df_source, df_target_value)
    write_excel(top_num_st, top_den_st, "./data/tests/spot_top50_stn_DPMS.xlsx", "./data/tests/spot_top50_std_DPMS.xlsx")

