# Standard library imports
import math
import os
import sys
import time
import traceback
import warnings
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd
import pytz
from snowflake.snowpark import Session



# changed ? : yes, 
# being used ? : yes, in the function : calculate_M_F_META and Meta
def allocate_difference(df):
    
    # take only the colums that we will need
    cols = ['girls','boys','women','men']
    df_demo = df[cols].astype(float) # make sure they are floats

    # Calculate the total before any rounding and the difference between rounded and no rounded
    # grab the total sum not rounded 
    pre_roundedsum = df_demo.sum(axis=1)
    # get fractional part
    diff = pre_roundedsum - np.floor(pre_roundedsum)
    
    # If % contribution was applied, we round. If the value is below 1, we make it 1
    pre_roundedsum = np.where(pre_roundedsum < 1, 1, np.where(diff < 0.0001, np.floor(pre_roundedsum) , np.ceil(pre_roundedsum)))

    # round the values
    # Calculate difference between RoundedSum and PreRoundedSum
    pos_roundedsum = df_demo.round(0) # it already has the actual values rounded 
    diff = pre_roundedsum - pos_roundedsum.sum(axis=1)

    # create a priority list : Prioritize G/B/W/M and allocate the  difference to that bucket
    # so create masks. if first is untrue, then second is tested and so on. 
    girl_pick  = df_demo['girls'].ne(0) # true if girl != 0
    boy_pick   = ~girl_pick & df_demo['boys'].ne(0) # true if girl = 0 and boys != 0 and so on. 
    women_pick =  ~boy_pick & df_demo['women'].ne(0)
    men_pick   = ~women_pick & df_demo['men'].ne(0)

    # create and populate new version of data frame to return 
    # Note that we look at the value as calculated in the dataframe that was sent,
    # as we may have a situation where the rounded figure is 0.
    agregate = pd.DataFrame(0.0, index=df.index, columns=cols)
    agregate.loc[girl_pick, 'girls'] = diff[girl_pick] # it takes only the rows where girl pick is true
    agregate.loc[boy_pick, 'boys'] = diff[boy_pick]
    agregate.loc[women_pick, 'women'] = diff[women_pick]
    agregate.loc[men_pick, 'men'] = diff[men_pick]

    # put together the values of agregate and pos_roundedsum
    output = (agregate + pos_roundedsum)[cols]

    return output

# changed ? : yes
# being used ? : yes quite a lot

def newBracket_vectorized(df, df_agemap, df_agelist, brackets):
    """
    Vectorized version of the newBracket function.
    
    Args:
        df (pd.DataFrame): Input DataFrame with columns 'age_group', 'shift', and 'unique'.
        df_agemap (pd.DataFrame): DataFrame containing age group mappings.
        df_agelist (pd.DataFrame): DataFrame containing age brackets.
        brackets (list): List of age brackets to process.
    
    Returns:
        pd.DataFrame: Transformed DataFrame with calculated relative values for each bracket.
    """
    # create a copy
    df = df.copy()


    # Map start and end groups using df_agemap
    df['start'] = df['age_group'].map(
        lambda age: df_agemap.loc[df_agemap.age_group.eq(age), 'start_group'].iloc[0]
        if age in df_agemap.age_group.values else float(age.split('-')[0])
    ) + df['shift']
    
    df['end'] = df['age_group'].map(
        lambda age: df_agemap.loc[df_agemap.age_group.eq(age), 'end_group'].iloc[0]
        if age in df_agemap.age_group.values else float(age.split('-')[1])
    ) + df['shift']
    
    # Cap start and end values
    df['start'] = df['start'].clip(upper=99)
    df['end'] = df['end'].clip(upper=100)
    
    # Create a DataFrame for cumulative and relative calculations
    results = {}
    for bracket in brackets:
        # grab the start and length of the bracket
        start_bracket = df_agelist.loc[df_agelist.bracket.eq(bracket), 'start_bracket'].iloc[0]
        length_bracket = df_agelist.loc[df_agelist.bracket.eq(bracket), 'length_bracket'].iloc[0]
        
        # Calculate cumulative values
        end_bracket = start_bracket + length_bracket
        # the if-else code in where.
        df['cumulative'] = np.where(start_bracket < df['start'], 
                                    np.where(df['end'] < (end_bracket + 1), df['end'] - df['start'], 0),
                                    np.where(df['end'] > (end_bracket), length_bracket , 
                                              np.where((df['end'] - start_bracket) > 0, df['end'] - start_bracket, 0 )))

        # Add results for this bracket
        results[bracket] = df['cumulative']


    # make a data frame with results    
    df_results = pd.DataFrame(results, index=df.index)
    ratio = df_results.sum(axis=1)
    relatives= df_results.div(ratio, axis=0).fillna(0)

    # exceptions
    non_people = df['age_group'].isin(["Not Applicable", "N/A", "Non-people"]) # mask
    relatives.loc[non_people] = 0

    # Merge all bracket results
    final_df = df[['age_group', 'shift', 'unique']].join(relatives)
    return final_df

