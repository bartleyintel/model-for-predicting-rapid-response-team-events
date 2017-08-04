import pandas as pd
import numpy as np
import time
import datetime as datetime
from impala.util import as_pandas

# The following dictionaries map Cerner event codes to human readable column names.
#avg_col_names = {   
#    '2700541': "avgHR",      
#    '703540':  "avgRR",
#    '703306':  "avgMAP",
#    '703516':  "avgDBP",
#    '703558':  "avgTemp",
#    '703565':  "avgGCS",
#    '703501':  "avgSBP",
#    '703511':  "avgPeriPR",
#    '3623994': "avgSpO2",
#    '4690633': "avgCO2"
#    }

#recent_col_names = {   
#    '2700541': "recentHR",
#    '703540':  "recentRR",
#    '703306':  "recentMAP",
#    '703516':  "recentDBP",
#    '703558':  "recentTemp",
#    '703565':  "recentGCS",
#    '703501':  "recentSBP",
#    '703511':  "recentPeriPR",
#    '3623994': "recentSpO2",
#    '4690633': "recentCO2"
#    }

def impala_to_pandas(path_to_query, CUR):
    """
    Input:        Path or filename of a SQL query (string), Impala cursor (connection cursor object)
    Output:       Pandas dataframe
    Description:  Reads a SQL query from a file, passes the query to Impala and returns the results
                  as a Pandas dataframe.
    """

    # Read a sql file into a string
    with open(path_to_query, 'rb') as f:
        query_string = f.read().replace('\n', ' ')
    # Connect to the cluster
    # Start timer
    t_start = time.time()
    CUR.execute(query_string)
    # Convert results of Impala query to a pandas dataframe 
    df = as_pandas(CUR)
    # End timer
    t_end = time.time()
    print "{0} rows and {1} columns cast to Pandas dataframe.".format(df.shape[0], df.shape[1])
    print "Time elapsed: {0} seconds.".format(t_end - t_start)
    print "\n"
    return df

def make_time_readable(time_string):
    """
    Input:        Unix epoch timestamp (string)
    Output:       Human readable date time (string)
    Description:  Takes the Unix timestamp used in Cerner and converts it to a human
                  readable format.
    """

    time = int(time_string) / 1000.
    return datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

def remove_duplicate_rrt_events(df):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Find the events that are duplicated. Duplicates are defined by 
                  the same RRT ID and the same RRT reason showing up more than once.
    """

    # Find the duplicated RRT events
    vc = df.rrt_ce_id.value_counts() 
    rrt = vc[vc > 1]
    dups = df[df.rrt_ce_id.isin(rrt.index)]
    non_dups = df[~df.rrt_ce_id.isin(rrt.index)]
    # Iterate through the reasons for each RRT id and concat the reasons
    # together for each duplicated RRT id.
    res = {}
    rrt_ids = dups.rrt_ce_id.unique()
    # Get the reasons and key them to each RRT id
    for rrt_id in rrt_ids:
        res[rrt_id] = []
        reasons = dups['rrt_reason'][dups.rrt_ce_id == rrt_id]
        for reason in reasons:
            if reason not in res[rrt_id]: 
                res[rrt_id].append(reason)
    # With the reasons preserved, we can remove duplicates           
    dups = dups.drop('rrt_reason', axis=1)
    deduped = dups.drop_duplicates()
    deduped.loc[:, 'rrt_reason'] = "no rrt"
    rrt_reasons = []
    for row in deduped.iterrows():
        rrt_reasons.append(" | ".join(res[row[1][0]]))    
    deduped.loc[:, 'rrt_reason'] = rrt_reasons
    # We now have removed duplicated rrt_events but combined the reasons 
    # from the duplicated event ids into a single reason
    # Combine the rrt_events into a single df again
    return pd.concat([deduped, non_dups])

def pre_process_most_recent(df, column_name_mappings, is_negative_class=False):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Group most recent values from chart data on a unique identifying key.

    			  1. Deduplicate repeated rows.
                  2. Cast data to numeric.
                  3. Pivot the dataframe on an identifying column. Pivot column is 'encounter_id' 
                     for patients without an RRT event (is_negative_class=True) or pivot column 
                     is 'rrt_ce_id' for patients with an RRT event (is_negative_class=False).
                  4. Map the cerner event codes to human readable columns (see column mappings at the top of the file).   
    """

    if is_negative_class:
        df = df.drop('dr', axis=1)
        pivot_index = 'encntr_id'
        df.columns = ["encntr_id", "event_cd", "recent_result"]
        deduped = df
    else: 
        pivot_index = 'rrt_ce_id'
        df.columns = ["encntr_id" ,"rrt_ce_id", "event_cd",	"recent_result"]
        deduped = df.drop('encntr_id', axis=1).drop_duplicates()
    # A number of results contain a blank space or NULL-- filter those out
    most_recent = deduped[(deduped.recent_result != " ") &
                           (deduped.recent_result.notnull())]
    # Cast the string values to numeric
    most_recent.loc[:, 'recent_result'] = most_recent['recent_result'].astype(float)
    # Pivot on the RRT event id such that the columns are the event codes and 
    # the values are the numeric values
    df_return = most_recent.pivot_table(values='recent_result', 
                                        index=pivot_index, 
                                        columns='event_cd')
    # Change the column names from event codes to human readable names
    df_return.columns = [column_name_mappings[col] for col in df_return.columns]
    return df_return

def pre_process_avg_values(df, column_name_mappings, is_negative_class=False):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Group all average columns values from chart data on a unique identifying key.

    			  1. Drop rows with null values.
                  2. Pivot the dataframe on an identifying column. Pivot column is 'encounter_id' 
                     for patients without an RRT event (is_negative_class=True) or pivot column 
                     is 'rrt_ce_id' for patients with an RRT event (is_negative_class=False).
                  3. Map the cerner event codes to human readable columns (see column mappings at the top of the file).    
    """


    # Filter out null valued columns
    avg_values = df[df.avg_result.notnull()]
    # Pivot on the RRT event id such that the columns are the event codes and 
    # the values are the numeric values
    if is_negative_class:
        pivot_index = 'encntr_id'
    else:
        pivot_index = 'rrt_ce_id'
    df_return = avg_values.pivot_table(values='avg_result', 
                                        index=pivot_index,
                                       columns='event_cd')
    # Change the column names from event codes to human readable names
    df_return.columns = [column_name_mappings[col] for col in df_return.columns]
    return df_return

def pre_process_characteristics(df, is_negative_class=False):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Merge all rows with patient characteristics (age, sex, race) into a single row, keyed by 
                  a unique identifier. 

                  1. Pivot the dataframe on an identifying column. Pivot column is 'encounter_id' 
                     for patients without an RRT event (is_negative_class=True) or pivot column 
                     is 'rrt_ce_id' for patients with an RRT event (is_negative_class=False).    
    """

    if is_negative_class:
        cols = ['age', 'sex', 'race']
        vals = df[['encntr_id'] + cols].drop_duplicates()
        # These are the people who did not have RRT events. We key them by encounter id
        grouped = df.drop(cols, axis=1).groupby('encntr_id').sum()
        res = grouped.merge(vals, how='left', right_on='encntr_id', left_index=True)
        return res
    return df.drop('encntr_id', axis=1).groupby('rrt_ce_id').sum()

def pre_process_medications(df):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Group all rows with patient characteristics (age, sex, race) into a single row, keyed by 
                  'encounter_id'. The nueric value represents the number of times a type of medication was
                  administered to a patient.    
    """
    
    # The query only pulls medication data from the 13 hours prior to an RRT event
    # Or 13 hours prior to the midpoint of an encounter for a nonRRT encounter
    meds = df.drop(['multum_category_id', 'orig_order_dt_tm'], axis=1)
    return meds.groupby('encntr_id').sum()

def combine_frames(frames, is_negative_class=False):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Concatenate all the dataframes together. When the input frames are for patients
                  without RRT events use is_negative_class=False, otherwise use is_negative_class=True.
    """

    if is_negative_class:
        assert len(frames) == 4, "4 dataframes are required for the negative class (i.e. nonRRT events)."
        most_recent, avg_values, chars, meds = frames[0], frames[1], frames[2], frames[3]
        int_1 = most_recent.merge(avg_values, how='left', left_index=True, right_index=True)
        int_2 = chars.merge(int_1, how='left', left_on='encntr_id', right_index=True)
        return int_2.merge(meds, how='left', left_on='encntr_id', right_index=True)
    assert len(frames) == 5, "5 dataframes are required for the positive class (i.e. RRT events)."
    reasons, most_recent, avg_values, chars, meds = frames[0], frames[1], frames[2], frames[3], frames[4]
    intermediate_1 = reasons.merge(most_recent, how='left', left_on='rrt_ce_id', right_index=True)
    intermediate_2 = intermediate_1.merge(avg_values, how='left', left_on='rrt_ce_id', right_index=True)
    intermediate_3 = intermediate_2.merge(chars, how='left', left_on='rrt_ce_id', right_index=True)
    return intermediate_3.merge(meds, how='left', left_on='encntr_id', right_index=True)

def fill_missing(df):
    """
    Input:        Pandas dataframe
    Output:       Pandas dataframe
    Description:  Impute missing values. Modify as needed.
    """

    # Fill these NA categoricals with 0
    fillna_with_zero = ["anticoagulants", "antipsychotics" , "bu_nal",            
                        "chemo", "narc_ans", "narcotics", 
                        "obese", "on_iv", "prev_rrt", "smoker"]
    df[fillna_with_zero] = df[fillna_with_zero].fillna(0)
    # Fill with a string
    df['rrt_reason'] = df['rrt_reason'].fillna("Staff Concern/Unknown -- Imputation")
    # Cast this binary string to 0 or 1
    df['sex'] = df['sex'].apply(lambda x: 1 if 'M' else 0)
    #fill with mean
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            mean_val = df[col].mean()
            df[col] = df[col].fillna(mean_val)
    return df