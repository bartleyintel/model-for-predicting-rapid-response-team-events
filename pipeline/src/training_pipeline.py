from sys import argv
import pandas as pd
import numpy as np
from impala.util import as_pandas
from impala.dbapi import connect

# These two libraries are the custom ones
from pre_processing import *
from my_impala_utils import create_modeling_table, insert_pandas_to_impala

# Get input parameters
assert argv[4], "You must pass the directory where the Impala queries are stored."
assert argv[5] or argv[6], "You must provide either a name for a Hive table or CSV file to write the ouput to."
cluster_host, port_num, db_name = argv[1].split('=')[1], argv[2].split('=')[1], argv[3].split('=')[1] 
dir_sql_queries, output_table_name, csv_file_name = argv[4].split('=')[1], argv[5].split('=')[1], argv[6].split('=')[1]

# Setup cluster connection and Impala cursor
conn = connect(host=cluster_host, port=port_num)
CUR = conn.cursor()
CUR.execute("USE {0}".format(db_name))

# Specify paths to RRT queries
rrt_queries = \
[
"{0}/rrt-info.sql".format(dir_sql_queries),
"{0}/rrt-most-recent-chart-value.sql".format(dir_sql_queries),
"{0}/rrt-avg-chart-value.sql".format(dir_sql_queries),
"{0}/rrt-patient-characteristics.sql".format(dir_sql_queries),
"{0}/rrt-on-medications.sql".format(dir_sql_queries)
]
    
# Specify paths to NonRRT queries
non_rrt_queries = \
[
"{0}/non-rrt-most-recent.sql".format(dir_sql_queries),
"{0}/non-rrt-avg-chart-value.sql".format(dir_sql_queries),
"{0}/non-rrt-patient-characteristics.sql".format(dir_sql_queries),
"{0}/non-rrt-on-medications.sql".format(dir_sql_queries)
]

# The following two dictionaries map Cerner event codes to human readable column names.
avg_col_names = {   
    '2700541': "avgHR",      
    '703540':  "avgRR",
    '703306':  "avgMAP",
    '703516':  "avgDBP",
    '703558':  "avgTemp",
    '703565':  "avgGCS",
    '703501':  "avgSBP",
    '703511':  "avgPeriPR",
    '3623994': "avgSpO2",
    '4690633': "avgCO2"
    }

recent_col_names = {   
    '2700541': "recentHR",
    '703540':  "recentRR",
    '703306':  "recentMAP",
    '703516':  "recentDBP",
    '703558':  "recentTemp",
    '703565':  "recentGCS",
    '703501':  "recentSBP",
    '703511':  "recentPeriPR",
    '3623994': "recentSpO2",
    '4690633': "recentCO2"
    }

# This is the schema that we want to write to Hive using Impala. Modify as desired.
schema = {
    "age": "float",
    "anticoagulants": "float",
    "antipsychotics": "float",
    "avgDBP": "float",
    "avgMAP": "float",
    "avgPeriPR": "float",
    "avgRR": "float",
    "avgSBP": "float",
    "avgSpO2": "float",
    "avgTemp": "float",
    "bu_nal": "float",
    "chemo": "float",
    "dialysis": "float",
    "narc_ans": "float",
    "narcotics": "float",
    "obese": "float",
    "on_iv": "float",
    "prev_rrt": "float",
    "recentDBP": "float",
    "recentMAP": "float",
    "recentPeriPR": "float",
    "recentRR": "float",
    "recentSBP": "float",
    "recentSpO2": "float",
    "recentTemp": "float",
    "sex": "float",
    "smoker": "float",
    "rrt_reason": "string"
}

# Start a timer
time_start = time.time()

# Query Impala for data on RRT events
df_rrt_info = impala_to_pandas(rrt_queries[0], CUR)
df_rrt_recent = impala_to_pandas(rrt_queries[1], CUR)
df_rrt_avg = impala_to_pandas(rrt_queries[2], CUR)
df_rrt_char = impala_to_pandas(rrt_queries[3], CUR)
df_rrt_meds = impala_to_pandas(rrt_queries[4], CUR)

# Query Impala for data on nonRRT events
df_recent = impala_to_pandas(non_rrt_queries[0], CUR)
df_avg = impala_to_pandas(non_rrt_queries[1], CUR)
df_char = impala_to_pandas(non_rrt_queries[2], CUR)
df_meds = impala_to_pandas(non_rrt_queries[3], CUR)

# Preprocess and combine the dataframes for the RRT events
rrt_info = remove_duplicate_rrt_events(df_rrt_info)
rrt_recent = pre_process_most_recent(df_rrt_recent, recent_col_names, is_negative_class=False)
rrt_avg = pre_process_avg_values(df_rrt_avg, avg_col_names, is_negative_class=False)
rrt_chars = pre_process_characteristics(df_rrt_char, is_negative_class=False)
rrt_meds = pre_process_medications(df_rrt_meds)
rrt = combine_frames([rrt_info, rrt_recent, rrt_avg, rrt_chars, rrt_meds], is_negative_class=False)

# Preprocess and combine the dataframes for the nonRRT events
recent = pre_process_most_recent(df_recent, recent_col_names, is_negative_class=True)
avg = pre_process_avg_values(df_avg, avg_col_names, is_negative_class=True)
chars = pre_process_characteristics(df_char, is_negative_class=True)
meds = pre_process_medications(df_meds)
non_rrt = combine_frames([recent, avg, chars, meds], is_negative_class=True)

# Add the RRT reason and number of previous RRT columns for nonRRT events
non_rrt.loc[:, 'rrt_reason'] = "no rrt"
non_rrt.loc[:, 'prev_rrt'] = non_rrt['prev_rrt'].fillna(0)

# Drop the columns that are not needed for training
cols_to_drop = ['rrt_ce_id', 'event_end_dt_tm', 'timestart', 'timeend' ]
rrt = rrt.drop(cols_to_drop, axis=1)
# Check that both the RRT event and nonRRT events have the same columns prior to combining
assert sorted(rrt.columns) == sorted(non_rrt.columns), "The columns are not in alignment or the names do not match."
cols = sorted(rrt.columns)
df1 = rrt[cols]
df2 = non_rrt[cols]
df_master = pd.concat([df1, df2])

# Drop the columns that are mostly missing values.
to_drop = ["race", "avgCO2", "recentCO2", "avgHR","recentHR", 
           "avgGCS", "recentGCS", "encntr_id"]
df_master = df_master.drop(to_drop, axis=1)

# Fill the missing values and drop duplicates
df_final = fill_missing(df_master).drop_duplicates()

n_rows, n_cols = df_final.shape[0], df_final.shape[1]
print "The processed data contains {0} rows and {1} columns.".format(n_rows, n_cols)
print "\n"

if csv_file_name:
    print "writing dataframe to {0}".format(csv_file_name)
    df_final.to_csv(csv_file_name, index=False)

if output_table_name:
    print "Creating Hive table named: {0}".format(output_table_name)
    print "\n"
    create_modeling_table(df_final, output_table_name, schema, CUR, drop_if_exists=True)
    print "Writing dataframe to {0}.".format(output_table_name)
    print "\n"
    insert_pandas_to_impala(df_final, output_table_name, CUR)

time_end = time.time()
print "Total pipeline run time was {0}".format(time_end - time_start)
