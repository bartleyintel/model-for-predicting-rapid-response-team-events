import pandas as pd
import numpy as np

def create_modeling_table(df, table_name, schema, CUR, drop_if_exists=True):
    """
    Input: table_name (string), schema (dict), CUR (Impala connection cursor obj), drop_if_exists (boolean)
    Output: None
    Description: Converts dictionary key, value pairs into a SQL formatted schema.

    """
    # First ensure that the schema columns are in the same order as the dataframe columns
    ordered_pairs = []
    for col_name in df.columns:
        ordered_pairs.append(str(col_name) + ' ' + str(schema[col_name]))
    sql_formatted_schema = str(tuple(ordered_pairs)).replace("'", "")
    sql_schema = "CREATE TABLE {0} {1}".format(table_name, sql_formatted_schema)
    if drop_if_exists:
        CUR.execute("DROP TABLE IF EXISTS {0}".format(table_name))
    CUR.execute(sql_schema)
    print "{0} successfully created!".format(table_name)
    print "\n"
    return None

def insert_pandas_to_impala(df, table_name, CUR):
    """
    Input: table_name (pandas dataframe), table_name (string)
    Output: None
    Description: Formats an Impala query to bulk insert all of the dataframe rows into a Hive table.

    """
    
    insert_query = "INSERT INTO {0} VALUES".format(table_name)
    for ix, row in enumerate(df.as_matrix()):
        if ix == len(df.as_matrix()) - 1:
            insert_query += str(tuple(row))
        else:
            insert_query += str(tuple(row)) + ", "
    CUR.execute(insert_query)
    print "{0} rows inserted into Hive table {1}".format(ix + 1, table_name) 
    print "\n"
    return None