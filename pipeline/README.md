# Pipeline

The pipeline builds the training dataset from the Cerner EMR system.

It is a Python script that runs a series of Impala queries to obtain the necessary data. The resulting data is processed and cleaned in Pandas before being output to either Hive or locally as a CSV file.

**Note:** The medications data are presented in the pipeline output as integer counts of the number of times a medication was taken in the 12-hour window preceeding an RRT event. This is in contrast to how we trained our model. We represented medications data as categorical variables that indicate if the particular medication had been administered or not (0 for not administered or 1 for administered). See page 6 of the implementation guide for more discussion on this topic.

# Usage

```
python pipeline/src/training_pipeline.py \
        host=mycluster.domain.com \
        port=my_impala_port_number \
        database=my_hive_db \
        query_dir=pipeline/queries \
        output_table_name=new_hive_table \
        csv_file_name=my_csv.csv
```
`host`, `port`, `database`, and `query_dir` are all required arguments. If one of arguments: `output_table_name` or `csv_file_name` is not passed then you must pass the other. The `output_table_name` argument is the name of a Hive table that you want the training data written to and `csv_file_name` is the location/name of a local file that you want the the training data written to. 

# To Do

1. Add a way to accept a Hive schema as an input, rather than having the schema hard-coded into training_pipeline.py
2. Creating a more robust argument parser to ensure the command line arguments passed to training_pipeline.py are the correct ones.
3. Parameterize the time windows of the Impala queries. Currently the queries are hard-coded to collect data from the t-minus 13 hr to the t-minus 1 hr window. This will allow users to experiment with other windows of data collection and different target windows for predicting adverse events.

