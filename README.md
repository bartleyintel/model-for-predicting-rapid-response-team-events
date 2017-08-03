# sharp-patient-risk-poc

# Overview

### Below is the file structure of the tech-transfer

```
tech-transfer
|   - README.md
│   - OVERVIEW.ipynb
|   - Sharp-Intel-Technology-Transfer-Workshop.pdf
|   - sharppatientrisk.yml
|   - datamodel.zip
│
└───pipeline
|    |
|    └───src
|    |     - pre_processing.py
|    |     - my_impala_utils.py
|    |     - training_pipeline.py
|    |
|    └───queries
|         - rrt-info.sql
|         - rrt-most-recent-chart-value.sql
|         - rrt-avg-chart-value.sql
|         - rrt-patient-characteristics.sql
|         - rrt-on-medications.sql
|         - non-rrt-most-recent.sql
|         - non-rrt-avg-chart-value.sql
|         - non-rrt-patient-characteristics.sql
|         - non-rrt-on-medications.sql
|
└───notebooks
|   |   
│   └───analytics_helpers
|   │     - analytics_helpers.py
|   |
│   └───modeling
|   │     - modeling_base.ipynb
|   |     - modeling_diff_algorithms.ipynb
|   |     - RunModelOnExamplePatients.ipynb
│   │
│   └───EDA
│         - encounter_durations[EDA].ipynb
│         - explore_vitals_by_encounter[EDA].ipynb
|         - medications[EDA].ipynb
|         - multi_rrts[EDA].ipynb
|         - probe_encounter_types_classes[EDA].ipynb
|         - rrt_reasons[EDA].ipynb
│         - vitals_avg_over_visit[EDA].ipynb
│   
└───etl-queries
       - Compare_arrival_depart_times.sql.txt
       - Count_MedCategory.sql.txt
       - demo_scores.sql.txt
       - demo_scores_with_changes.sql.txt
       - DrugCategories.sql.txt
       - DrugName_to_DrugCategory.sql.txt
       - encounter_location_history.sql.txt
       - encounter_location_history_pairs.sql.txt
       - med_hist_encntr_med_admin.sql.txt
       - med_hist_encntr_med_admin_hr_cnt.sql.txt
       - med_hist_RRT_event.sql.txt
       - med_hist_RRT_event_distinct_med_hr_bucket.sql.txt
       - med_hist_RRT_event_med_hr_bucket.sql.txt
       - med_hist_RRT_non-event.sql.txt
       - med_hist_RRT_non-event_distinct_med_hr_bucket.sql.txt
       - med_hist_RRT_non-event_med_hr_bucket.sql.txt
       - MostFrequentVitalsWLoc.sql.txt
       - PersonQuery_KnownPersonID.sql.txt
```

## Approach to work
We typically explored the data using the Impala query editor in Hue. Once the data of interest were initially identified, we then ran such queries and worked with the results in jupyter notebooks.

datamodel.zip is a zip file of the Cerner data dictionary.

# notebooks

### This section talks about what is in each of the notebooks and why we did what we did

## analytics_helpers (subfolder)
Contains analytics_helpers.py

#### analytics_helpers.py
A python library that contains useful helper functions for exploratory analysis, data cleaning, and visualization.

## modeling (subfolder)
Contains notebooks which cover the creation of the predictive model and cross validation.

#### modeling_base.ipynb
The main notebook for modeling.

#### modeling_diff_algorithms.ipynb
Exploring different modeling algorithms -- for reference only


#### RunModelOnExamplePatients.ipynb
Extracts a small subset of patients, collects their statistics into a modeling tables based on different timeframes, loads the saved model, uses model to generate risk scores, then writes the scores and modeling tables to

#### gbc_base.compressed
Note: The trained model was removed from the public facing repo.
The saved model file, in sklearn's [joblib](http://scikit-learn.org/stable/modules/model_persistence.html) format.



## EDA (subfolder)
Contains notebooks which cover Exploratory Data Analysis of the data.

#### encounter_durations[EDA].ipynb
Explores encounter durations for patients with and without RRT events. Explores subselection of patients without RRT events who have similar encounter durations to patients with RRT events.

#### explore_vitals_by_encounter[EDA].ipynb
Creates time series of vitals signs for RRT patients which indicate time of RRT.

#### medications[EDA].ipynb
Explores the number of patients taking different kinds of medications, and how that breaks down for patients with and without RRT events

#### multi_rrts[EDA].ipynb
Explores patients with multiple RRT events. Only text output.

#### probe_encounter_types_classes[EDA].ipynb
Examine breakdowns of different patient/encounter types. Only text output.

#### rrt_reasons[EDA].ipynb
Explore the reasons for RRT events & their frequencies

#### vitals_avg_over_visit[EDA].ipynb
Compare if patients with RRTs have different average vitals than patients without RRTs, visually.

## Features used & mapping to Cerner records (data dictionary)
- Join clinical_event values to code_value table to see description
- Some modeling features reference multiple Cerner fields
- We create separate "recent" and "average" features for vitals signs taken during the time frame of interest.

| feature description (final feature in model) | feature type| Cerner table | Cerner field |
|---|---|---|---|
| Mean Arterial Pressure (MAP)| vital sign | clinical_event | event_cd = 703306 |
| Systolic Blood Pressure (SBP)| vital sign | clinical_event | event_cd = 703501 |
| Peripheral Pulse Rate (pulse)|  vital sign |clinical_event | event_cd = 703511 |
| Diastolic Blood Pressure (DPB)|  vital sign |clinical_event | event_cd = 703516 |
| Respiratory Rate (RR)|  vital sign |clinical_event | event_cd = 703540 |
| Temperature Oral (temp) | vital sign | clinical_event | event_cd = 703558 |
| Height/length (obese) | vital sign | clinical_event | event_cd = 2700653 |
| SpO2 (SPO2)| vital sign | clinical_event | event_cd = 3623994 |
| Measured Weight (obese) |  vital sign |clinical_event | event_cd = 4674677 |
| smoking code (smoker)| patient info | clinical_event | event_cd = 75144985 |
| On IV indicator (on_iv)| patient info | clinical_event | event_cd = 679984 |
| buprenorphine-naloxone (bu-nal) | medication | clinical_event | event_cd = 2797130 |
| naloxone (bu-nal)| medication | clinical_event | event_cd = 2798305 |
| buprenorphine (bu-nal)| medication | clinical_event | event_cd = 2797129 |
| narcotic analgesic (narcotics) | medication | mltm_drug_categories | multum_category_id = 60 |
| narcotic analgesic combination (narcotics) | medication | mltm_drug_categories | multum_category_id = 191 |
| antipsychotics (antipsychotics)| medication | mltm_drug_categories | multum_category_id = 77, 210, 251, 341 |
| chemo drugs (chemo) | medication | mltm_drug_categories | multum_category_id = 20, 21, 22, 23, 24, 25, 26 |
| anticoagulants (anticoagulants) | medication | mltm_drug_categories | multum_category_id = 261, 262, 283, 285 |
| age | patient info | person | age |
| sex (is_male)| patient info | person | sex |



# ETL-queries

### We used the Impala Editor via Cloudera Hue to run queries on and explore the data. Queries which were not included in the notebooks are saved in this folder.
#### Queries are saved as .txt so files will open on jupyter.

### Saved Queries

| Query (file) name  | Description | Subject |
|-------------|-------------|-------------------------|
||||
| <b>encounter_location_history</b> | Show history of changes to patient location or level of care. | location_history |
| <b>encounter_location_history_pairs</b> | Show the distinct pairings of [from > to] locations. | location_history |
||||
| <b>med_hist_encntr_med_admin</b> | Associate medication with ordinal hour of administration within an encounter. | med_history |
| <b>med_hist_encntr_med_admin_hr_cnt</b> | Count number of medication administrations in each ordinal hour of encounter. | med_history |
|<b>med_hist_encntr_distinct_med_admin_hr_cnt</b>|Count number of distinct medications administered in each ordinal hour of encounter.| med_history|
| <b>med_hist_RRT_event</b> | Associate RRT event with ordinal hour of occurrence within an encounter. |med_history|
| <b>med_hist_RRT_non-event</b> | Associate non-RRT-event with ordinal hour of occurrence within an encounter. | med_history|
| <b>med_hist_RRT_event_med_hr_bucket</b> | Count number of medication administrations in 10 hourly buckets leading up to event| med_history |
|<b>med_hist_RRT_non-event_med_hr_bucket</b> | Count number of medication administrations in 10 hourly buckets leading up to non-event. | med_history|
| <b>med_hist_RRT_event_distinct_med_hr_bucket</b> | Count number of distinct medications administered in 10 hourly buckets leading up to event. | med_history |
| <b>med_hist_RRT_non-event_distinct_med_hr_bucket</b> |Count number of distinct medications administered in 10 hourly buckets leading up to non-event. | med_history |
||||
|<b>demo_scores</b> | Join rows in scoring table to associated encounter and patient. | demo |
| <b>demo_scores_with_changes</b> | Show changes in score and feature values across sequential rows in scoring table. | demo |
||||
|<b>MostFrequentVitalsWLoc</b> | Returns counts for potentially useful vitals signs | vitals |
|<b> DrugName_to_DrugCategory</b> | Return drug id and drug category given a partial drug name | drugs |
|<b> DrugCategories</b> | Show all the different drug categorizations | drugs |
|<b> Count_MedCategory</b> | Count the number of encounters where patients are taking various drug classes | drugs |
|<b> Compare_arrival_depart_times</b> | Output the different timestamps associated with an encounter | time |
|<b> PersonQuery_KnownPersonID</b> | Return info related to person, given a personid | person info |


# Environment and notes

##### Environment and install: We recommmend users install the [Anaconda scientific python distribution](https://www.continuum.io/downloads). We used python v2.7. We relied on the [impyla](https://github.com/cloudera/impyla/blob/master/README.md) and [ibis](http://www.ibis-project.org/) packages to pull data from HDFS to the jupyter notebook, and to write back to the tables. Other dependencies include: pandas, numpy, matplotlib, scikit-learn, cPickle, and seaborn. The dependencies are included in the "sharppatientrisk.yml" environment file. The environment can be loaded by the command:
```conda env create -f sharppatientrisk.yml```

##### - The times of RRT events (and all events from the clinical_event table) was recorded in the field "event_end_dt_tm" in the clinical_event table. This is an example where it is very important to have good relationships with your subject matter experts. This field records when the event took place, not the time of the end of the event.

##### - We discovered partway through the process that not all arrival time information was recorded consistently in the encounters table. Sometimes, "arrival_dt_tm" field in the encounters table was overwritten with the time a patient became an inpatient in the facility, rather than the true time of arrival. To get true time of arrival, we need to join to the tracking_item and tracking_checkin tables. Below is an example of querying for the encounter id and the true arrival time. The MIN in the subquery is to select only one timestamp, as some records contained duplicate entries. The difference in arrival time may or may not be relevant to the question at hand.

```
SELECT enc.encntr_id, COALESCE(tci.checkin_dt_tm, enc.arrive_dt_tm) AS check_in_time
FROM encounter enc
INNER JOIN clinical_event ce
ON ce.encntr_id = enc.encntr_id
LEFT OUTER JOIN  ( SELECT ti.encntr_id AS encntr_id, MIN(tc.checkin_dt_tm)  AS checkin_dt_tm
    FROM tracking_item ti
  JOIN tracking_checkin  tc ON  ti.tracking_id  = tc.tracking_id
GROUP BY ti.encntr_id ) tci
ON tci.encntr_id = enc.encntr_id```
