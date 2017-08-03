# This is an effort to rewrite the functions that take a very long time to create the modeling table (create_modeling_table.ipynb). These are Impala snippets that can be used as an alternative to the very slow function pull_and_writedata_2query.


## Query to pull together people/rrt info and rrt description  (previously 2 queries):

```sql
'''with rrtreasons as (SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
  FROM encounter enc
  INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
  WHERE
  enc.loc_facility_cd='633867'
  AND enc.encntr_complete_dt_tm < 4e12  
  AND ce.event_cd='54408578'
  AND ce.result_status_cd NOT IN ('31', '36')  
  AND ce.valid_until_dt_tm > 4e12  
  AND ce.event_class_cd not in ('654645')
  AND enc.admit_type_cd !='0'
  AND enc.encntr_type_class_cd='391'
  )
SELECT ce.clinical_event_id as RRT_ce_id,
enc.encntr_id, ce.event_end_dt_tm
, (ce.event_end_dt_tm - {0}*3600000) as timestart
, (ce.event_end_dt_tm - 1*3600000) as timeend
, year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
, CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
, cvr.description as race
, rrtreasons.rrt_reason as rrt_reason
FROM encounter enc
INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
INNER JOIN person p ON p.person_id = enc.person_id
LEFT OUTER JOIN rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
WHERE
enc.loc_facility_cd='633867'
AND enc.encntr_complete_dt_tm < 4e12  
AND ce.event_cd='54411998'
AND ce.result_status_cd NOT IN ('31', '36')  
AND ce.valid_until_dt_tm > 4e12  
AND ce.event_class_cd not in ('654645')
AND enc.admit_type_cd !='0'
AND enc.encntr_type_class_cd='391' ORDER BY enc.encntr_id, event_end_dt_tm
;'''.format(interval_hr)
```

-----
# Rewriting the long queries that goes by each encounter to aggregate data below. Here, we're doing the aggregation in Impala. Each of these queries should return the relevant patient data and the RRT information. You can use these queries as a starting off point for refactoring the existing code.


### For Max (numeric):

Get the dense rank over the partitions then take the value of the top result (descending)

```sql

SELECT encntr_id
 , rrt_ce_id
 , event_cd
 , result_val as recent_result

FROM
( with rrt_shell as (
   with rrtreasons as (
    SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
    FROM encounter enc
    INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
    WHERE
    enc.loc_facility_cd='633867'
    AND enc.encntr_complete_dt_tm < 4e12  
    AND ce.event_cd='54408578'
    AND ce.result_status_cd NOT IN ('31', '36')  
    AND ce.valid_until_dt_tm > 4e12  
    AND ce.event_class_cd not in ('654645')
    AND enc.admit_type_cd !='0'
    AND enc.encntr_type_class_cd='391'
                    )
  SELECT ce.clinical_event_id as RRT_ce_id,
  enc.encntr_id, ce.event_end_dt_tm
  , (ce.event_end_dt_tm - 12*3600000) as timestart
  , (ce.event_end_dt_tm - 1*3600000) as timeend
  , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
  , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
  , cvr.description as race
  , rrtreasons.rrt_reason as rrt_reason
  FROM encounter enc
  INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
  INNER JOIN person p ON p.person_id = enc.person_id
  LEFT OUTER JOIN rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
  LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
  WHERE
  enc.loc_facility_cd='633867'
  AND enc.encntr_complete_dt_tm < 4e12  
  AND ce.event_cd='54411998'
  AND ce.result_status_cd NOT IN ('31', '36')  
  AND ce.valid_until_dt_tm > 4e12  
  AND ce.event_class_cd not in ('654645')
  AND enc.admit_type_cd !='0'
  AND enc.encntr_type_class_cd='391'
       )
SELECT
rrt_shell.encntr_id
, rrt_shell.rrt_ce_id
 ,ce.event_end_dt_tm
, ce.event_cd
, ce.result_val      
  , DENSE_RANK() OVER(partition by ce.encntr_id, ce.event_cd, rrt_shell.rrt_ce_id order by ce.event_end_dt_tm DESC) as dr
, rrt_shell.rrt_reason
FROM rrt_shell
JOIN clinical_event ce
ON ce.encntr_id=rrt_shell.encntr_id

WHERE ce.event_end_dt_tm > rrt_shell.timestart
AND ce.event_end_dt_tm < rrt_shell.timeend

AND ce.event_cd IN ('703501', '703516', '2700541', '703306', '703558', '703540',
                 '3623994', '703511', '4690633', '703565')

) t

WHERE dr = 1
;
```

produces:
  encntr_id	rrt_ce_id	 event_cd	recent_result
1	100009623	6758153566	4690633	22
2	100009623	6758153566	703306	84
3	100009623	6758153566	703511	88

---


### For Avg:  
Will need to massage & join in pandas, but should do most of the heavy lifting for averages. Note -- the time frame is hard coded here.

```sql
SELECT DISTINCT encntr_id, rrt_ce_id, event_cd, avg_result
FROM
( with rrt_shell as (
 with rrtreasons as (
    SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
    FROM encounter enc
    INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
    WHERE
    enc.loc_facility_cd='633867'
    AND enc.encntr_complete_dt_tm < 4e12  
    AND ce.event_cd='54408578'
    AND ce.result_status_cd NOT IN ('31', '36')  
    AND ce.valid_until_dt_tm > 4e12  
    AND ce.event_class_cd not in ('654645')
    AND enc.admit_type_cd !='0'
    AND enc.encntr_type_class_cd='391'
                    )
  SELECT ce.clinical_event_id as RRT_ce_id,
  enc.encntr_id, ce.event_end_dt_tm
  , (ce.event_end_dt_tm - 12*3600000) as timestart
  , (ce.event_end_dt_tm - 1*3600000) as timeend
  , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
  , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
  , cvr.description as race
  , rrtreasons.rrt_reason as rrt_reason
  FROM encounter enc
  INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
  INNER JOIN person p ON p.person_id = enc.person_id
  LEFT OUTER JOIN rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
  LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
  WHERE
  enc.loc_facility_cd='633867'
  AND enc.encntr_complete_dt_tm < 4e12  
  AND ce.event_cd='54411998'
  AND ce.result_status_cd NOT IN ('31', '36')  
  AND ce.valid_until_dt_tm > 4e12  
  AND ce.event_class_cd not in ('654645')
  AND enc.admit_type_cd !='0'
  AND enc.encntr_type_class_cd='391'
         )
SELECT
rrt_shell.encntr_id
, rrt_shell.rrt_ce_id
--  ,ce.event_end_dt_tm
, ce.event_cd
, ce.result_val
    , AVG(CAST(ce.result_val as float)) OVER(partition by ce.encntr_id, ce.event_cd, rrt_shell.rrt_ce_id) as avg_result
    , rrt_shell.rrt_reason
FROM rrt_shell
JOIN clinical_event ce
ON ce.encntr_id=rrt_shell.encntr_id
WHERE ce.event_end_dt_tm > rrt_shell.timestart
AND ce.event_end_dt_tm < rrt_shell.timeend
AND ce.event_cd IN ('703501', '703516', '2700541', '703306', '703558', '703540',
                 '3623994', '703511', '4690633', '703565')
) t
;
```

----

## For binary values (Male or not, obese or not, etc)


```sql
SELECT DISTINCT encntr_id, rrt_ce_id, event_cd, avg_result
FROM
( with rrt_shell as (
 with rrtreasons as (
    SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
    FROM encounter enc
    INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
    WHERE
    enc.loc_facility_cd='633867'
    AND enc.encntr_complete_dt_tm < 4e12  
    AND ce.event_cd='54408578'
    AND ce.result_status_cd NOT IN ('31', '36')  
    AND ce.valid_until_dt_tm > 4e12  
    AND ce.event_class_cd not in ('654645')
    AND enc.admit_type_cd !='0'
    AND enc.encntr_type_class_cd='391'
                    )
  SELECT ce.clinical_event_id as RRT_ce_id,
  enc.encntr_id, ce.event_end_dt_tm
  , (ce.event_end_dt_tm - 12*3600000) as timestart
  , (ce.event_end_dt_tm - 1*3600000) as timeend
  , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
  , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
  , cvr.description as race
  , rrtreasons.rrt_reason as rrt_reason
  FROM encounter enc
  INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
  INNER JOIN person p ON p.person_id = enc.person_id
  LEFT OUTER JOIN rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
  LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
  WHERE
  enc.loc_facility_cd='633867'
  AND enc.encntr_complete_dt_tm < 4e12  
  AND ce.event_cd='54411998'
  AND ce.result_status_cd NOT IN ('31', '36')  
  AND ce.valid_until_dt_tm > 4e12  
  AND ce.event_class_cd not in ('654645')
  AND enc.admit_type_cd !='0'
  AND enc.encntr_type_class_cd='391'
         )
SELECT
rrt_shell.encntr_id
, rrt_shell.rrt_ce_id
--  ,ce.event_end_dt_tm

CASE ce.event_cd
  WHEN '679984' IS NOT NULL THEN 1 else 0
  END AS 'on_iv'

CASE WHEN
       ce.event_cd = '2797130' IS NOT NULL OR  -- #OR or AND? pretty sure it's or
       ce.event_cd = '2798305' IS NOT NULL OR
       ce.event_cd = '2797129' IS NOT NULL
       THEN 1 ELSE 0
  END AS 'bu-nal'

CASE ce.event_cd
 WHEN '186470117' IS NOT NULL THEN 1 ELSE 0 END AS 'dialysis'

CASE ce.event_cd WHEN
  '75144985' = 'Heavy tobacco smoker' THEN 1
  '75144985' = 'Light tobacco smoker' THEN 1
  '75144985' = 'Current every day smoker' THEN 1
  '75144985' = 'Smoker, current status unknown' THEN 1
  ELSE 0
  END AS 'smoker'

CASE ce.event_cd WHEN '54411998' IS NOT NULL THEN 1 else 0 END AS 'prev_rrt'

-- NOTE: WILL NEED TO THINK ABOUT HOW TO CALCULATE height/weight... TO DO!!!
CASE WHEN
  ce.event_cd = '2700653' IS NULL THEN 0
  ce.event_cd = '4674677' IS NULL THEN 0


FROM rrt_shell
JOIN clinical_event ce
ON ce.encntr_id=rrt_shell.encntr_id
WHERE ce.event_end_dt_tm > rrt_shell.timestart
AND ce.event_end_dt_tm < rrt_shell.timeend
AND ce.event_cd IN ('679984', '2797130','2798305', '2700653', '4674677', '4686698', '679984', '2797130','2798305', '2797129', '75144985', '54411998', '2700653', '4674677', '3618608', '186470117')
) t
;
```


-----

## And lastly, medications

```sql
with rrt_shell as (
 with rrtreasons as (
    SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
    FROM encounter enc
    INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
    WHERE
    enc.loc_facility_cd='633867'
    AND enc.encntr_complete_dt_tm < 4e12  
    AND ce.event_cd='54408578'
    AND ce.result_status_cd NOT IN ('31', '36')  
    AND ce.valid_until_dt_tm > 4e12  
    AND ce.event_class_cd not in ('654645')
    AND enc.admit_type_cd !='0'
    AND enc.encntr_type_class_cd='391'
                    )
  SELECT ce.clinical_event_id as RRT_ce_id,
  enc.encntr_id, ce.event_end_dt_tm
  , (ce.event_end_dt_tm - 12*3600000) as timestart
  , (ce.event_end_dt_tm - 1*3600000) as timeend
  , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
  , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
  , cvr.description as race
  , rrtreasons.rrt_reason as rrt_reason
  FROM encounter enc
  INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id
  INNER JOIN person p ON p.person_id = enc.person_id
  LEFT OUTER JOIN rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
  LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
  WHERE
  enc.loc_facility_cd='633867'
  AND enc.encntr_complete_dt_tm < 4e12  
  AND ce.event_cd='54411998'
  AND ce.result_status_cd NOT IN ('31', '36')  
  AND ce.valid_until_dt_tm > 4e12  
  AND ce.event_class_cd not in ('654645')
  AND enc.admit_type_cd !='0'
  AND enc.encntr_type_class_cd='391'
         )
 SELECT rrt_shell.encntr_id, mdx.multum_category_id, orig_order_dt_tm

 CASE WHEN
     mdx.multum_category_id = '261' OR
     mdx.multum_category_id = '262' OR
     mdx.multum_category_id = '283' OR
     mdx.multum_category_id = '285' THEN 1
     ELSE 0 END AS 'anticoagulants',

 CASE WHEN mdx.multum_category_id = '60' THEN 1 ELSE 0 END AS 'narcotics',

 CASE WHEN mdx.multum_category_id = '191' THEN 1 ELSE 0 END AS 'narc-ans',

 CASE WHEN
     mdx.multum_category_id = '77' OR
     mdx.multum_category_id = '210' OR
     mdx.multum_category_id ='251' OR
     mdx.multum_category_id ='341' THEN 1
     ELSE 0 END AS 'antipsychotics',

 CASE mdx.multum_category_id WHEN
     mdx.multum_category_id = '20' OR
     mdx.multum_category_id = '21' OR
     mdx.multum_category_id = '22' OR
     mdx.multum_category_id = '23' OR
     mdx.multum_category_id = '24' OR
     mdx.multum_category_id = '25' OR
     mdx.multum_category_id = '26' THEN 1
     ELSE 0 END AS 'chemo'


 FROM (SELECT encntr_id, cki, substr(cki,9) as cki_id, order_id, orig_order_dt_tm FROM orders) ords
 LEFT OUTER JOIN mltm_category_drug_xref mdx ON ords.cki_id = mdx.drug_identifier
 LEFT OUTER JOIN mltm_drug_categories mdc ON mdc.multum_category_id = mdx.multum_category_id
 JOIN rrt_shell
 ON ords.encntr_id = rrt_shell.encntr_id
 WHERE mdx.multum_category_id IN ('261', '262','285', '283', '60', '191', '77', '210', '251', '341', '20', '21', '22', '23', '24', '25', '26')
 AND ords.orig_order_dt_tm < rrt_shell.timeend
 AND ords.orig_order_dt_tm > rrt_shell.timestart
  ;    

```
