with rrtreasons as (SELECT ce.clinical_event_id as RRT_ce_id, ce.event_tag as rrt_reason, ce.event_end_dt_tm
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
, (ce.event_end_dt_tm - 13*3600000) as timestart
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
;