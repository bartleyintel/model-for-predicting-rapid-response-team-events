with obesity_status as

(select 
     ce.encntr_id
   , CASE WHEN ce.result_val = 'Yes' THEN 1 ELSE 0 END as 'obese'
 from clinical_event ce
 where ce.event_cd = '49810672'
)

SELECT 
    DISTINCT t.encntr_id
  , rrt_ce_id
  , on_iv
  , bu_nal
  , dialysis
  , smoker
  , CASE WHEN obese IS NULL THEN 0 ELSE obese END AS 'obese'
  , prev_rrt
FROM
( with rrt_shell as 
    (
   
    with rrtreasons as 
      (
      SELECT 
          ce.clinical_event_id as RRT_ce_id
        , ce.event_tag as rrt_reason
        , ce.event_end_dt_tm
      FROM 
        encounter enc
      INNER JOIN 
        clinical_event ce ON enc.encntr_id = ce.encntr_id
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
   
  SELECT 
      ce.clinical_event_id as RRT_ce_id
    , enc.encntr_id, ce.event_end_dt_tm
    , (ce.event_end_dt_tm - 12*3600000) as timestart
    , (ce.event_end_dt_tm - 1*3600000) as timeend
    , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age
    , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
    , cvr.description as race
    , rrtreasons.rrt_reason as rrt_reason
  FROM 
    encounter enc
  INNER JOIN 
    clinical_event ce ON enc.encntr_id = ce.encntr_id
  INNER JOIN 
    person p ON p.person_id = enc.person_id
  LEFT OUTER JOIN 
    rrtreasons ON rrtreasons.event_end_dt_tm = ce.event_end_dt_tm
  LEFT OUTER JOIN 
    code_value cvr ON cvr.code_value = p.race_cd
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
  , age
  , sex
  , race
  , CASE WHEN ce.event_cd = '679984' THEN 1 else 0 END AS 'on_iv'
  , CASE WHEN 
      ce.event_cd = '2797130' OR
      ce.event_cd = '2798305' OR 
      ce.event_cd = '2797129' 
      THEN 1 ELSE 0 END AS 'bu_nal'
  , CASE WHEN ce.event_cd = '186470117' THEN 1 ELSE 0 END AS 'dialysis'
  , CASE WHEN
      (ce.event_cd = '75144985' and ce.result_val = 'Heavy tobacco smoker') OR
      (ce.event_cd = '75144985' and ce.result_val = 'Light tobacco smoker') OR
      (ce.event_cd = '75144985' and ce.result_val = 'Current every day smoker') OR
      (ce.event_cd = '75144985' and ce.result_val = 'Smoker, current status unknown') 
      THEN 1 ELSE 0 END AS 'smoker'
  , CASE WHEN ce.event_cd = '54411998' THEN 1 ELSE 0 END AS 'prev_rrt'
  
FROM 
  rrt_shell
JOIN 
  clinical_event ce ON ce.encntr_id=rrt_shell.encntr_id
WHERE 
   ce.event_end_dt_tm < rrt_shell.timeend

AND ce.event_cd IN ('679984',  
                     '2797130',  
                     '2798305', 
                     '4686698',  
                     '2797129', 
                    '75144985', 
                    '54411998', 
                    '186470117')
) t
LEFT JOIN obesity_status os
ON t.encntr_id=os.encntr_id;