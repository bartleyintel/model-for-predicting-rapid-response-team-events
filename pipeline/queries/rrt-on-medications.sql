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

 , CASE WHEN
     mdx.multum_category_id = '261' OR
     mdx.multum_category_id = '262' OR
     mdx.multum_category_id = '283' OR
     mdx.multum_category_id = '285' THEN 1
     ELSE 0 END AS 'anticoagulants'

 , CASE WHEN mdx.multum_category_id = '60' THEN 1 ELSE 0 END AS 'narcotics'

 , CASE WHEN mdx.multum_category_id = '191' THEN 1 ELSE 0 END AS 'narc_ans'

 , CASE WHEN
     mdx.multum_category_id = '77' OR
     mdx.multum_category_id = '210' OR
     mdx.multum_category_id ='251' OR
     mdx.multum_category_id ='341' THEN 1
     ELSE 0 END AS 'antipsychotics'

 , CASE WHEN
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