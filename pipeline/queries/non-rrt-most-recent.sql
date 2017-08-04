
SELECT * FROM (

SELECT 
	non_rrts.encntr_id
  , ce.event_cd
  , ce.result_val
  , DENSE_RANK() OVER(partition by ce.encntr_id, ce.event_cd ORDER BY ce.event_end_dt_tm DESC) as dr 


FROM (
    SELECT enc.encntr_id
    , round(enc.arrive_dt_tm + (enc.depart_dt_tm-enc.arrive_dt_tm)/2) as not_rrt_time
    , round(enc.arrive_dt_tm + (enc.depart_dt_tm-enc.arrive_dt_tm)/2) - 46800000 as timestart
    , round(enc.arrive_dt_tm + (enc.depart_dt_tm-enc.arrive_dt_tm)/2) - 3600000 as timeend
    , year(now()) - year(from_unixtime(CAST(p.birth_dt_tm/1000 as bigint))) AS age 
    , CASE p.sex_cd WHEN '362' then 'F' ELSE 'M' END as sex
    , cvr.description as race

    FROM encounter enc
    INNER JOIN person p on p.person_id = enc.person_id
    LEFT OUTER JOIN code_value cvr ON cvr.code_value = p.race_cd
    
    WHERE enc.depart_dt_tm - enc.arrive_dt_tm > 93600000
    AND enc.admit_type_cd != '0'
    AND enc.encntr_type_class_cd = '391'
    AND enc.loc_facility_cd='633867'
    AND encntr_complete_dt_tm < 4e12
    AND enc.encntr_id NOT IN (  
        SELECT enc.encntr_id FROM encounter enc 
        INNER JOIN clinical_event ce ON enc.encntr_id = ce.encntr_id 
        WHERE enc.loc_facility_cd='633867' AND enc.encntr_complete_dt_tm < 4e12  
        AND ce.event_cd='54411998' 
        AND ce.result_status_cd NOT IN ('31', '36')  
        AND ce.valid_until_dt_tm > 4e12  
        AND ce.event_class_cd not in ('654645') 
        AND enc.admit_type_cd !='0' 
        AND enc.encntr_type_class_cd='391'  
                            )
    ORDER BY enc.encntr_id
    LIMIT 2700
    ) non_rrts

LEFT JOIN clinical_event ce
ON ce.encntr_id = non_rrts.encntr_id
AND ce.event_end_dt_tm > non_rrts.timestart
AND ce.event_end_dt_tm < non_rrts.timeend
AND ce.event_cd IN ('703501', '703516', '2700541', '703306', '703558', '703540',
                    '3623994', '703511', '4690633', '703565') 

) enc_vals
WHERE dr = 1
;