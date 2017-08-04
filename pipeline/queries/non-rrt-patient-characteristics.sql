with obesity_status AS

(SELECT
     ce.encntr_id
   , CASE WHEN ce.result_val = 'Yes' THEN 1 ELSE 0 END AS 'obese'
 FROM clinical_event ce
 WHERE ce.event_cd = '49810672'
)


SELECT DISTINCT
    chars.encntr_id
  , code
  , age
  , sex
  , race  
  , on_iv
  , bu_nal
  , dialysis
  , smoker
  , prev_rrt
  , CASE WHEN obese IS NULL THEN 0 ELSE obese END AS 'obese'

FROM (

  SELECT
      non_rrts.encntr_id
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
    , ce.event_cd as code

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
  AND ce.event_end_dt_tm < non_rrts.timeend
  AND ce.event_cd IN ('679984',  
                     '2797130', 
                     '2798305',  
                     '4686698',  
                     '2797129',  
                    '75144985',  
                    '54411998',  
                    '186470117') 
  ) chars
LEFT JOIN obesity_status os
ON chars.encntr_id=os.encntr_id
;
