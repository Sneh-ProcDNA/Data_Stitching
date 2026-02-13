WITH base AS (
    SELECT  
        skcb.*,
        REPLACE(
            REPLACE(sd.primary_icd_code, '.', ''),
            '-', ''
        ) AS icd_code,
        LEFT(sd.primary_icd_code,3) as parent
    FROM data_dev.sp_komodo_core_base skcb
    LEFT JOIN data_dev.sp_data sd  
        ON skcb.sp_patient_id = sd.patient_id  	
    WHERE sd.primary_icd_code IS NOT NULL
),
 
diag_check AS (
    SELECT DISTINCT
        a.patient_id,
        b.sp_patient_id,
        count(distinct a.medical_event_id) as diag_frq
    FROM data_dev.mx_core_table a
    JOIN base b
        ON a.patient_id = b.matched_patient_id
       AND a.diagnosis_codes ILIKE '%' || b.icd_code || '%'
       
       group by 1,2
),
 
parent_check as (
 
    SELECT DISTINCT
        a.patient_id,
        b.sp_patient_id
    FROM data_dev.mx_core_table a
    JOIN base b
        ON a.patient_id = b.matched_patient_id
       AND a.diagnosis_codes ILIKE '%' || b.parent || '%'
)
 
 
 
 
 
 
 
SELECT
    skcb.*,
    CASE
        WHEN dc.sp_patient_id IS NOT NULL THEN 1
        ELSE 0
    END AS diag_match_flag,
    case
    	when concat(skcb.sp_patient_id, skcb.matched_patient_id) in (select concat(sp_patient_id, patient_id) from parent_check) then 1
    	else 0
    end as parent_flag
    ,
    dc.diag_frq
FROM data_dev.sp_komodo_core_base skcb
LEFT JOIN diag_check dc
    ON skcb.sp_patient_id = dc.sp_patient_id
   AND skcb.matched_patient_id = dc.patient_id