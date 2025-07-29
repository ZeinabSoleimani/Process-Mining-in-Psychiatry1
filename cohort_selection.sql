---------------------------------------------------------------------
--@block
-- Create the table
CREATE TABLE soleiz01.AD_ancestors as (
    select
        concept_id,
        concept_code,
        concept_name
    from 
        CONCEPT
    where 
        (concept_id in(
            '797617',
            '19010886',
            '715259',
            '797617',
            '715939',
            '755695',
            '751412',
            '714684',
            '722031',
            '40234834',
            '19080226',
            '715259',
            '743670',
            '717607',
            '43560354',
            '19084693',
            '703547',
            '725131',
            '44507700',
            '40234834',
            '750982',
            '36878783',
            '19084693',
            '778268',
            '721724',
            '710062',
            '738156',
            '716968',
            '754270',
            '705755',
            '794147',
            '713109',
            '703470',
            '733896',
            '781705',
            '766209',
            '19010652',
            '757688',
            '766814',
            '19124477',
            '19017241',
            '1366610',
            '785649'
        ) AND vocabulary_id = 'RxNorm' AND domain_id = 'Drug')
)
--------------------------------------------------------------------- 
--@blockd 
CREATE TABLE SOLEIZ01.AD_DESCENDANTs AS( 
    SELECT   
        AL.concept_id as  ANCESTOR_CONCEPT_ID,
        AL.concept_code as  ANCESTOR_CONCEPT_code,
        AL.concept_name as  ANCESTOR_CONCEPT_name,
        CA.descendant_concept_id ,
        cp.concept_name as descendant_concept_name,
        cp.concept_class_id as descendant_concept_class_id,
        cp.vocabulary_id as descendant_vocabulary_id
    FROM CONCEPT_ANCESTOR AS CA 
    JOIN SOLEIZ01.AD_ancestors as AL
    ON CA.ANCESTOR_CONCEPT_ID = AL.CONCEPT_ID
    JOIN CONCEPT as cp
    ON CA.descendant_concept_id = cp.concept_id
); 
---------------------------------------------------------------------
--@block 
CREATE TABLE soleiz01.condition_ancestor_id_list AS (
    SELECT 
        cpt.concept_id       as ancestor_concept_id,
        cpt.concept_code     as ancestor_concept_code,
        cpt.concept_class_id as ancestor_concept_class_id,
        cpt.vocabulary_id    as vocabulary_id,
        cpt.concept_name     as ancestor_concept_name
    FROM concept as cpt
    WHERE cpt.concept_id IN ( 
                '432883', '433440', '433991', '434911', '435220', '435520',  
                '438406', '438727', '438998', '440383', '441534', '4025677',  
                '4049623', '4077577', '4098302', '4141454', '4195572',  
                '4228802', '4263748', '4282096', '4282316', '4323418'
            )
            AND cpt.vocabulary_id = 'SNOMED'
);
----------------------------------------------------------------------- 
 --@block 
-- Select all diagnosis descendant concepts to retrieve them from the concept table
CREATE TABLE soleiz01.condition_descendant_id_list AS(
    SELECT DISTINCT 
        cail.ancestor_concept_id,
        cail.ancestor_concept_name,
        cpt_rel.descendant_concept_id,
        cpt.concept_name              AS descendant_concept_name,
        cpt.concept_class_id          AS descendant_concept_class_id,
        cpt.vocabulary_id             AS descendant_vocabulary_id
    FROM concept_ancestor AS cpt_rel
    INNER JOIN soleiz01.condition_ancestor_id_list  AS cail
    ON cail.ancestor_concept_id = cpt_rel.ancestor_concept_id
    INNER JOIN concept AS cpt    
    ON cpt.concept_id = cpt_rel.descendant_concept_id
);
 ---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.first_visit as (
    SELECT 
        DISTINCT person_id,
        MIN(visit_start_date) as first_visit_date
    FROM visit_occurrence
    GROUP BY person_id
); 
---------------------------------------------------------------------"
--@block
CREATE TABLE soleiz01.first_condition as (
    SELECT 
        DISTINCT person_id,
        MIN(condition_start_date) as first_condition_date
    FROM condition_occurrence
    GROUP BY person_id
);
---------------------------------------------------------------------"
--@block
CREATE TABLE soleiz01.first_records_new AS (
    SELECT 
        COALESCE(fc.person_id, fv.person_id) AS person_id,
        fc.first_condition_date,
        fv.first_visit_date,
        COALESCE(fc.first_condition_date, fv.first_visit_date) AS first_record_date
    FROM 
        soleiz01.first_condition fc
    FULL OUTER JOIN 
        soleiz01.first_visit fv
    ON 
        fc.person_id = fv.person_id
);
---------------------------------------------------------------------"
--@block
--patients with MDD diagnoses, providing the earliest diagnosis date for each relevant condition per person
CREATE TABLE soleiz01.mdd_person AS (
    SELECT DISTINCT 
        co.PERSON_ID,
        co.CONDITION_CONCEPT_ID,
        co.CONDITION_CONCEPT_NAME,
        MIN(co.CONDITION_START_DATE) AS FIRST_DIAGNOSIS_DATE
    FROM 
        CONDITION_OCCURRENCE co
    INNER JOIN 
        soleiz01.condition_descendant_id_list cdil
    ON 
        cdil.descendant_concept_id = co.CONDITION_CONCEPT_ID
    GROUP BY
        co.PERSON_ID, co.CONDITION_CONCEPT_ID, co.CONDITION_CONCEPT_NAME
);
---------------------------------------------------------------------"
--@block
CREATE TABLE soleiz01.first_record_mdd_patients AS (
    WITH ranked_mdd AS (
        SELECT 
            mp.person_id,
            mp.condition_concept_id,
            mp.CONDITION_CONCEPT_NAME,
            mp.FIRST_DIAGNOSIS_DATE AS first_mdd_diagnosis,
            fr.first_record_date,
            DAYS_BETWEEN(fr.first_record_date, mp.FIRST_DIAGNOSIS_DATE) AS days_diff_diagnosis_and_first_record,
            ROW_NUMBER() OVER (PARTITION BY mp.person_id ORDER BY mp.FIRST_DIAGNOSIS_DATE ASC) AS rn
        FROM 
            soleiz01.mdd_person mp
        JOIN 
            soleiz01.first_records_new fr
        ON 
            mp.person_id = fr.person_id
    )
    SELECT 
        person_id, 
        condition_concept_id, 
        CONDITION_CONCEPT_NAME, 
        first_mdd_diagnosis, 
        first_record_date, 
        days_diff_diagnosis_and_first_record
    FROM 
        ranked_mdd
    WHERE  
        rn = 1
);
---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.mdd_with_history AS(
    SELECT 
        *
    FROM 
        soleiz01.first_record_mdd_patients
    WHERE 
        days_diff_diagnosis_and_first_record >=365
);
---------------------------------------------------------------------
--@block
--drug exposure records for all descendants concept IDs.
CREATE TABLE soleiz01.AD_expo_All_patients AS(
    SELECT 
        de.*
    FROM
        drug_exposure de
    JOIN
        soleiz01.new_descendants dl
    ON
        de.drug_concept_id = dl.descendant_concept_id
);
---------------------------------------------------------------------
--@block
--AD_expo_mdd_patients for mdd_with_history
CREATE TABLE soleiz01.AD_expo_mdd_patients AS(
    SELECT 
        Ad_all.*
    FROM
        soleiz01.ad_expo_All_patients Ad_all
    JOIN
        soleiz01.mdd_with_history mwh
    ON
        mwh.person_id = Ad_all.person_id
    where
        Ad_all.xtn_pharmaceutical_class_source_concept_name NOT IN(
            'INVESTIGATIONAL',
            'ANTIPRURITICS,TOPICAL',
            'GENERAL ANESTHETICS,INJECTABLE',
            'SMOKING DETERRENTS, OTHER',
            'No matching concept',
            'ANTIPARKINSONISM DRUGS,OTHER',
            'SEDATIVE-HYPNOTICS,NON-BARBITURATE',
            'INVESTIGATIONAL'
        )
);
---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.first_ad_exposure_mdd AS(
    SELECT 
        person_id, 
        drug_concept_id, 
        drug_concept_name, 
        use_case, 
        first_ad_exposure, 
        first_mdd_diagnosis, 
        DAYS_BETWEEN(first_mdd_diagnosis, first_ad_exposure) AS days_diff_diagnosis_and_aad_exposure
    FROM (
        SELECT 
            admdd.person_id,
            admdd.drug_concept_id,
            admdd.drug_concept_name,
            admdd.xtn_therapeutic_class_source_concept_name AS use_case,
            admdd.drug_exposure_start_date AS first_ad_exposure,
            mwh.first_mdd_diagnosis,
            ROW_NUMBER() OVER (PARTITION BY admdd.person_id ORDER BY admdd.drug_exposure_start_date ASC) AS row_num
        FROM 
            soleiz01.AD_expo_mdd_patients admdd
        JOIN 
            soleiz01.mdd_with_history mwh
        ON 
            mwh.person_id = admdd.person_id
       ) filtered
    WHERE row_num = 1
    ORDER BY person_id
);
---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.final_cohort AS(
    SELECT DISTINCT 
        mwh.person_id
    FROM 
        soleiz01.mdd_with_history mwh
    LEFT JOIN 
        soleiz01.new_first_ad_exposure_mdd fpae
    ON 
        mwh.person_id = fpae.person_id
    WHERE 
        fpae.person_id IS NULL OR fpae.days_diff_diagnosis_and_aad_exposure > -30
    ORDER BY 
        mwh.person_id
);
---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.ad_exposed_pid AS(
    SELECT distinct fcm.person_id
    FROM soleiz01.final_cohort fcm
    JOIN drug_exposure de
    ON fcm.person_id = de.person_id
    WHERE 
        de.drug_concept_id IN(
            SELECT distinct descendant_concept_id
            FROM SOLEIZ01.new_DESCENDANTs
        ) 
        AND
            de.xtn_pharmaceutical_class_source_concept_name NOT IN(
                'INVESTIGATIONAL',
                'ANTIPRURITICS,TOPICAL',
                'GENERAL ANESTHETICS,INJECTABLE',
                'SMOKING DETERRENTS, OTHER',
                'No matching concept',
                'ANTIPARKINSONISM DRUGS,OTHER',
                'SEDATIVE-HYPNOTICS,NON-BARBITURATE',
                'INVESTIGATIONAL'
            )
        AND 
            de.xtn_therapeutic_class_source_concept_name = 'PSYCHOTHERAPEUTIC DRUGS'
);
---------------------------------------------------------------------
--@block
drop table soleiz01.AD_records_subcohort;
---------------------------------------------------------------------
--@block
CREATE TABLE soleiz01.AD_records_subcohort AS(
    Select 
        adpid.person_id,
        drug_concept_name,
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_datetime,
        drug_exposure_end_date,
        drug_exposure_id,
        stop_reason,
        drug_type_concept_name, 
        refills,
        quantity,
        days_supply,
        route_concept_id,
        route_source_value,
        dose_unit_source_value
    from 
        soleiz01.new_AD_expo de
    join
        soleiz01.ad_exposed_pid adpid
    on
        adpid.person_id = de.person_id
    WHERE
        drug_concept_name!= 'No matching concept'
);
---------------------------------------------------------------------
--@block
SELECT count(DISTINCT person_id)
FROM soleiz01.AD_records_subcohort;