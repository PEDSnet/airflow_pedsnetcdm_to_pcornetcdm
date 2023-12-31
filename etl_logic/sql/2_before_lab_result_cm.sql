
begin;
create table {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements as
(
  select measurement_id, person_id, visit_occurrence_id, measurement_concept_id, measurement_source_Concept_id, measurement_source_value, measurement_order_date, measurement_order_datetime,  measurement_datetime, measurement_date, measurement_Result_date, measurement_result_datetime,value_as_number, range_low, range_high, unit_source_value, unit_concept_id, m.value_as_concept_id,measurement_type_concept_id, priority_source_value, range_high_source_value, range_low_source_value, operator_concept_id,range_low_operator_concept_id, range_high_operator_concept_id, value_source_value, measurement_concept_name, measurement_source_concept_name,  measurement_type_concept_name,priority_concept_name, range_high_operator_concept_name, range_low_operator_concept_name, specimen_concept_name,unit_concept_name, value_as_concept_name, site, site_id, provider_id, operator_concept_name, priority_concept_id, specimen_source_value, specimen_concept_id, c1.concept_code as lab_loinc_vocab,c1.concept_name as loinc_desc, raw_loinc.concept_code as raw_lab_loinc_vocab,raw_loinc.concept_name as raw_loinc_desc,
	case when range_high_operator_concept_id = range_low_operator_concept_id  then 'EQ|EQ'
	when range_low_operator_concept_id in (4171754,4172703,4171755) and 
	range_high_operator_concept_id in (4172703,4171755,4171754,4171756) then 'GE|NO'
	when range_low_operator_concept_id in (4171754,4171756) and 
	range_high_operator_concept_id in (4172703,4171755,4171756) then 'GT|NO'
	when range_low_operator_concept_id in (4171755,4172704) and 
	range_high_operator_concept_id in (4172703,4171756) then 'NO|LE'
	when range_low_operator_concept_id in (4172703,4172704,4171754,4171756) and 
	range_high_operator_concept_id in (4172704) then 'NO|LT'
	else hi.target_concept||'|'||lo.target_concept end as modifier
  from {{ dag_run.conf['site'] }}_pedsnet.measurement m
  inner join vocabulary.concept c1 on m.measurement_concept_id = c1.concept_id and c1.vocabulary_id = 'LOINC' and concept_class_id='Lab Test'
  left join vocabulary.concept raw_loinc on m.measurement_source_concept_id = raw_loinc.concept_id and raw_loinc.vocabulary_id = 'LOINC' and raw_loinc.concept_class_id='Lab Test'
  left join pcornet_maps.pedsnet_pcornet_valueset_map hi on hi.source_concept_id = m.range_high_operator_concept_id::text and hi.source_concept_class = 'Result modifier'
  left join pcornet_maps.pedsnet_pcornet_valueset_map lo on lo.source_concept_id = m.range_low_operator_concept_id::text and lo.source_concept_class = 'Result modifier'
  where measurement_type_Concept_id = 44818702 and measurement_concept_id>0 
  and m.visit_occurrence_id IN (select visit_id from {{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001)
  and EXTRACT(YEAR FROM m.measurement_date)>=2001
 );
commit;
begin;
CREATE INDEX idx_labms_visitid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements USING btree
    (visit_occurrence_id)
    TABLESPACE pg_default;
commit;
begin;
CREATE INDEX idx_labms_obsconid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements USING btree
    (operator_concept_id)
    TABLESPACE pg_default;
commit;
begin;
CREATE INDEX idx_labms_raglwid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements USING btree
    (range_low_operator_concept_id)
    TABLESPACE pg_default;
commit;
begin;
CREATE INDEX idx_labms_raghiid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements USING btree
    (range_high_operator_concept_id)
    TABLESPACE pg_default;
commit;
begin;
CREATE INDEX idx_labms_valconid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements USING btree
    (value_as_concept_id)
    TABLESPACE pg_default;
commit;

begin;
create table {{ dag_run.conf['site'] }}_pcornet_airflow.specimen_values as
select distinct on (measurement_id) measurement_id, person_id,visit_occurrence_id,measurement_concept_id, measurement_date, modifier, measurement_datetime, measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, priority_concept_id, priority_source_value, range_high, range_high_operator_concept_id, range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, specimen_source_value, unit_concept_id, unit_source_value, m.value_as_concept_id, value_as_number, value_source_value, measurement_concept_name, measurement_source_concept_name, measurement_type_concept_name, operator_concept_name, priority_concept_name, range_high_operator_concept_name, range_low_operator_concept_name, specimen_concept_name, unit_concept_name, value_as_concept_name, site, site_id, provider_id, loinc_desc as raw_lab_name,lab_loinc_vocab as lab_loinc, raw_lab_loinc_vocab as raw_lab_code, coalesce(c.target_concept, s.target_concept,'OT') as specimen_source
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_measurements m
left join pcornet_maps.pedsnet_pcornet_valueset_map c on c.source_concept_id = m.specimen_concept_id::text and c.source_concept_class = 'Specimen concept'
left join pcornet_maps.pedsnet_pcornet_valueset_map s on s.source_concept_id = m.lab_loinc_vocab and s.source_concept_class = 'specimen_loinc';
commit;

