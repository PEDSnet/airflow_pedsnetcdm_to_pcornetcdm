begin; 

create table {{ dag_run.conf['site'] }}_pcornet_airflow.rx_dose_form_data as 
    with de as (
        select 
            distinct drug_concept_id
        from 
            {{ dag_run.conf['site'] }}_pedsnet.drug_exposure
        where 
            drug_type_concept_id IN (38000177, 38000180,581373) 
            and drug_concept_id > 0 
    ),
    cr as (
        select *
        from vocabulary.concept_relationship
        where relationship_id = 'RxNorm has dose form'
    )
    select  
        de.drug_concept_id, 
        c.concept_id as rx_dose_form_concept_id, 
        c.concept_name as rx_dose_form_concept_name
    from 
        de
    inner join 
        cr
        on de.drug_concept_id = cr.concept_id_1
    inner join 
        vocabulary.concept c  
        on cr.concept_id_2 = c.concept_id;

create table {{ dag_run.conf['site'] }}_pcornet_airflow.ndc_concepts as 
    select concept_code, concept_id
    from  vocabulary.concept
    where vocabulary_id='NDC';

insert into {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin (medadminid,
            patid, encounterid,
            medadmin_start_date, medadmin_start_time, medadmin_stop_date, 
            medadmin_stop_time,
            prescribingid,
            medadmin_providerid, medadmin_type,
            medadmin_code, 
            medadmin_dose_admin, medadmin_dose_admin_unit,
            medadmin_route, medadmin_source,
            raw_medadmin_med_name, 
            raw_medadmin_code, 
            raw_medadmin_dose_admin, raw_medadmin_dose_admin_unit, 
            raw_medadmin_route, site
            )
select 
	distinct drug_exposure_id as medadminid,
	cast(de.person_id as text) as patid,
	cast(de.visit_occurrence_id as text) as encounterid,
	de.drug_exposure_start_date as medadmin_start_date,
	LPAD(date_part('hour',drug_exposure_start_datetime)::text,2,'0')||':'||LPAD(date_part('minute',drug_exposure_start_datetime)::text,2,'0') as medadmin_start_time,
	de.drug_exposure_end_date as medadmin_stop_date,
	LPAD(date_part('hour',drug_exposure_end_datetime)::text,2,'0')||':'||LPAD(date_part('minute',drug_exposure_start_datetime)::text,2,'0') as medadmin_stop_time,
	null as prescribingid,
	de.provider_id as medadmin_providerid,
	coalesce(
	case 
		when ndc_via_source_concept.concept_id is not null then 'ND'
		when ndc_via_source_value.concept_id is not null then 'ND'
		when rxnorm_via_concept.concept_id > 0  then 'RX'
	end
	,'OT')
		 as medadmin_type, 
	case 
		when ndc_via_source_concept.concept_id is not null then ndc_via_source_concept.concept_code
		when ndc_via_source_value.concept_id is not null then ndc_via_source_value.concept_code
		when rxnorm_via_concept.concept_id > 0  then rxnorm_via_concept.concept_code 
	end
	as
	medadmin_code,
	de.effective_drug_dose as medadmin_dose_admin, 
	coalesce(m1.target_concept,'OT') as medadmin_dose_admin_unit,
	coalesce(m2.target_concept,'OT') as medadmin_route, 
	'OD' as medadmin_source, 
	rxnorm_via_concept.concept_name as raw_medadmin_name, 
    de.drug_concept_id as raw_medadmin_code, 
    de.eff_drug_dose_source_value as raw_medadmin_dose_admin, 
    de.dose_unit_source_value as raw_medadmin_dose_admin_unit, 
    de.route_source_value as raw_medadmin_route,
	'{{ dag_run.conf["site"] }}' as site
from 
	{{ dag_run.conf['site'] }}_pedsnet.drug_exposure de
inner join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001 pvs
	on de.visit_occurrence_id = pvs.visit_id
left join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.ndc_concepts ndc_via_source_concept 
	on ndc_via_source_concept.concept_id = drug_source_concept_id
left join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.ndc_concepts ndc_via_source_value 
	on ndc_via_source_value.concept_code = split_part(drug_source_value,'|',1)
left join 
	vocabulary.concept rxnorm_via_concept 
	on rxnorm_via_concept.concept_id = drug_concept_id 
	and vocabulary_id = 'RxNorm'
left join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.rx_dose_form_data rdf 
	on de.drug_concept_id =  rdf.drug_concept_id
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m1 
	on cast(dose_unit_concept_id as text) = m1.source_concept_id 
	and m1.source_concept_class='Dose unit'
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m2 
	on cast(de.route_concept_id as text) = m2.source_concept_id 
	and m2.source_concept_class='Route'
where
	de.drug_type_concept_id = 38000180
	and EXTRACT(YEAR FROM drug_exposure_start_date) >= 2001;
commit;

/* removing tpn and bad values from med_admin if they aren't mapped to a "Tier 1" RxNorm class */ 
begin;
with tpn as (
	select
		medadminid
		from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin n
	inner join 
		{{ dag_run.conf['site'] }}_pedsnet.drug_exposure de 
		on n.medadminid::bigint = de.drug_exposure_id
	left join 
		vocabulary.concept v
		on n.medadmin_code = v.concept_code 
		and vocabulary_id = 'RxNorm'
	where
		(
			concept_class_id not in 
			('Clinical Drug', 'Branded Drug', 'Quant Clinical Drug', 
			'Quant Branded Drug', 'Clinical Pack', 'Branded Pack')	
			or concept_class_id is null
		)
		and 
		(
			lower(drug_source_value) like '%undiluted diluent%'
			or lower(drug_source_value) like '%kcal/oz%'
			or lower(drug_source_value) like '%breastmilk%'
			or lower(drug_source_value) like '%kit%'
			or lower(drug_source_value) like '%item%'
			or lower(drug_source_value) like '%formula%'
			or lower(drug_source_value) like '%tpn%'
			or lower(drug_source_value) like '%custom%'
			or lower(drug_source_value) like '%parenteral nutrition%'
			or lower(drug_source_value) like '%breast milk%'
			or lower(drug_source_value) like '%fat emul%'
			or lower(drug_source_value) like '%human milk%'
			or lower(drug_source_value) like '%tpn%'
			or lower(drug_source_value) like '%similac%'
			or lower(drug_source_value) like '%formula%'
			or lower(drug_source_value) like '%fish oil%'
			or lower(drug_source_value) like '%omega 3%'
			or lower(drug_source_value) like '%omega3%'
			or lower(drug_source_value) like '%omega-3%'
			or lower(drug_source_value) like '%empty bag%'
			or lower(drug_source_value) like '%unable to find%'
			or lower(drug_source_value) like '%ims template%'
			or lower(drug_source_value) like '%extemporaneous template%'
			or lower(drug_source_value) like '%iv infusion builder%'
			or lower(drug_source_value) like '%patient supplied medication%'
			or lower(drug_source_value) like '%misc med%'
			or lower(drug_source_value) like '%drug formulationzor%'
			or lower(drug_source_value) like '%water for injection%'
		)
)
delete from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin
where medadminid in (select medadminid from tpn);
commit;

-- stanford map high count ingredients to clincal drugs
begin;
with herapin_1 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 1367571
	and drug_source_value ilike any
			(array[
			'%HEPARIN 1 UNIT/ML%',
			'%HEPARIN (PORCINE) 250 UNIT/250 ML (1 UNIT/ML)%'
			])
)
update {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin
set MEDADMIN_CODE = '1362025', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_1); 
commit;

begin;
with herapin_2 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 1367571
	and drug_source_value ilike '%HEPARIN 2 UNIT/ML%'
)
update {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin
set MEDADMIN_CODE = '1362935', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_2); 
commit;

begin;
with herapin_100 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 43011476
	and drug_source_value ilike '%HEPARIN 100 UNITS/ML%'
)
update {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin
set MEDADMIN_CODE = '1361048', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_100); 
commit;

begin;
with sodium_chloride_9 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id in (967823,46276153)
	and drug_source_value ilike '%SODIUM CHLORIDE 0.9%'
)
update {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin
set MEDADMIN_CODE = '313002', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from sodium_chloride_9); 
commit;