begin;
/* out of CDM ICD px codes */
delete from {{ dag_run.conf['site'] }}_pcornet.procedures
where length(px) != 7  and px_type = '10';					
commit;

begin;
delete from {{ dag_run.conf['site'] }}_pcornet.procedures
where length(px) < 5  and px_type = 'CH';					
commit;

begin;
with vals (source_concept_class,target_concept,pcornet_name,source_concept_id,concept_description, value_as_concept_id) AS (VALUES
	('vx_code_source','48','CX','40213315','HIB (PRP-T)',''),
	('vx_code_source','171','CX','40213143','INFLUENZA, INJ., MDCK, PF, QUAD',''),
	('vx_code_source','185','CX','40213152','INFLUENZA, RECOMBINANT, QUADRIVALENT, PF',''),
	('vx_code_source','185','CX','40213152','INFLUENZA, QUADRIVALENT, PF, PEDIATRICS', ''),
	('vx_code_source','98','CX','40213237','PPD TEST',''),
	('vx_code_source','09','CX','40213228','TD (ADULT),2 LF TETANUS TOXOID,PRESERV VACCINE',''),
	('vx_code_source','115','CX','40213230','TDAP VACCINE',''),
	('vx_code_source','146','CX','40213284','DTAP/HEPB/IPV COMBINED VACCINE',''),
	('vx_code_source','49','CX','40213314','HIB (PRP-OMP)',''),
	('vx_code_source','15','CX','40213156','INFLUENZA SPLIT HIGH DOSE PF VACCINE',''),
	('vx_code_source','94','CX','40213184','MMR/VARICELLA COMBINED VACCINED',''),
	('vx_code_source','208','CX','724907','PR PFIZER SARS-COV-2 VACCINE',''),
	('vx_code_source','208','CX','724907','PFIZER SARS-COV-2 VACCINATION',''),
	('vx_code_source','208','CX','724907','COVID-19, MRNA, LNP-S, PF, 30 MCG/0.3 ML DOSE',''),
	('vx_code_source','208','CX','724907','PFIZER COVID-19',''),
	('vx_code_source','208','CX','724907','PFIZER COVID-19|78',''),
	('vx_code_source','208','CX','724907','SARS-COV-2 (PFIZER)',''),
	('vx_code_source','208','CX','724907','SARS-COV-2 (PFIZER)|319',''),
	('vx_code_source','207','CX','724907','MODERNA SARS-COV-2 VACCINE',''),
	('vx_code_source','207','CX','724907','MODERNA COVID-19',''),
	('vx_code_source','207','CX','724907','MODERNA COVID-19|71',''),
	('vx_code_source','207','CX','724907','SARS-COV-2 (MODERNA)',''),
	('vx_code_source','207','CX','724907','SARS-COV-2 (MODERNA)|318',''),
	('vx_code_source','207','CX','724907','MODERNA SARS-COV-2 VACCINATION',''),
	('vx_code_source','212','CX','702866','JANSSEN SARS-COV-2 VACCINE',''),
	('vx_code_source','212','CX','702866','JANSSEN (J&J) COVID-19',''),
	('vx_code_source','212','CX','702866','JANSSEN (J&J) COVID-19|86',''),
	('vx_code_source','212','CX','702866','SARS-COV-2 (JANSSEN)',''),
	('vx_code_source','212','CX','702866','SARS-COV-2 (JANSSEN)|321',''),
	('vx_code_source','213','CX','724904','COVID-19 VACCINE (NOT SPECIFIED)',''),
	('vx_code_source','213','CX','724904','COVID-19 VACCINE (NOT SPECIFIED)|79',''),
	('vx_code_source','213','CX','724904','SARS-COV-2, UNSPECIFIED','')
)
update {{ dag_run.conf['site'] }}_pcornet.immunization
set vx_code = coalesce(target_concept,'999'),
vx_code_type = coalesce(pcornet_name, 'CX')
from {{ dag_run.conf['site'] }}_pcornet.immunization imm
left join {{ dag_run.conf['site'] }}_pedsnet.immunization dimm on dimm.site = '{{ dag_run.conf['site'] }}' and dimm.immunization_id = imm.immunizationid::int
left join vals on vals.concept_description ilike dimm.immunization_source_value
where imm.vx_code = ''
and imm.immunizationid = {{ dag_run.conf['site'] }}_pcornet.immunization.immunizationid
and {{ dag_run.conf['site'] }}_pcornet.immunization.vx_code = ''
and {{ dag_run.conf['site'] }}_pcornet.immunization.vx_code = imm.vx_code;
commit;

begin;
/* updateing the NO mapps to UN */
update {{ dag_run.conf['site'] }}_pcornet.obs_clin
set obsclin_result_modifier = 'UN'
where obsclin_result_modifier = 'NO';
commit;
begin;
/* updateing the NO mapps to UN */
update {{ dag_run.conf['site'] }}_pcornet.obs_gen
set obsgen_result_modifier = 'UN'
where obsgen_result_modifier = 'NO';
commit;

/* removing tpn and bad values from med_admin if they aren't mapped to a "Tier 1" RxNorm class */ 
begin;
with tpn as (
	select
		medadminid
		from {{ dag_run.conf['site'] }}_pcornet.med_admin n
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
		and drug_source_value ilike any
			(array[
			'%human milk%',
			'%breastmilk%',
			'%breast milk%',
			'%formula%',
			'%similac%',
			'%tpn%',
			'%parenteral nutrition%',
			'%fat emulsion%',
			'%fat emul%',
			'%fish oil%',
			'%OMEGA 3%',
			'%omega-3%',
			'%UNDILUTED DILUENT%',
			'%KCAL/OZ%',
			'%kit%',
			'%item%',
			'%custom%',
			'%EMPTY BAG%',
			'%UNABLE TO FIND%',
			'%IMS TEMPLATE%',
			'%EXTEMPORANEOUS TEMPLATE%',
			'%IV INFUSION BUILDER%',
			'%PATIENT SUPPLIED MEDICATION%',
			'%MISC MED%'
			])
)
delete from {{ dag_run.conf['site'] }}_pcornet.med_admin
where medadminid in (select medadminid from tpn);
commit;

/* removing tpn from prescribing */
begin;
with 
tpn as 
(select drug_exposure_id -- select count(*)
from {{ dag_run.conf['site'] }}_pcornet.prescribing n
inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on n.prescribingid::int = de.drug_exposure_id
where rxnorm_cui is null and lower(drug_source_value) ilike any(array['%UNDILUTED DILUENT%','%KCAL/OZ%','%human milk%','%tpn%','%similac%','%fat emulsion%']))
delete from {{ dag_run.conf['site'] }}_pcornet.prescribing
where prescribingid::int in (select drug_exposure_id from tpn);
commit;

/* removing TPN from dispensing */
begin;
with 
tpn as 
(select drug_exposure_id 
from {{ dag_run.conf['site'] }}_pcornet.dispensing n
inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on n.dispensingid::int = de.drug_exposure_id
where lower(drug_source_value) ilike any(array['%UNDILUTED DILUENT%','%KCAL/OZ%','%human milk%','%tpn%','%similac%','%fat emulsion%']))
delete from {{ dag_run.conf['site'] }}_pcornet.dispensing
where dispensingid::int in (select drug_exposure_id from tpn);
commit;

-- stanford map high count ingredients to clincal drugs
begin;
with herapin_1 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 1367571
	and drug_source_value ilike any
			(array[
			'%HEPARIN 1 UNIT/ML%',
			'%HEPARIN (PORCINE) 250 UNIT/250 ML (1 UNIT/ML)%'
			])
)
update {{ dag_run.conf['site'] }}_pcornet.med_admin
set MEDADMIN_CODE = '1362025', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_1); 
commit;

begin;
with herapin_2 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 1367571
	and drug_source_value ilike '%HEPARIN 2 UNIT/ML%'
)
update {{ dag_run.conf['site'] }}_pcornet.med_admin
set MEDADMIN_CODE = '1362935', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_2); 
commit;

begin;
with herapin_100 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id = 43011476
	and drug_source_value ilike '%HEPARIN 100 UNITS/ML%'
)
update {{ dag_run.conf['site'] }}_pcornet.med_admin
set MEDADMIN_CODE = '1361048', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from herapin_100); 
commit;

begin;
with sodium_chloride_9 as (
	select medadminid 
	from {{ dag_run.conf['site'] }}_pcornet.med_admin med
	inner join {{ dag_run.conf['site'] }}_pedsnet.drug_exposure de on med.medadminid::bigint = de.drug_exposure_id
	where drug_concept_id in (967823,46276153)
	and drug_source_value ilike '%SODIUM CHLORIDE 0.9%'
)
update {{ dag_run.conf['site'] }}_pcornet.med_admin
set MEDADMIN_CODE = '313002', MEDADMIN_TYPE = 'RX'
where medadminid in (select medadminid from sodium_chloride_9); 
commit;

begin;
/* updating norm_modifier_low for the values */
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'EQ',
norm_modifier_high = 'EQ'
where result_modifier = 'EQ' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GE',
norm_modifier_high = 'NO'
where result_modifier = 'GE' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'EQ',
norm_modifier_high = 'EQ'
where result_modifier = 'EQ' and norm_modifier_low = 'OT' and norm_modifier_high in ('GE','GT');
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GE',
norm_modifier_high = 'NO'
where result_modifier = 'GE' and norm_modifier_low = 'OT' and norm_modifier_high = 'GE';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set result_modifier = 'GT',
norm_modifier_high = 'NO',
norm_modifier_low = 'GT'
where result_modifier = 'OT' and norm_modifier_low = 'OT' and norm_modifier_high = 'GT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LT'
where result_modifier = 'LT' and norm_modifier_low = 'OT' and norm_modifier_high = 'GE';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GT',
norm_modifier_high = 'NO'
where result_modifier = 'GT' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GT',
norm_modifier_high = 'NO'
where result_modifier = 'GT' and norm_modifier_low in ('OT') and norm_modifier_high in ('GT','GE');
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LT'
where result_modifier = 'LT' and norm_modifier_low in ('OT') and norm_modifier_high in ('GT','GE');
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LE'
where result_modifier = 'LE' and norm_modifier_low in ('LT') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LT'
where result_modifier = 'LT' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'OT'
where result_modifier = 'OT' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LE'
where result_modifier = 'LE' and norm_modifier_low in ('LT','LE') and norm_modifier_high = 'OT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GT',
norm_modifier_high = 'NO'
where result_modifier = 'GT' and norm_modifier_low = 'OT' and norm_modifier_high = 'GE';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GT',
norm_modifier_high = 'NO',
result_modifier = 'GT'
where result_modifier = 'OT' and norm_modifier_low = 'OT' and norm_modifier_high = 'GE';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'EQ',
norm_modifier_high = 'EQ'
where result_modifier = 'EQ' and norm_modifier_low in ('OT') and norm_modifier_high = 'GT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'GE',
norm_modifier_high = 'NO'
where result_modifier = 'GT' and norm_modifier_low in ('OT') and norm_modifier_high = 'GT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_low = 'NO',
norm_modifier_high = 'LT'
where result_modifier = 'LT' and norm_modifier_low in ('OT') and norm_modifier_high = 'GT';
commit;
begin;
update {{ dag_run.conf['site'] }}_pcornet.lab_result_cm
set norm_modifier_high = 'OT'
where result_modifier = 'OT' and norm_modifier_low in ('OT') and norm_modifier_high = 'GT';
commit;