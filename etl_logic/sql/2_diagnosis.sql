begin;
create table {{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag as
select co.* 
from 
	{{ dag_run.conf['site'] }}_pedsnet.condition_occurrence co
inner join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001 pvs 
	on co.visit_occurrence_id = pvs.visit_id
where
	co.condition_type_concept_id not in (2000000089, 2000000090, 2000000091, 38000245)
	and extract (year from condition_start_date) >=2001
	and not condition_source_value ~ 'NOD.X';
commit;

begin;
CREATE INDEX idx_filtdia_encid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag USING btree
    (visit_occurrence_id )
    TABLESPACE pg_default;

CREATE INDEX idx_filtdia_patid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag USING btree
    (person_id )
    TABLESPACE pg_default;
	
CREATE INDEX idx_filtdia_diagid
    ON {{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag USING btree
    (condition_occurrence_id )
    TABLESPACE pg_default;
commit;

begin;
insert into {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis(
            diagnosisid,patid, encounterid, enc_type, admit_date, providerid, dx, dx_type, dx_date,
            dx_source, pdx, dx_origin, raw_dx, raw_dx_type, raw_dx_source, raw_pdx,site, dx_poa)
select 
	cast(co.condition_occurrence_id as text) as diagnosisid,
	cast(co.person_id as text) as patid,
	cast(co.visit_occurrence_id as text) encounterid,
	enc.enc_type,
	enc.admit_date,
	enc.providerid,
	-- look for ICDs, followed by SNOMED, following by others
	left(
		case 
			when c3.vocabulary_id in ('ICD9CM', 'ICD10','ICD10CM') then c3.concept_code
	     	else 
				case 
					when co.condition_concept_id>0
		        	then c2.concept_code
	    			else 
			 			case 
							when length(trim(split_part(condition_source_value,'|',3)))>0 then 
								case 
									when trim(split_part(condition_source_value,'|',3)) like '%,%' then trim(split_part(trim(leading ',' from split_part(condition_source_value,'|',3)),',',1)) 
									else trim(split_part(condition_source_value,'|',3)) 
								end
         	       			else trim(split_part(condition_source_value,'|',2))
         	  			end
         		end
    	end,
	18) as dx,
	case 
		when c3.vocabulary_id = 'ICD9CM' then '09'
		else 
			case 
				when  c3.vocabulary_id in ('ICD10','ICD10CM') then '10'
		     	else
		     		case 
						when co.condition_concept_id > 0 then 'SM'
		          		else 'OT'
		     		end
			end
	end as dx_type,
	co.condition_start_date as dx_date,
	coalesce(m1.target_concept,'OT') as dx_source,
	coalesce(m2.target_concept, 'NI') as pdx,
	coalesce(m3.target_concept,'OT') as dx_origin,
	concat(split_part(condition_source_value,'|',1), '|', split_part(condition_source_value,'|',3)) as raw_dx,
	case 
		when co.condition_source_concept_id = '44814649' then 'OT'
	    else c3.vocabulary_id
	end as raw_dx_type,
    c4.concept_name as raw_dx_source,
	case 
		when co.condition_type_concept_id IN (2000000092, 2000000093, 2000000094, 2000000098, 2000000099, 2000000100, 38000201, 38000230) then c4.concept_name
		else NULL
	end as raw_pdx,
	'{{ dag_run.conf['site'] }}' as site,
	coalesce(m4.target_concept,'OT') as dx_poa
from 
	{{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag co
join 
	vocabulary.concept c2 on co.condition_concept_id = c2.concept_id
join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.encounter enc on cast(co.visit_occurrence_id as text)=enc.encounterid
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m1 on m1.source_concept_class='dx_source' 
	and cast(co.condition_type_concept_id as text) = m1.source_concept_id
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m2 
	on cast(co.condition_type_concept_id as text) = m2.source_concept_id  
	and m2.source_concept_class='pdx'
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m3 
	on cast(co.condition_type_concept_id as text) = m3.source_concept_id  
	and m3.source_concept_class='dx origin'
left join 
	vocabulary.concept c3 
	on co.condition_source_concept_id = c3.concept_id
left join 
	vocabulary.concept c4 
	on co.condition_type_concept_id = c4.concept_id
left join 
	pcornet_maps.pedsnet_pcornet_valueset_map m4 
	on cast(co.poa_concept_id as text) = m4.source_concept_id  
	and m4.source_concept_class='dx_poa'
;
commit;

begin;
delete from {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis
where length(dx) < 2;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis
set 
	dx = v.concept_code, 
	dx_type = case 
		when v.vocabulary_id = 'ICD9CM'  then '09'
		else
			case 
				when  v.vocabulary_id in ('ICD10','ICD10CM')
		     	then '10'
		     	else 'OT' 
			end 
		end
from 
	{{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis d
inner join 
	{{ dag_run.conf['site'] }}_pedsnet.condition_occurrence c 
	on c.condition_occurrence_id = d.diagnosisid::int
inner join 
	vocabulary.concept v 
	on v.concept_code ilike trim(split_part(condition_source_value,'|',3)) 
	and v.vocabulary_id in ('ICD10','ICD9CM','ICD10CM')
	and trim(split_part(condition_source_value,'|',3)) ilike any (array['%B97.28%','%U07.1%','%B34.2%','%B34.9%','%B97.2%','%B97.21%','%J12.81%','%U04%','%U04.9%','%U07.2%','%Z20.828%'])
where 
	d.dx_type in ('SM','OT') 
	and {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis.diagnosisid = d.diagnosisid 
	and {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis.dx_type in ('SM','OT')
;
commit;

-- solution to DC 3.06
 -- flips 1 diagosis on an encounter to primary if no primaries exist (stratified by enc_type and dx_origin)
 -- For dx_orign = OD enc_type = IP and dx_orign = OD enc_type = EI
begin;
 -- get distribution of the number of primary and secondary OD diagnoses for each IP encounter
 with primary_diagnosis_distribution as (
 select 
 	encounterid, 
 	enc_type,
 	sum(case when pdx = 'P' then 1 else 0 end) as prim,
 	sum(case when pdx <> 'P' then 1 else 0 end) as sec
 from 
 	{{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis
 where
 	enc_type in ('IP', 'EI')
 	and dx_origin = 'OD'
 group by 
 	encounterid, 
 	enc_type
 ),

 -- get all encounters with 0 primary diagnoses
 no_primary_diagnoses as (
 select 
     *
 from 
 	primary_diagnosis_distribution 
where 
	prim = 0
 ),

 -- define row numbers for each secondary diagnosis
 secondary_diagnosisids as (
 select 
 	encounterid,
 	diagnosisid,
 	ROW_NUMBER() OVER (PARTITION BY encounterid, enc_type order by diagnosisid) as rn
 from 
 	{{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis
 where 
 	encounterid in (select encounterid from no_primary_diagnoses)
 	and enc_type in ('IP', 'EI')
 	and dx_origin = 'OD'
 ),

 -- get 1 secondary diagnosis for each encounter
 scondary_to_primary_candidate as (
 select diagnosisid
 from secondary_diagnosisids
 where rn = 1
 )

 -- update that secondary diagnosis as primary
 update {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis
 set pdx = 'P'
 where diagnosisid in (select diagnosisid from scondary_to_primary_candidate);
 commit;