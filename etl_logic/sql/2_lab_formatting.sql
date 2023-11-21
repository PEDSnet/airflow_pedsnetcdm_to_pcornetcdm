begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = target_concept
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
left join pcornet_maps.pedsnet_pcornet_valueset_map map on lower(map.target_concept) = lower(l.raw_unit) and source_concept_class in ('result_unit_source','Result unit')
where l.result_unit in ('','OT','NI') 
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.result_unit in ('','OT','NI') 
and l.lab_result_cm_id = {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '/100{WBCs}'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('/100 WBC','/100 WBCS','/100(WBCs)','/100 wbc','/100WBC','/100wbc')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 's'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('second(s)','SECONDS','Seconds')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '/[HPF]'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('/HPF','/hpf')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '/[LPF]'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('/LPF')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'mm/h'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mm/hr','MM/HR','mm/Hr','MM/Hr','mm/1hr')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'mg/(24.h)'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mg/24hr','mg/day')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'g/(24.h)'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('g/day')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '%{vol}'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('vol %')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'Cel'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('CELSIUS')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '{cells}/uL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('cells/uL','cells/ul','cell/uL')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
 commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'mm[Hg]'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mmHg','mmHG','mmHG.')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '10*3/uL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('THOU/uL','Thousand/uL','THOU/ul','10 3/uL','th/uL','Thousands/uL','thousand/u;','Thou/uL','thou/uL','THOUS/MCL','thous/mcL','thousand/u;','Thousand/uL','thousand/ul','Thousands/uL', 'K/mm3','k/mm3')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '10*6/uL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('10 6/uL','MIL/uL','mil/uL','MILL/MCL','mill/mcL','Mill/uL','mill/uL','Million/uL','million/ul','x10E6/uL','M/mm3','m/mm3')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'ug/dL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mcg/dL','MCG/DL','mcg/dl','mcg/dL (calc)')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'mg/dL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mg/dL')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '%'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('%')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '/mm3'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('/mm3')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '10*-3.eq/L'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mEq/L')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'fL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('fL')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'g/dL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('g/dL')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = '[IU]/L'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('IU/L')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'pg'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('pg')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'ng/mL'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('ng/mL')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set result_unit = 'mmol/L'
from {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm l
inner join {{ dag_run.conf['site'] }}_pedsnet.measurement m on m.measurement_id = l.lab_result_cm_id::bigint and m.unit_source_value in ('mmol/L')
where (l.result_unit in ('NI','UN','OT','') or l.result_unit is null)
and {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm.lab_result_cm_id = l.lab_result_cm_id;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set result_unit = '[pH]'
where 
    (
    lab_loinc in ('11558-4','2749-0','5803-2','2746-6')
    or lower(raw_lab_name) like any(
    array[
        '%ph of venous blood%',
        '%ph of blood%',
        '%ph of urine by test strip%',
        '%ph of blood%',
        '%ph of urine by test strip%',
        '%ph of gastric fluid%',
        '%ph of capillary blood%',
        '%ph of venous blood%',
        '%ph of urine by test strip%'
        ])
    )
and result_unit in ('OT','','NI','UN');
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set result_unit = '{ratio}'
where 
    (
    lab_loinc in ('5811-5','1759-0','2965-2','6301-6')
    or lower(raw_lab_name) like any(
    array[
        '%inr in platelet poor plasma by coagulation assay%',
        '%specific gravity of urine%',
        '%specific gravity of urine by test strip%',
        '%albumin/globulin [mass ratio] in serum or plasma%'
        ])
    )
and result_unit in ('OT','','NI','UN');
commit;

-- DC 2.06 --> BASOPHILS ABSOLUTE, LYMPHOCYTES ABSOLUTE, MONOCYTES ABSOLUTE, NEUTROPHILS ABSOLUTE
begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set 
	result_num = result_num / 1000,
	result_unit = '10*3/uL'
where
	lab_loinc = (
        '704-7', -- BASOPHILS
        '731-0', -- LYMPHOCYTES
        '743-5', -- MONOCYTES
        '742-7', -- MONOCYTES
        '26499-4', -- NEUTROPHILS
        '751-8' -- NEUTROPHILS
    )
	and result_unit in ('uL','/uL','{#}/uL','{cells}/uL', '/mm3');
commit;

-- DC 3.13 --> White Blood Cell LOINC codes
 begin;
 update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
 set 
    lab_loinc = '26464-8'
 where 
    lab_loinc = '20584-9' 
    and specimen_source = 'BLD';
commit;

/* updating norm_modifiers for the values */
begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'EQ',
    norm_modifier_high = 'EQ'
where 
    result_modifier = 'EQ' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GE',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GE' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'EQ',
    norm_modifier_high = 'EQ'
where 
    result_modifier = 'EQ' 
    and norm_modifier_low = 'OT' 
    and norm_modifier_high in ('GE','GT');
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GE',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GE' 
    and norm_modifier_low = 'OT' 
    and norm_modifier_high = 'GE';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    result_modifier = 'GT',
    norm_modifier_high = 'NO',
    norm_modifier_low = 'GT'
where 
    result_modifier = 'OT' 
    and norm_modifier_low = 'OT' 
    and norm_modifier_high = 'GT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LT'
where 
    result_modifier = 'LT' 
    and norm_modifier_low = 'OT' 
    and norm_modifier_high = 'GE';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GT',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GT' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GT',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GT' 
    and norm_modifier_low in ('OT') 
    and norm_modifier_high in ('GT','GE');
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LT'
where 
    result_modifier = 'LT' 
    and norm_modifier_low in ('OT') 
    and norm_modifier_high in ('GT','GE');
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LE'
where 
    result_modifier = 'LE' 
    and norm_modifier_low in ('LT') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LT'
where 
    result_modifier = 'LT' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'OT'
where 
    result_modifier = 'OT' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LE'
where 
    result_modifier = 'LE' 
    and norm_modifier_low in ('LT','LE') 
    and norm_modifier_high = 'OT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GT',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GT' 
    and norm_modifier_low = 'OT'
    and norm_modifier_high = 'GE';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GT',
    norm_modifier_high = 'NO',
    result_modifier = 'GT'
where 
    result_modifier = 'OT' 
    and norm_modifier_low = 'OT' 
    and norm_modifier_high = 'GE';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'EQ',
    norm_modifier_high = 'EQ'
where 
    result_modifier = 'EQ' 
    and norm_modifier_low in ('OT') 
    and norm_modifier_high = 'GT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'GE',
    norm_modifier_high = 'NO'
where 
    result_modifier = 'GT' 
    and norm_modifier_low in ('OT') 
    and norm_modifier_high = 'GT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LT'
where 
    result_modifier = 'LT'
    and norm_modifier_low in ('OT') 
    and norm_modifier_high = 'GT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm
set 
    norm_modifier_high = 'OT'
where 
    result_modifier = 'OT' 
    and norm_modifier_low in ('OT') 
    and norm_modifier_high = 'GT';
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set 
    result_modifier = 'EQ' 
where 
    norm_modifier_high = 'EQ' 
    and norm_modifier_low = 'OT' 
    and result_modifier = 'OT';	
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set 
    result_modifier = 'LE' 
where 
    norm_modifier_high = 'LE' 
    and norm_modifier_low = 'OT' 
    and (result_modifier in ('','OT') or result_modifier is null);
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set 
    result_modifier = 'LT' 
where 
    norm_modifier_high = 'LT' 
    and norm_modifier_low = 'OT' 
    and result_modifier = 'OT';		
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set 
    result_modifier = 'EQ' 
where 
    norm_modifier_high = 'OT' 
    and norm_modifier_low = 'EQ' 
    and (result_modifier in ('','OT') or result_modifier is null);
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set  
    norm_modifier_low = 'NO',
    norm_modifier_high = 'LT'
where 
    result_modifier = 'EQ'
    and norm_modifier_high = 'EQ' 
    and norm_modifier_low = 'EQ' 
    and norm_range_low is null
    and norm_range_high is not null;
commit;

begin;
update {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm 
set  
    result_modifier = 'EQ',
    norm_modifier_low = 'EQ',
    norm_modifier_high = 'EQ'
where 
    result_modifier = 'LT'
    and norm_modifier_low = 'NO' 
    and norm_modifier_high = 'LT' 
    and norm_range_low is not null
    and norm_range_high is not null;
commit;
