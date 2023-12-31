begin;
insert into {{ dag_run.conf['site'] }}_pcornet_airflow.enrollment (patid, enr_start_date, enr_end_date, chart, enr_basis, site)
select distinct
	cast(op.person_id as text) as pat_id,
	cast(
	cast(date_part('year', observation_period_start_date) as text)||'-'||lpad(cast(date_part('month', observation_period_start_date) as text),2,'0')||'-'||lpad(cast(date_part('day', observation_period_start_date) as text),2,'0')
	as date)
	as enr_start_date,
	cast( cast(date_part('year', observation_period_end_date) as text)||'-'||lpad(cast(date_part('month', observation_period_end_date) as text),2,'0')||'-'||lpad(cast(date_part('day', observation_period_end_date) as text),2,'0')
	as date)
	as enr_end_date,
	'Y' as chart, -- defaulting to yes
	'E' as ENR_basis,
	'{{ dag_run.conf['site'] }}' as site
from
	{{ dag_run.conf['site'] }}_pedsnet.observation_period op
inner join 
	{{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001 pvs 
	on op.person_id = pvs.person_id
;
commit;
