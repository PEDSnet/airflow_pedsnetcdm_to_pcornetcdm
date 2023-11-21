do $$
declare
  select_row record;
  query varchar;
  result integer;
begin
  for select_row in
	select 'select count(*) from {{ dag_run.conf['site'] }}_pcornet_airflow.' || tablename ||' where patid::int not in (select person_id from {{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001);' as query
    from pg_tables
    where schemaname = '{{ dag_run.conf['site'] }}_pcornet'
	and tablename in ('condition','diagnosis','death', 'death_cause','demographic','dispensing','encounter','enrollment','hash_token','immunization','lab_result_cm','lds_address_history','med_admin','obs_clin','obs_gen','pcornet_trial','prescribing','procedures','pro_cm','vital')
    loop
    	execute select_row.query into result;
		if result > 0 then
		    query := replace(select_row.query, 'select count(*)','delete');
			execute query;
	   		raise warning 'orphan patid % was deleted from %', result, query;
		end if;
    end loop;
	
	for select_row in
	select 'select count(*) from {{ dag_run.conf['site'] }}_pcornet_airflow.' || tablename ||' where encounterid is not null and encounterid::int not in (select visit_id from {{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001);' as query
    from pg_tables
    where schemaname = '{{ dag_run.conf['site'] }}_pcornet'
	and tablename in ('condition','diagnosis','immunization','lab_result_cm','med_admin','obs_clin','obs_gen','prescribing','procedures','pro_cm','vital')
    loop
    	execute select_row.query into result;
		if result > 0 then
		    query := replace(select_row.query, 'select count(*)','delete');	
			execute query;
		    raise warning 'orphan encounterid % deleted from %', result, query;
		end if;
    end loop;
end;
$$;
