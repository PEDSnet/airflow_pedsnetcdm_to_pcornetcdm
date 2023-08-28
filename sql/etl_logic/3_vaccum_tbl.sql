begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.demographic;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.death;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.death_cause;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.diagnosis;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.condition;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.dispensing;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.enrollment;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.encounter;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.lab_result_cm;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.procedures;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.vital;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.provider;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.med_admin;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.obs_clin;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.obs_gen;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.prescribing;
commit;
begin;
VACUUM (VERBOSE, ANALYZE) {{ dag_run.conf['site'] }}_pcornet.private_demographic;
commit;