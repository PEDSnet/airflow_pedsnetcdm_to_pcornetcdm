begin;

CREATE TABLE {{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001 AS
    SELECT 
        cast(person_id as bigint), 
        cast(visit_occurrence_id as bigint) AS visit_id
    FROM 
        {{ dag_run.conf['site'] }}_pedsnet.visit_occurrence
    WHERE 
        EXTRACT(YEAR FROM visit_start_date) >= 2001;
commit;

begin;
ALTER TABLE {{ dag_run.conf['site'] }}_pcornet_airflow.person_visit_start2001
ADD CONSTRAINT xpk_person_visit_start2001
PRIMARY KEY (visit_id);
commit;