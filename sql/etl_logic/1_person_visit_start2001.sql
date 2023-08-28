begin;

CREATE TABLE {{ dag_run.conf['site'] }}_pcornet.person_visit_start2001
AS
SELECT person_id, visit_occurrence_id AS visit_id
FROM {{ dag_run.conf['site'] }}_pedsnet.visit_occurrence
WHERE EXTRACT(YEAR FROM visit_start_date) >= 2001;
commit;
begin;
ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.person_visit_start2001
ADD CONSTRAINT xpk_person_visit_start2001
PRIMARY KEY (visit_id);
commit;
begin;
-- Index: idx_pervis_personid

-- DROP INDEX {{ dag_run.conf['site'] }}_pcornet.idx_pervis_personid;

CREATE INDEX idx_pervis_personid
    ON {{ dag_run.conf['site'] }}_pcornet.person_visit_start2001 USING btree
    (person_id)
    TABLESPACE pg_default;
commit;
begin;
-- Index: idx_pervis_visitid

-- DROP INDEX {{ dag_run.conf['site'] }}_pcornet.idx_pervis_visitid;


CREATE INDEX idx_pervis_visitid
    ON {{ dag_run.conf['site'] }}_pcornet.person_visit_start2001 USING btree
    (visit_id)
    TABLESPACE pg_default;

commit;