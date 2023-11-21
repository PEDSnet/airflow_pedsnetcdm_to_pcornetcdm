begin;

INSERT INTO {{ dag_run.conf['site'] }}_pcornet_airflow.version_history (operation, model, model_version, dms_version, dmsa_version) VALUES ('create indexes', 'pcornet', '6.1.0', '1.0.3-alpha', '0.6.1');

CREATE INDEX idx_enrol_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.enrollment (patid);

CREATE INDEX idx_death_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.death (patid);

CREATE INDEX idx_death_cause_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.death_cause (patid);

CREATE INDEX idx_encounter_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.encounter (patid);

CREATE INDEX idx_encounter_enctype ON {{ dag_run.conf['site'] }}_pcornet_airflow.encounter (enc_type);

-- CREATE INDEX idx_cond_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.condition (encounterid);

CREATE INDEX idx_cond_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.condition (patid);

CREATE INDEX idx_condition_ccode ON {{ dag_run.conf['site'] }}_pcornet_airflow.condition (condition);

CREATE INDEX idx_diag_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis (patid);

CREATE INDEX idx_diag_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis (encounterid);

CREATE INDEX idx_diag_code ON {{ dag_run.conf['site'] }}_pcornet_airflow.diagnosis (dx);

CREATE INDEX idx_proc_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.procedures (encounterid);

CREATE INDEX idx_proc_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.procedures (patid);

CREATE INDEX idx_proc_px ON {{ dag_run.conf['site'] }}_pcornet_airflow.procedures (px);

CREATE INDEX idx_disp_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.dispensing (patid);

CREATE INDEX idx_disp_ndc ON {{ dag_run.conf['site'] }}_pcornet_airflow.dispensing (ndc);

--CREATE INDEX idx_pres_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.prescribing (encounterid);

CREATE INDEX idx_pres_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.prescribing (patid);

CREATE INDEX idx_pres_rxnorm ON {{ dag_run.conf['site'] }}_pcornet_airflow.prescribing (rxnorm_cui);

CREATE INDEX idx_vital_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.vital (patid);

CREATE INDEX idx_vital_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.vital (encounterid);

CREATE INDEX idx_lab_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm (patid);

CREATE INDEX idx_lab_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm (encounterid);

CREATE INDEX idx_loinc_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.lab_result_cm (lab_loinc);

CREATE INDEX idx_med_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin (patid);

CREATE INDEX idx_med_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.med_admin (encounterid);

CREATE INDEX idx_obsclin_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.obs_clin(patid);

CREATE INDEX idx_obsclin_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.obs_clin(encounterid);

CREATE INDEX idx_obsgen_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.obs_gen (patid);

CREATE INDEX idx_obsgen_encid ON {{ dag_run.conf['site'] }}_pcornet_airflow.obs_gen (encounterid);

CREATE INDEX idx_geocode_addr ON {{ dag_run.conf['site'] }}_pcornet_airflow.private_address_geocode (addressid);

CREATE INDEX idx_procm_patid ON {{ dag_run.conf['site'] }}_pcornet_airflow.pro_cm (patid);

commit;
