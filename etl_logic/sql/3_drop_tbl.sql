begin;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms_bmi;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms_dia;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms_sys;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms_wt;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ndc_concepts;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ob_smoking;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ob_tobacco;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ob_tobacco_type;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ob_tobacco_data;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.rxnorm_ndc_crosswalk;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.vital_extract;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.vital_transform;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.dis_disposition;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.encounter_extract;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.encounter_transform;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.ms_ht;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.rx_dose_form_data;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.specimen_values;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.visit_payer;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.drg_value;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.device_obs;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.meas_obs;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.meas_obsclin;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.obs_chief_compl_transform;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.obs_vaping;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.device_obs;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.adt_obs;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.cond_chief_compl_transf;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.filter_adt;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.filter_obs;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.filter_obs_deriv;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.obs_deriv_transform;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.condition_transform;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.filter_condition;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.imm_body_site;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.lab_qual;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.filter_diag;
drop table IF EXISTS {{ dag_run.conf['site'] }}_pcornet_airflow.observation_extract;
commit;
