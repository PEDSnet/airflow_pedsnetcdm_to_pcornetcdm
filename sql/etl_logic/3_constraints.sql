begin;
INSERT INTO {{ dag_run.conf['site'] }}_pcornet.version_history (operation, model, model_version, dms_version, dmsa_version) VALUES ('create constraints', 'pcornet', '6.1.0', '1.0.3-alpha', '0.6.1');

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.encounter ADD CONSTRAINT fk_encounter_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.procedures ADD CONSTRAINT fk_procedures_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.procedures ADD CONSTRAINT fk_procedures_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.procedures ADD CONSTRAINT fk_procedures_providerid FOREIGN KEY(providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.pro_cm ADD CONSTRAINT fk_pro_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.pro_cm ADD CONSTRAINT fk_pro_cm_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_clin ADD CONSTRAINT fk_obsclin_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_clin ADD CONSTRAINT fk_obsclin_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_clin ADD CONSTRAINT fk_obsclin_providerid FOREIGN KEY(obsclin_providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_gen ADD CONSTRAINT fk_obsgen_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_gen ADD CONSTRAINT fk_obsgen_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.obs_gen ADD CONSTRAINT fk_obsgen_providerid FOREIGN KEY(obsgen_providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.pcornet_trial ADD CONSTRAINT fk_pcornet_trial_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.enrollment ADD CONSTRAINT fk_enrollment_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.death ADD CONSTRAINT fk_death_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.death_cause ADD CONSTRAINT fk_death_cause_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.condition ADD CONSTRAINT fk_condition_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.diagnosis ADD CONSTRAINT fk_diagnosis_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.dispensing ADD CONSTRAINT fk_dispensing_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.med_admin ADD CONSTRAINT fk_medadmin_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.prescribing ADD CONSTRAINT fk_prescribing_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.prescribing ADD CONSTRAINT fk_prescribing_providerid FOREIGN KEY(rx_providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.dispensing ADD CONSTRAINT fk_dispensing_prescribingid FOREIGN KEY(prescribingid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.prescribing (prescribingid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.diagnosis ADD CONSTRAINT fk_diagnosis_providerid FOREIGN KEY(providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.med_admin ADD CONSTRAINT fk_medadmin_providerid FOREIGN KEY(medadmin_providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.condition ADD CONSTRAINT fk_condition_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.diagnosis ADD CONSTRAINT fk_diagnosis_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.prescribing ADD CONSTRAINT fk_prescribing_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.med_admin ADD CONSTRAINT fk_medadmin_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.vital ADD CONSTRAINT fk_vital_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.vital ADD CONSTRAINT fk_vital_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.encounter ADD CONSTRAINT fk_encounter_providerid FOREIGN KEY(providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.hash_token ADD CONSTRAINT fk_hash_token_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.immunization ADD CONSTRAINT fk_immun_encounterid FOREIGN KEY(encounterid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.immunization ADD CONSTRAINT fk_immun_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.immunization ADD CONSTRAINT fk_immun_procedureid FOREIGN KEY(proceduresid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.procedures (proceduresid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.immunization ADD CONSTRAINT fk_immun_providerid FOREIGN KEY(vx_providerid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.provider (providerid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.lds_address_history ADD CONSTRAINT fk_lds_addhist_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.private_address_geocode ADD CONSTRAINT fk_gecode_addressid FOREIGN KEY(addressid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.private_address_history (addressid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.private_address_history ADD CONSTRAINT fk_add_history_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE {{ dag_run.conf['site'] }}_pcornet.private_demographic ADD CONSTRAINT fk_priv_demographic_patid FOREIGN KEY(patid) REFERENCES {{ dag_run.conf['site'] }}_pcornet.demographic (patid) DEFERRABLE INITIALLY DEFERRED;

commit;