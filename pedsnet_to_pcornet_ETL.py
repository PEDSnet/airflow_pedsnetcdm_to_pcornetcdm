from datetime import timedelta, datetime
import os
import shutil
import glob
from airflow.decorators import dag, task, task_group
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models.param import Param

db_conn = '...'
site = '...'
etl_sql_directory = 'dags/PEDSnet_to_PCORnet_ETL/sql/etl_logic/'

default_args = {
		'owner': 'bossa',
		'retries': 2,
        'schedule': "None",
		'retry_delay': timedelta(minutes=0.5),
}

@dag(dag_id='ETL_PEDSnet_to_PCORnet', 
     default_args=default_args,
     schedule_interval=None, 
     start_date=datetime(2023, 5, 25), 
     template_searchpath = ['.../airflow/dags/PEDSnet_to_PCORnet_ETL/sql/etl_logic/'],
     catchup=False,
     params = {
         'site': Param(None, type=['null','string']),
        }
     )

def PEDSnet_to_PCORnet_ETL():
    
    create_mapping_schema_and_tables = PostgresOperator(
        task_id="create_mapping_schema_and_tables",
        postgres_conn_id=db_conn,
        database='pcornet_preserve_indiana',
        sql=['1_create_mapping_schema_and_tables.sql'],
    )

    # @task()
    # def populate_mapping_tables():

    # phase 1 tasks
    ddl_pcornet = PostgresOperator(
        task_id="ddl_pcornet",
        postgres_conn_id=db_conn,
        database='pcornet_preserve_indiana',
        sql=['1_ddl_pcornet_v61.sql'],
    )

    harvest_upload = PostgresOperator(
    task_id = 'harvest_upload',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['1_harvest_upload.sql'], 
   )

    alter_tbl_owner = PostgresOperator(
    task_id = 'alter_tbl_owner',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['1_alter_tbl_owner.sql'], 
   )
    
    site_col = PostgresOperator(
    task_id = 'site_col',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['1_site_col.sql'], 
   )
    
    # phase 2 tasks
    person_visit_start2001 = PostgresOperator(
    task_id = 'person_visit_start2001',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['1_person_visit_start2001.sql'], 
   )

    demographic = PostgresOperator(
    task_id = 'demographic',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_demographic.sql'], 
   )
    
    private_demographic = PostgresOperator(
    task_id = 'private_demographic',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_private_demographic.sql'], 
   )
    
    enrollment = PostgresOperator(
    task_id = 'enrollment',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_enrollment.sql'], 
   )

    condition = PostgresOperator(
    task_id = 'condition',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_condition.sql'], 
   )
    
    pro_cm = PostgresOperator(
    task_id = 'pro_cm',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_pro_cm.sql'], 
   )
    
    provider = PostgresOperator(
    task_id = 'provider',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_provider.sql'], 
   )
    
    encounter = PostgresOperator(
    task_id = 'encounter',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_encounter.sql'], 
   )

    encounter_vacuum = PostgresOperator(
    task_id = 'encounter_vacuum',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_encounter_vacuum.sql']
   ) 

    diagnosis = PostgresOperator(
    task_id = 'diagnosis',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_diagnosis.sql'], 
    )

    obs_gen = PostgresOperator(
    task_id = 'obs_gen',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_obs_gen.sql'], 
   )

    procedures = PostgresOperator(
    task_id = 'procedures',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_procedures.sql'], 
   )
    
    procedures_vacuum = PostgresOperator(
    task_id = 'procedures_vacuum',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_procedures_vacuum.sql']
   )

    immunization = PostgresOperator(
    task_id = 'immunization',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_immunization.sql'], 
   )

    immunization_vacuum = PostgresOperator(
    task_id = 'immunization_vacuum',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_immunization_vacuum.sql']
   )

    not_map_immunization = PostgresOperator(
    task_id = 'not_map_immunization',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_not_map_immunization.sql'], 
   )

    death = PostgresOperator(
    task_id = 'death',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_death.sql'], 
   )

    death_cause = PostgresOperator(
    task_id = 'death_cause',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_death_cause.sql'],
    ) 

    lds_address_history = PostgresOperator(
    task_id = 'lds_address_history',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_lds_address_history.sql'],
    ) 

    private_address_geocode = PostgresOperator(
    task_id = 'private_address_geocode',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_private_address_geocode.sql'],
    ) 

    vitals = PostgresOperator(
    task_id = 'vitals',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_vitals.sql'],
    ) 

    obs_before_clin_vacuum1 = PostgresOperator(
    task_id = 'obs_before_clin_vacuum1',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_obs_before_clin_vacuum1.sql']
    )

    obs_before_clin_vacuum2 = PostgresOperator(
    task_id = 'obs_before_clin_vacuum2',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_obs_before_clin_vacuum2.sql']
    )

    obs_before_clin_vacuum3 = PostgresOperator(
    task_id = 'obs_before_clin_vacuum3',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_obs_before_clin_vacuum3.sql']
    )

    obs_before_clin_vacuum4 = PostgresOperator(
    task_id = 'obs_before_clin_vacuum4',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_obs_before_clin_vacuum4.sql']
    )

    obs_before_clin = PostgresOperator(
    task_id = 'obs_before_clin',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_obs_before_clin.sql'],
    )
    
    obs_clin = PostgresOperator(
    task_id = 'obs_clin',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_obs_clin.sql'],
    )

    dispensing = PostgresOperator(
    task_id = 'dispensing',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_dispensing.sql'],
    )

    prescribing = PostgresOperator(
    task_id = 'prescribing',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_prescribing.sql'],
    )

    med_admin = PostgresOperator(
    task_id = 'med_admin',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_med_admin.sql'],
    )

    before_lab_result_cm = PostgresOperator(
    task_id = 'before_lab_result_cm',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_before_lab_result_cm.sql'],
    )

    before_lab_result_cm_vacuum = PostgresOperator(
    task_id = 'before_lab_result_cm_vacuum',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_before_lab_result_vacuum.sql']
    )

    lab_qual_vacuum = PostgresOperator(
    task_id = 'lab_qual_vacuum',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['2_lab_qual_vacuum.sql']
    )

    lab_result_cm = PostgresOperator(
    task_id = 'lab_result_cm',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_lab_result_cm.sql'],
    )

    lab_result_cm_finalize = PostgresOperator(
    task_id = 'lab_result_cm_finalize',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['2_lab_result_cm_finalize.sql'],
    )
    
    vacuum_tbl_condition = PostgresOperator(
    task_id = 'vacuum_tbl_condition',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_condition.sql']
    )

    vacuum_tbl_death_cause = PostgresOperator(
    task_id = 'vacuum_tbl_death_cause',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_death_cause.sql']
    )

    vacuum_tbl_death = PostgresOperator(
    task_id = 'vacuum_tbl_death',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_death.sql']
    )

    vacuum_tbl_demographic = PostgresOperator(
    task_id = 'vacuum_tbl_demographic',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_demographic.sql']
    )

    vacuum_tbl_diagnosis = PostgresOperator(
    task_id = 'vacuum_tbl_diagnosis',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_diagnosis.sql']
    )

    vacuum_tbl_dispensing = PostgresOperator(
    task_id = 'vacuum_tbl_dispensing',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_dispensing.sql']
    )

    vacuum_tbl_encounter = PostgresOperator(
    task_id = 'vacuum_tbl_encounter',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_encounter.sql']
    )

    vacuum_tbl_enrollment = PostgresOperator(
    task_id = 'vacuum_tbl_enrollment',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_enrollment.sql']
    )

    vacuum_tbl_immunization = PostgresOperator(
    task_id = 'vacuum_tbl_immunization',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_immunization.sql']
    )

    vacuum_tbl_lab_result_cm = PostgresOperator(
    task_id = 'vacuum_tbl_lab_result_cm',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_lab_result_cm.sql']
    )

    vacuum_tbl_lds_address_history = PostgresOperator(
    task_id = 'vacuum_tbl_lds_address_history',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_lds_address_history.sql']
    )

    vacuum_tbl_med_admin = PostgresOperator(
    task_id = 'vacuum_tbl_med_admin',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_med_admin.sql']
    )

    vacuum_tbl_obs_clin = PostgresOperator(
    task_id = 'vacuum_tbl_obs_clin',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_obs_clin.sql']
    )

    vacuum_tbl_obs_gen = PostgresOperator(
    task_id = 'vacuum_tbl_obs_gen',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_obs_gen.sql']
    )

    vacuum_tbl_prescribing = PostgresOperator(
    task_id = 'vacuum_tbl_prescribing',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_prescribing.sql']
    )

    vacuum_tbl_private_address_geocode = PostgresOperator(
    task_id = 'vacuum_tbl_private_address_geocode',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_private_address_geocode.sql']
    )

    vacuum_tbl_private_demographic = PostgresOperator(
    task_id = 'vacuum_tbl_private_demographic',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_private_demographic.sql']
    )

    vacuum_tbl_pro_cm = PostgresOperator(
    task_id = 'vacuum_tbl_pro_cm',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_pro_cm.sql']
    )

    vacuum_tbl_procedures = PostgresOperator(
    task_id = 'vacuum_tbl_procedures',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_procedures.sql']
    )

    vacuum_tbl_provider = PostgresOperator(
    task_id = 'vacuum_tbl_provider',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_provider.sql']
    )

    vacuum_tbl_vital = PostgresOperator(
    task_id = 'vacuum_tbl_vital',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    autocommit = True,
    sql = ['3_vacuum_tbl_vital.sql']
    )

    index = PostgresOperator(
    task_id = 'index',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['3_index.sql'],
    )

    constraints = PostgresOperator(
    task_id = 'constraints',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['3_constraints.sql'],
    )

    drop_tbl = PostgresOperator(
    task_id = 'drop_tbl',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['3_drop_tbl.sql'],
    )

    cdm_conformance = PostgresOperator(
    task_id = 'cdm_conformance',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['4_cdm_conformance.sql'],
    )

    update = PostgresOperator(
    task_id = 'update',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['4_update.sql'],
    )

    check_orphan_ids = PostgresOperator(
    task_id = 'check_orphan_ids',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['4_check_orphan_ids.sql'],
    )

    harvest = PostgresOperator(
    task_id = 'harvest',
    postgres_conn_id = db_conn,
    database =  'pcornet_preserve_indiana',
    sql = ['4_harvest.sql'],
    )


    # phase 1
    # prep_sql_files_for_execution() >> create_mapping_schema_and_tables 
    create_mapping_schema_and_tables  >> ddl_pcornet
    ddl_pcornet >> [harvest_upload,alter_tbl_owner,site_col] >> person_visit_start2001

    #phase 2
    person_visit_start2001 >> demographic >> private_demographic
    person_visit_start2001 >> enrollment
    person_visit_start2001 >> condition
    person_visit_start2001 >> provider
    person_visit_start2001 >> pro_cm
    [person_visit_start2001, provider] >> encounter >> encounter_vacuum >> diagnosis
    [person_visit_start2001, encounter_vacuum] >> obs_gen
    [person_visit_start2001, encounter_vacuum] >> procedures >> procedures_vacuum
    [person_visit_start2001, procedures_vacuum] >> immunization >> immunization_vacuum >> not_map_immunization
    person_visit_start2001 >> death >> death_cause
    person_visit_start2001 >> lds_address_history >> private_address_geocode
    person_visit_start2001 >> vitals >> [obs_before_clin_vacuum3,obs_before_clin_vacuum4]
    [person_visit_start2001,obs_before_clin_vacuum3,obs_before_clin_vacuum4]>> obs_before_clin >> [obs_before_clin_vacuum1,obs_before_clin_vacuum2]
    [vitals, obs_before_clin_vacuum1,obs_before_clin_vacuum2] >> obs_clin
    person_visit_start2001 >> dispensing
    person_visit_start2001 >> prescribing
    [person_visit_start2001,dispensing,prescribing] >> med_admin
    person_visit_start2001 >> before_lab_result_cm >> before_lab_result_cm_vacuum >> lab_result_cm >> lab_qual_vacuum >> lab_result_cm_finalize

    # phase 3
    [private_demographic, enrollment, condition, pro_cm, diagnosis, obs_gen, not_map_immunization, 
    death_cause, private_address_geocode, med_admin, obs_clin, lab_result_cm_finalize] >> drop_tbl
    drop_tbl >> [vacuum_tbl_vital, vacuum_tbl_provider, vacuum_tbl_procedures, vacuum_tbl_pro_cm, vacuum_tbl_private_demographic,
                 vacuum_tbl_private_address_geocode, vacuum_tbl_prescribing, vacuum_tbl_obs_gen, vacuum_tbl_obs_clin, vacuum_tbl_med_admin,
                 vacuum_tbl_lds_address_history, vacuum_tbl_lab_result_cm, vacuum_tbl_immunization, vacuum_tbl_enrollment, vacuum_tbl_encounter,
                 vacuum_tbl_dispensing, vacuum_tbl_diagnosis, vacuum_tbl_demographic, vacuum_tbl_death, vacuum_tbl_death_cause, vacuum_tbl_condition] >> index >> constraints

    # phase 4
    constraints >> cdm_conformance >> update  >> check_orphan_ids >> harvest
dag = PEDSnet_to_PCORnet_ETL()