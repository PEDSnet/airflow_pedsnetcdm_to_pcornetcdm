#----------- imports -------------
import glob
import io
import os
import numpy as np
import pandas as pd
import psycopg2
import shutil

from airflow.decorators import dag, task, task_group
from airflow.hooks.base_hook import BaseHook
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from common.operator.spark import mySparkSubmitOperator
from datetime import timedelta, datetime

#----------- class extensions -------------
class TriggerDagRunOperator(TriggerDagRunOperator):
    ui_color = "#f5dd9d"
    ui_fgcolor = "black"

class MySQLExecuteQueryOperator(SQLExecuteQueryOperator):
    ui_color = "#d6a692"
    ui_fgcolor = "black"
    template_fields = tuple({"hook_params"} | set(SQLExecuteQueryOperator.template_fields))

class MyPythonOperator(PythonOperator):
    template_fields = tuple({"hook_params"} | set(PythonOperator.template_fields))
database_template = "{{ dag_run.conf['database'] }}"

#----------- DEFINE DAG -------------
default_args = {
		'owner': 'bossa',
		'retries': 0,
        'schedule': "None"
}

@dag(dag_id='ETL_PEDSnet_to_PCORnet', 
     default_args=default_args,
     schedule_interval=None, 
     start_date=datetime(2023, 5, 25), 
     catchup=False,
     params = {
         'site': Param(None, type=['null','string']),
         "conn_id": Param('bossa_db06', type=['null','string'], enum=["bossa_db06"]),
         'database': Param(None, type=['null','string'])
        }
     )
#----------- Define Tasks within DAG Call -------------
def PEDSnet_to_PCORnet_ETL():

#----------- PHASE 1 Tasks - DDL Set Up -------------
    ddl_pcornet = MySQLExecuteQueryOperator(
        task_id="ddl_pcornet",
        conn_id= '{{ dag_run.conf["conn_id"] }}',
        hook_params={"schema": database_template},
        sql='etl_logic/sql/1_ddl_pcornet_v61.sql'
    )

    harvest_upload = MySQLExecuteQueryOperator(
    task_id = 'harvest_upload',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/1_harvest_upload.sql', 
   )

#     alter_tbl_owner = MySQLExecuteQueryOperator(
#     task_id = 'alter_tbl_owner',
#     conn_id = "{{dag_run.conf['conn_id']}}",
#     hook_params={"schema": database_template},
#     sql = 'etl_logic/sql/1_alter_tbl_owner.sql', 
#    )
    
    site_col = MySQLExecuteQueryOperator(
    task_id = 'site_col',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/1_site_col.sql', 
   )
    
    def load_maps(**kwargs):
        #connect to db
        conn = psycopg2.connect(
            host = kwargs['host'],
            database = kwargs['database'], 
            user = kwargs['user'], 
            password = kwargs['password']
            )
        cur = conn.cursor()

        #clean out old tables, generate empty new ones
        cur.execute("""CREATE SCHEMA IF NOT EXISTS pcornet_maps;""")
        conn.commit()
        cur.execute("""DROP TABLE IF EXISTS pcornet_maps.pedsnet_pcornet_valueset_map; DROP TABLE IF EXISTS pcornet_maps.chief_complaint_map;""")
        conn.commit()
        create_tables = """ 
        CREATE TABLE pcornet_maps.pedsnet_pcornet_valueset_map (
            source_concept_class character varying(200),
            target_concept character varying(200),
            pcornet_name character varying(200),
            source_concept_id character varying(200),
            concept_description character varying(200),
            value_as_concept_id character varying(200)
        );

        CREATE TABLE pcornet_maps.chief_complaint_map (
            observation_source_value character varying(256),
            count integer,
            condition_concept_id bigint,
            condition_concept_name character varying(256),
            concept_code character varying(256),
            vocabulary_id character varying(256),
            pcornet_condition_type character varying(256)
        );
        """
        cur.execute(create_tables)
        conn.commit()

       # populate pedsnet_pcornet_valueset_map from etl_logic/data/pedsnet_pcornet_valueset_map.csv
        print('Populating pedsnet_pcornet_valueset_map ...')
        columns = (
            "source_concept_class",
            "target_concept",
            "pcornet_name",
            "source_concept_id",
            "concept_description",
            "value_as_concept_id"
        )
        column_names = ','.join(columns)
        with open('/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/data/pedsnet_pcornet_valueset_map.csv', 'r') as f:
            copy_cmd = f"copy pedsnet_pcornet_valueset_map({column_names}) from stdout (format csv, HEADER TRUE)"
            cur.execute("SET search_path TO pcornet_maps;")
            cur.copy_expert(copy_cmd, f)
            conn.commit()
            print("loaded successfully...")

        # populate chief complaint
        print('Populating chief_complaint_map ...')
        columns = (
            "observation_source_value",
            "count",
            "condition_concept_id",
            "condition_concept_name",
            "concept_code",
            "vocabulary_id",
            "pcornet_condition_type"
        )
        column_names = ','.join(columns)
        with open('/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/data/chief_complaint_map.csv', 'r') as f:
            copy_cmd = f"copy chief_complaint_map({column_names}) from stdout (format csv, HEADER TRUE)"
            cur.execute("SET search_path TO pcornet_maps;")
            cur.copy_expert(copy_cmd, f)
            conn.commit()
            print("loaded successfully...")

        conn.close()

    create_mapping_tables = PythonOperator(
        task_id="create_mapping_tables",
        provide_context=True,
        python_callable=load_maps,
        op_kwargs={
            'host': "{{ conn[dag_run.conf['conn_id']].host }}", 
            'database' : "{{dag_run.conf['database']}}", 
            'user' : "{{ conn[dag_run.conf['conn_id']].login }}", 
            'password' : "{{ conn[dag_run.conf['conn_id']].password }}"
        },
    )
    
#----------- PHASE 2 Tasks - ETL PEDSnet Data into Empty PCORnet DDL -------------
    person_visit_start2001 = MySQLExecuteQueryOperator(
    task_id = 'person_visit_start2001',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/1_person_visit_start2001.sql', 
   )

    demographic = MySQLExecuteQueryOperator(
    task_id = 'demographic',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_demographic.sql', 
   )
    
    private_demographic = MySQLExecuteQueryOperator(
    task_id = 'private_demographic',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_private_demographic.sql', 
   )
    
    enrollment = MySQLExecuteQueryOperator(
    task_id = 'enrollment',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_enrollment.sql', 
   )

    condition = MySQLExecuteQueryOperator(
    task_id = 'condition',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_condition.sql', 
   )
    
    pro_cm = MySQLExecuteQueryOperator(
    task_id = 'pro_cm',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_pro_cm.sql', 
   )
    
    provider = MySQLExecuteQueryOperator(
    task_id = 'provider',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_provider.sql', 
   )
    
    encounter = MySQLExecuteQueryOperator(
    task_id = 'encounter',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_encounter.sql', 
   )

    encounter_vacuum = MySQLExecuteQueryOperator(
    task_id = 'encounter_vacuum',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    autocommit = True,
    sql = 'etl_logic/sql/2_encounter_vacuum.sql'
   ) 

    diagnosis = MySQLExecuteQueryOperator(
    task_id = 'diagnosis',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_diagnosis.sql', 
    )

    procedures = MySQLExecuteQueryOperator(
    task_id = 'procedures',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_procedures.sql', 
   )
    
    procedures_vacuum = MySQLExecuteQueryOperator(
    task_id = 'procedures_vacuum',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    autocommit = True,
    sql = 'etl_logic/sql/2_procedures_vacuum.sql'
   )

    death = MySQLExecuteQueryOperator(
    task_id = 'death',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_death.sql', 
   )

    death_cause = MySQLExecuteQueryOperator(
    task_id = 'death_cause',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_death_cause.sql',
    ) 

    lds_address_history = MySQLExecuteQueryOperator(
    task_id = 'lds_address_history',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_lds_address_history.sql',
    ) 

    private_address_geocode = MySQLExecuteQueryOperator(
    task_id = 'private_address_geocode',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_private_address_geocode.sql',
    ) 

    vital = TriggerDagRunOperator(
            task_id="vital",
            trigger_dag_id="spark_to_hdfs_to_postgres",
            conf={
                'site': "{{dag_run.conf['site']}}",
                'conn_id': "{{dag_run.conf['conn_id']}}",
                'database': "{{dag_run.conf['database']}}",
                'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_vitals.py',
                'final_table': 'vital',
                'cdm': 'pcornet_airflow'
            },
            wait_for_completion=True,
            poke_interval=10
    )

    obs_clin = TriggerDagRunOperator(
        task_id="obs_clin",
        trigger_dag_id="spark_to_hdfs_to_postgres",
        conf={
            'site': "{{dag_run.conf['site']}}",
            'conn_id': "{{dag_run.conf['conn_id']}}",
            'database': "{{dag_run.conf['database']}}",
            'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_obs_clin.py',
            'final_table': 'obs_clin',
            'cdm': 'pcornet_airflow'
        },
        wait_for_completion=True,
        poke_interval=10
    )

    obs_gen = TriggerDagRunOperator(
        task_id="obs_gen",
        trigger_dag_id="spark_to_hdfs_to_postgres",
        conf={
            'site': "{{dag_run.conf['site']}}",
            'conn_id': "{{dag_run.conf['conn_id']}}",
            'database': "{{dag_run.conf['database']}}",
            'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_obs_gen.py',
            'final_table': 'obs_gen',
            'cdm': 'pcornet_airflow'
        },
        wait_for_completion=True,
        poke_interval=10
   )

    dispensing = TriggerDagRunOperator(
        task_id="dispensing",
        trigger_dag_id="spark_to_hdfs_to_postgres",
        conf={
            'site': "{{dag_run.conf['site']}}",
            'conn_id': "{{dag_run.conf['conn_id']}}",
            'database': "{{dag_run.conf['database']}}",
            'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_dispensing.py',
            'final_table': 'dispensing',
            'cdm': 'pcornet_airflow'
        },
        wait_for_completion=True,
        poke_interval=10
    )

    prescribing = TriggerDagRunOperator(
        task_id="prescribing",
        trigger_dag_id="spark_to_hdfs_to_postgres",
        conf={
            'site': "{{dag_run.conf['site']}}",
            'conn_id': "{{dag_run.conf['conn_id']}}",
            'database': "{{dag_run.conf['database']}}",
            'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_prescribing.py',
            'final_table': 'prescribing',
            'cdm': 'pcornet_airflow'
        },
        wait_for_completion=True,
        poke_interval=10
    )

    immunization = TriggerDagRunOperator(
        task_id="immunization",
        trigger_dag_id="spark_to_hdfs_to_postgres",
        conf={
            'site': "{{dag_run.conf['site']}}",
            'conn_id': "{{dag_run.conf['conn_id']}}",
            'database': "{{dag_run.conf['database']}}",
            'pyspark_filepath': '/data/airflow/dags/PEDSnet_to_PCORnet_ETL/etl_logic/spark/2_immunization.py',
            'final_table': 'immunization',
            'cdm': 'pcornet_airflow'
        },
        wait_for_completion=True,
        poke_interval=10
    )

    med_admin = MySQLExecuteQueryOperator(
    task_id = 'med_admin',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_med_admin.sql',
    )

    before_lab_result_cm = MySQLExecuteQueryOperator(
    task_id = 'before_lab_result_cm',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_before_lab_result_cm.sql',
    )

    before_lab_result_cm_vacuum = MySQLExecuteQueryOperator(
    task_id = 'before_lab_result_cm_vacuum',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    autocommit = True,
    sql = 'etl_logic/sql/2_before_lab_result_vacuum.sql'
    )

    lab_qual_vacuum = MySQLExecuteQueryOperator(
    task_id = 'lab_qual_vacuum',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    autocommit = True,
    sql = 'etl_logic/sql/2_lab_qual_vacuum.sql'
    )

    lab_result_cm = MySQLExecuteQueryOperator(
    task_id = 'lab_result_cm',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_lab_result_cm.sql',
    )

    lab_result_cm_finalize = MySQLExecuteQueryOperator(
    task_id = 'lab_result_cm_finalize',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_lab_result_cm_finalize.sql',
    )

    lab_formatting = MySQLExecuteQueryOperator(
    task_id = 'lab_formatting',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/2_lab_formatting.sql',
    )

#----------- PHASE 3 Tasks - Clean Up, Indexes, Constraints -------------
    index = MySQLExecuteQueryOperator(
    task_id = 'index',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/3_index.sql',
    )

    constraints = MySQLExecuteQueryOperator(
    task_id = 'constraints',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/3_constraints.sql',
    )

    drop_tbl = MySQLExecuteQueryOperator(
    task_id = 'drop_tbl',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/3_drop_tbl.sql',
    )

    check_orphan_ids = MySQLExecuteQueryOperator(
    task_id = 'check_orphan_ids',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/3_check_orphan_ids.sql',
    )

    harvest = MySQLExecuteQueryOperator(
    task_id = 'harvest',
    conn_id = "{{dag_run.conf['conn_id']}}",
    hook_params={"schema": database_template},
    sql = 'etl_logic/sql/3_harvest.sql',
    )

#----------- Define Task Dependencies within DAG -------------
    # phase 1
    ddl_pcornet >> [harvest_upload,site_col, create_mapping_tables] >> person_visit_start2001

    #phase 2
    person_visit_start2001 >> demographic >> private_demographic
    person_visit_start2001 >> enrollment
    person_visit_start2001 >> condition
    person_visit_start2001 >> provider
    person_visit_start2001 >> pro_cm
    [person_visit_start2001, provider] >> encounter >> encounter_vacuum >> diagnosis
    [person_visit_start2001, encounter_vacuum] >> procedures >> procedures_vacuum
    person_visit_start2001 >> death >> death_cause
    person_visit_start2001 >> lds_address_history >> private_address_geocode
    person_visit_start2001 >> dispensing >> prescribing >> vital  >> obs_clin >> obs_gen
    [obs_gen, procedures_vacuum] >> immunization
    person_visit_start2001 >> med_admin
    person_visit_start2001 >> before_lab_result_cm >> before_lab_result_cm_vacuum >> lab_result_cm >> lab_qual_vacuum >> lab_result_cm_finalize >> lab_formatting

    # phase 3
    [private_demographic, enrollment, condition, pro_cm, diagnosis, immunization, 
    death_cause, private_address_geocode, med_admin, lab_formatting] >> drop_tbl
    drop_tbl >> check_orphan_ids >> index >> constraints >> harvest

PEDSnet_to_PCORnet_ETL()
