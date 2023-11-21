#------------------------------ Imports ------------------------------
import csv
import json
import os
import psycopg2
import re
import sys
import time
import traceback

from io import StringIO
from typing import Dict
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, when, isnull, array_contains, to_date, col, to_timestamp, trim, date_format, concat, lit, coalesce
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType, DecimalType, TimestampType, LongType, DoubleType

#------------------------------ Establish Connections ------------------------------
spark = SparkSession.builder \
    .appName("postgres_read_write") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
 
#set the values for the host information
host = os.environ['host']
port = '5432'
dbname = os.environ['database']
user = os.environ['user']
password = os.environ['pass']
site = os.environ['site']
 
properties = {"user": f"{user}", "password": f"{password}"}
url = f"jdbc:postgresql://{host}:{port}/{dbname}"

#------------------------------ 1. Extract Data from Postgres into Spark Dataframes ------------------------------
#person_visit_start2001
print("Extracting person_visit_start2001 from postgres into spark (1/5)")
query = "SELECT MIN(visit_id), MAX(visit_id) FROM {}_pcornet_airflow.person_visit_start2001".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
person_visit_start2001 = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pcornet_airflow.person_visit_start2001'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","visit_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
person_visit_start2001.createOrReplaceTempView("person_visit_start2001")
person_visit_start2001.cache()
print(person_visit_start2001.count())

# vocabulary.concept
print("Extracting concept from postgres into spark (2/5)")
query = "SELECT MIN(concept_id), MAX(concept_id) FROM vocabulary.concept"
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
concept = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'vocabulary.concept')\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","concept_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
concept.createOrReplaceTempView("concept")
concept.cache()
print(concept.count())

#pedsnet_pcornet_valueset_map
print("Extracting pedsnet_pcornet_valueset_map from postgres into spark (3/5)")
pedsnet_pcornet_valueset_map = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'pcornet_maps.pedsnet_pcornet_valueset_map')\
    .option("user",user)\
    .option("password",password)\
    .load()
pedsnet_pcornet_valueset_map.createOrReplaceTempView("pedsnet_pcornet_valueset_map")
pedsnet_pcornet_valueset_map.cache()
print(pedsnet_pcornet_valueset_map.count())

#procedure_occurrence
print("Extracting procedure_occurrence from postgres into spark (4/5)")
query = "SELECT MIN(procedure_occurrence_id), MAX(procedure_occurrence_id) FROM {}_pedsnet.procedure_occurrence".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
procedure_occurrence = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.procedure_occurrence'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","procedure_occurrence_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
procedure_occurrence.createOrReplaceTempView("procedure_occurrence")
procedure_occurrence.cache()
print(procedure_occurrence.count())

#immunization
print("Extracting immunization from postgres into spark (5/5)")
query = "SELECT MIN(immunization_id), MAX(immunization_id) FROM {}_pedsnet.immunization".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
immunization = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.immunization'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","immunization_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
immunization.createOrReplaceTempView("immunization")
immunization.cache()
print(immunization.count())

#------------------------------ 2. Transform extracted data into finalized Spark Dataframe ------------------------------
# --- Transform immunization data ---
imm_code_query = """
    select
        cast(imm.immunization_id as varchar(256)) as immunizationid,
        cast(imm.person_id as varchar(256)) as patid,
        cast(imm.visit_occurrence_id as varchar(256)) as encounterid,
        imm.procedure_occurrence_id as procedure_occurrence_id,
        cast(imm.provider_id as varchar(256)) as vx_providerid,
        imm.imm_recorded_date  as vx_record_date,
        imm.immunization_date as vx_admin_date,
        left(case 
                when imm.immunization_concept_id != 0 then code.concept_code 
                when left(trim(split_part(imm.immunization_source_value,'|',2)),11) != '' then left(trim(split_part(imm.immunization_source_value,'|',2)),11)
                when lower(imm.immunization_source_value) like '%hib (prp-t)%' then '48'
                when lower(imm.immunization_source_value) like '%influenza, inj., mdck, pf, quad%' then '171'
                when lower(imm.immunization_source_value) like '%influenza, recombinant, quadrivalent, pf%' then '185'
                when lower(imm.immunization_source_value) like '%influenza, quadrivalent, pf, pediatrics%' then '185'
                when lower(imm.immunization_source_value) like '%ppd test%' then '98'
                when lower(imm.immunization_source_value) like '%td (adult),2 lf tetanus toxoid,preserv vaccine%' then '9'
                when lower(imm.immunization_source_value) like '%tdap vaccine%' then '115'
                when lower(imm.immunization_source_value) like '%dtap/hepb/ipv combined vaccine%' then '146'
                when lower(imm.immunization_source_value) like '%hib (prp-omp)%' then '49'
                when lower(imm.immunization_source_value) like '%influenza split high dose pf vaccine%' then '15'
                when lower(imm.immunization_source_value) like '%mmr/varicella combined vaccined%' then '94'
                when lower(imm.immunization_source_value) like '%pr pfizer sars-cov-2 vaccine%' then '208'
                when lower(imm.immunization_source_value) like '%pfizer sars-cov-2 vaccination%' then '208'
                when lower(imm.immunization_source_value) like '%covid-19, mrna, lnp-s, pf, 30 mcg/0.3 ml dose%' then '208'
                when lower(imm.immunization_source_value) like '%pfizer covid-19%' then '208'
                when lower(imm.immunization_source_value) like '%sars-cov-2 (pfizer)%' then '208'
                when lower(imm.immunization_source_value) like '%moderna sars-cov-2 vaccine%' then '207'
                when lower(imm.immunization_source_value) like '%moderna covid-19%' then '207'
                when lower(imm.immunization_source_value) like '%sars-cov-2 (moderna)%' then '207'
                when lower(imm.immunization_source_value) like '%moderna sars-cov-2 vaccination%' then '207'
                when lower(imm.immunization_source_value) like '%janssen sars-cov-2 vaccine%' then '212'
                when lower(imm.immunization_source_value) like '%janssen (j&j) covid-19%' then '212'
                when lower(imm.immunization_source_value) like '%sars-cov-2 (janssen)%' then '212'
                when lower(imm.immunization_source_value) like '%covid-19 vaccine (not specified)%' then '213'
                when lower(imm.immunization_source_value) like '%sars-cov-2, unspecified%' then '213'
                else '999'
            end, 
            11) as vx_code,
        'CP' as vx_status,
        null as vx_status_reason,
        coalesce(imm_type.concept_code,'OT') as vx_source, 
        case 
		    when length(imm.immunization_dose) <= 8 then cast(nullif(regexp_replace(imm.immunization_dose, '[^0-9.]*','','g'), '') as Decimal)
		    else cast(left(nullif(regexp_replace(imm.immunization_dose, '[^0-9.]*','','g'), ''),8) as Decimal(20, 5))
	    end as vx_dose,        
        coalesce(dose_unit.target_concept, 'OT') as vx_dose_unit,        
	    coalesce(m3.target_concept, 'OT') as vx_route,
        imm.imm_body_site_concept_id, 
        imm.imm_body_site_source_value, 
        imm.imm_manufacturer,		
        replace(imm.imm_lot_num,'"','') as vx_lot_num,
        imm.imm_exp_date as vx_exp_date,
        replace(code.concept_name,',','') as raw_vx_name,
        code.concept_code as raw_vx_code,
        code.vocabulary_id as raw_vx_code_type,
        imm.immunization_dose as raw_vx_dose,
        imm.imm_dose_unit_source_value as raw_vx_dose_unit,
        imm.imm_route_source_value as raw_vx_route,
        'CP' as raw_vx_status,
        null as raw_vx_status_reason,
        imm.site as site
    from 
        immunization imm		
    left join 
        concept code 
        on code.concept_id = imm.immunization_concept_id
    left join 
        concept imm_type 
        on imm_type.concept_id = imm.immunization_type_concept_id 
        and imm_type.domain_id = 'Immunization Type' 
        and imm_type.vocabulary_id = 'PEDSnet'											 																	   
    left join 
        pedsnet_pcornet_valueset_map dose_unit 
        on cast(imm.imm_dose_unit_concept_id as varchar(256)) = dose_unit.source_concept_id
        and dose_unit.source_concept_class='Dose unit'
    left join 
        pedsnet_pcornet_valueset_map m3 
        on cast(imm.imm_route_concept_id as varchar(256)) = m3.source_concept_id
        and m3.source_concept_class='Route'
"""

print('transforming imm_code (1/4)')
imm_code = spark.sql(imm_code_query)
imm_code.createOrReplaceTempView("imm_code")
imm_code.cache()
print(imm_code.count())

immunization.unpersist()

imm_metadata_query = """
    select 
        immunizationid,
        patid,
        encounterid,
        imm.procedure_occurrence_id,
        vx_providerid,
        vx_record_date, 
        vx_admin_date,
        case 
            when imm.raw_vx_code_type = 'CVX' then 'CX' 
            when imm.raw_vx_code_type = 'NDC' then 'ND'
            when lower(imm.raw_vx_code_type) like 'rxnorm%' then 'RX'
            when imm.raw_vx_code_type = 'CPT4' or imm.raw_vx_code_type = 'HCPCS' then 'CH'
            when c.concept_code is not null then 'CX'
            else 'UN'
        end as vx_code_type,
        vx_code, 
        vx_status,
        vx_status_reason,
        vx_source, 
        vx_dose,
        vx_dose_unit, 
        vx_route,
	    coalesce(bdy_site.target_concept,bdy_site_src.target_concept,'NI') as vx_body_site, 
	    imm.imm_body_site_source_value as raw_vx_body_site,
        case 
            when imm.imm_manufacturer is null or imm.imm_manufacturer = '' then 'NI'
            else coalesce(manf_1.target_concept,manf_2.target_concept,'OTH') 
        end as vx_manufacturer,
        vx_lot_num,
        vx_exp_date,
        raw_vx_name,
        raw_vx_code,
        raw_vx_code_type, 
        raw_vx_dose,
        raw_vx_dose_unit,
        raw_vx_route,
        raw_vx_status,
        raw_vx_status_reason,
        null as raw_vx_manufacturer,
        imm.site
from 
	imm_code imm																																				 
left join 
	pedsnet_pcornet_valueset_map manf_1 
	on lower(manf_1.pcornet_name) like '%'||lower(imm.imm_manufacturer)||'%' 
	and manf_1.source_concept_class = 'vx_manufacturer_source'
left join 
	pedsnet_pcornet_valueset_map manf_2 
	on manf_2.source_concept_id = imm.imm_manufacturer 
	and manf_2.source_concept_class = 'vx_manufacturer'
left join 
	pedsnet_pcornet_valueset_map bdy_site 
	on cast(imm.imm_body_site_concept_id as varchar(264)) = bdy_site.source_concept_id 
	and bdy_site.source_concept_class = 'imm_body_site' 
	and bdy_site.source_concept_id is not null
left join 
	pedsnet_pcornet_valueset_map bdy_site_src 
	on lower(bdy_site_src.source_concept_id) like '%'||lower(trim(split_part(imm_body_site_source_value,'|',1)))||'%' 
	and bdy_site.source_concept_class = 'imm_body_site_source'
left join 
    concept c
    on vocabulary_id = 'CVX'	
    and imm.vx_code = c.concept_code 	
where
    (
    encounterid is null 
    or cast(encounterid as integer) IN (select visit_id from person_visit_start2001)
    )
    and cast(patid as integer) in (select person_id from person_visit_start2001)
"""

print('transforming imm_metadata (2/4)')
imm_metadata = spark.sql(imm_metadata_query)
imm_metadata.createOrReplaceTempView("imm_metadata")
imm_metadata.cache()
print(imm_metadata.count())

person_visit_start2001.unpersist()
imm_code.unpersist()

imm_proc_query = """
    select 
        immunizationid,
        patid,
        encounterid,
        cast(proc.procedure_occurrence_id as varchar(256)) as proceduresid,
        vx_providerid,
        vx_record_date, 
        vx_admin_date,
        vx_code_type,
        vx_code, 
        vx_status,
        vx_status_reason,
        vx_source, 
        vx_dose,
        vx_dose_unit, 
        vx_route,
	    vx_body_site, 
	    raw_vx_body_site,
        vx_manufacturer,
        vx_lot_num,
        vx_exp_date,
        raw_vx_name,
        raw_vx_code,
        raw_vx_code_type, 
        raw_vx_dose,
        raw_vx_dose_unit,
        raw_vx_route,
        raw_vx_status,
        raw_vx_status_reason,
        raw_vx_manufacturer,
        imm.site
from 
	imm_metadata imm																																				 	
left join 
    procedure_occurrence proc 
    on proc.procedure_occurrence_id = imm.procedure_occurrence_id	
"""

print('transforming imm_proc (3/4)')
imm_proc = spark.sql(imm_proc_query)
imm_proc.createOrReplaceTempView("imm_proc")
imm_proc.cache()
print(imm_proc.count())

procedure_occurrence.unpersist()
imm_metadata.unpersist()

# --- drop duplicates, and format before loading to HDFS ---
immunization_query = """
    select
        cast(first(encounterid) as VARCHAR(256)) as encounterid, 
        cast(immunizationid as VARCHAR(256)) as immunizationid, 
        cast(first(patid) as VARCHAR(256)) as patid, 
        cast(first(proceduresid) as VARCHAR(256)) as proceduresid, 
        cast(first(raw_vx_body_site) as VARCHAR(256)) as raw_vx_body_site, 
        cast(first(raw_vx_code) as VARCHAR(256)) as raw_vx_code, 
        cast(first(raw_vx_code_type) as VARCHAR(256)) as raw_vx_code_type, 
        cast(first(raw_vx_dose) as VARCHAR(256)) as raw_vx_dose, 
        cast(first(raw_vx_dose_unit) as VARCHAR(256)) as raw_vx_dose_unit, 
        cast(first(raw_vx_manufacturer) as VARCHAR(256)) as raw_vx_manufacturer, 
        cast(first(raw_vx_name) as VARCHAR(256)) as raw_vx_name, 
        cast(first(raw_vx_route) as VARCHAR(256)) as raw_vx_route, 
        cast(first(raw_vx_status) as VARCHAR(256)) as raw_vx_status, 
        cast(first(raw_vx_status_reason) as VARCHAR(256)) as raw_vx_status_reason, 
        cast(first(vx_admin_date) as DATE) as vx_admin_date, 
        cast(first(vx_body_site) as VARCHAR(256)) as vx_body_site, 
        cast(first(vx_code) as VARCHAR(11)) as vx_code, 
        cast(first(vx_code_type) as VARCHAR(2)) as vx_code_type, 
        cast(first(vx_dose) as Decimal(20, 5)) as vx_dose, 
        cast(first(vx_dose_unit) as VARCHAR(256)) as vx_dose_unit, 
        cast(first(vx_exp_date) as DATE) as vx_exp_date, 
        cast(first(vx_lot_num) as VARCHAR(256)) as vx_lot_num, 
        cast(first(vx_manufacturer) as VARCHAR(256)) as vx_manufacturer, 
        cast(first(vx_providerid) as VARCHAR(256)) as vx_providerid, 
        cast(first(vx_record_date) as DATE) as vx_record_date, 
        cast(first(vx_route) as VARCHAR(256)) as vx_route, 
        cast(first(vx_source) as VARCHAR(2)) as vx_source, 
        cast(first(vx_status) as VARCHAR(2)) as vx_status, 
        cast(first(vx_status_reason) as VARCHAR(2)) as vx_status_reason
    from
        imm_proc
    group by
        immunizationid
"""

print('transforming immunization (4/4)')
immunization = spark.sql(immunization_query)
immunization.createOrReplaceTempView("immunization")
immunization.cache()
print(immunization.count())

imm_metadata.unpersist()

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
print("writing obs_gen to hdfs")
immunization.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_immunization.csv".format(site))