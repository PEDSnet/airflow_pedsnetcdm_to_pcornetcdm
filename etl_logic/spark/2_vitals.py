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
from pyspark.sql.functions import length, when, isnull, array_contains, to_date, col, to_timestamp, trim, date_format, concat, lit, coalesce, monotonically_increasing_id
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
#pcornet_maps.pedsnet_pcornet_valueset_map
print("Extracting pedsnet_pcornet_valueset_map from postgres into spark (1/5)")
pedsnet_pcornet_valueset_map = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'pcornet_maps.pedsnet_pcornet_valueset_map')\
    .option("user",user)\
    .option("password",password)\
    .load()
pedsnet_pcornet_valueset_map.createOrReplaceTempView("pedsnet_pcornet_valueset_map")
pedsnet_pcornet_valueset_map.cache()
print(pedsnet_pcornet_valueset_map.count())

#measurement
print("Extracting measurement from postgres into spark (2/5)")
query = "SELECT MIN(measurement_id), MAX(measurement_id) FROM {}_pedsnet.measurement".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
measurement = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.measurement'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","measurement_id")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
measurement.createOrReplaceTempView("measurement")
measurement.cache()
print(measurement.count())

# person_visit_start2001
print("Extracting person_visit_start2001 from postgres into spark (3/5)")
query = "SELECT MIN(visit_id), MAX(visit_id) FROM {}_pcornet_airflow.person_visit_start2001".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
pvs = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pcornet_airflow.person_visit_start2001'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","visit_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
pvs.createOrReplaceTempView("pvs")
pvs.cache()
print(pvs.count())

#observation
print("Extracting obs from postgres into spark (4/5)")
query = "SELECT MIN(observation_id), MAX(observation_id) FROM {}_pedsnet.observation".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
obs = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.observation'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","observation_id")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
obs.createOrReplaceTempView("obs")
obs.cache()
print(obs.count())

#fact_relationship
print("Extracting fact_rel from postgres into spark (5/5)")
query = "SELECT MIN(fact_id_1), MAX(fact_id_1) FROM {}_pedsnet.fact_relationship".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
fact_rel = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable","{}_pedsnet.fact_relationship".format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","fact_id_1")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
fact_rel.createOrReplaceTempView("fact_rel")
fact_rel.cache()
print(fact_rel.count())

#------------------------------ 2. Transform extracted data into finalized Spark Dataframe ------------------------------
#transform vitals from the meaasurement table into vitals format
ms_query = """
    select distinct 
        person_id, 
        '{}' as site, 
        measurement_id,
        visit_occurrence_id, 
        measurement_date, 
        measurement_datetime, 
        value_as_number,
        measurement_concept_id,
        measurement_source_value, 
        provider_id,
        operator_concept_id, 
        unit_concept_id, 
        unit_source_value, 
        value_as_concept_id,
	    value_source_value,
        measurement_type_concept_id
	from 
        measurement
	where 
        measurement_concept_id in (3023540, 3036277)
union
    select 
        person_id, 
        '{}' as site, 
        measurement_id,
        visit_occurrence_id, 
        measurement_date, 
        measurement_datetime, 
        value_as_number,
        measurement_concept_id,
        measurement_source_value, 
        provider_id,
        operator_concept_id, 
        unit_concept_id, 
        unit_source_value, 
        value_as_concept_id,
	    value_source_value,
        measurement_type_concept_id
	from 
        measurement
	where 
        measurement_concept_id in (3013762,3025315)
union
    select distinct 
        person_id, 
        '{}' as site, 
        measurement_id,
        visit_occurrence_id, 
        measurement_date, 
        measurement_datetime, 
        value_as_number,
        measurement_concept_id,
        measurement_source_value, 
        provider_id,
        operator_concept_id, 
        unit_concept_id, 
        unit_source_value, 
        value_as_concept_id,
	    value_source_value,
        measurement_type_concept_id
    from 
        measurement
    where  
        measurement_concept_id = 3038553
union
    select distinct 
        person_id, 
        '{}' as site, 
        measurement_id,
        visit_occurrence_id, 
        measurement_date, 
        measurement_datetime, 
        value_as_number,
        measurement_concept_id,
        measurement_source_value, 
        provider_id,
        operator_concept_id, 
        unit_concept_id, 
        unit_source_value, 
        value_as_concept_id,
	    value_source_value,
        measurement_type_concept_id
   from 
        measurement
   where 
        measurement_concept_id in (3018586,3035856,3009395,3004249)
union
    select distinct 
        person_id, 
        '{}' as site, 
        measurement_id,
        visit_occurrence_id, 
        measurement_date, 
        measurement_datetime, 
        value_as_number,
        measurement_concept_id,
        measurement_source_value, 
        provider_id,
        operator_concept_id, 
        unit_concept_id, 
        unit_source_value, 
        value_as_concept_id,
	    value_source_value,
        measurement_type_concept_id
	from 
        measurement
	where 
        measurement_concept_id in (3034703,3019962,3013940,3012888)
""".format(site,site,site,site,site)
print('transforming ms (1/10)')
ms = spark.sql(ms_query)
ms.createOrReplaceTempView("ms")
ms.cache()
print(ms.count())

measurement.unpersist()

ms_filter_query = """
select * 
from ms 
where EXTRACT(YEAR FROM measurement_date) >= 2001 and
      ms.person_id in (select person_id from pvs) and
      ms.visit_occurrence_id in (select visit_id from pvs)
"""
print('transforming ms_filter (2/10)')
ms_filter = spark.sql(ms_filter_query)
ms_filter.createOrReplaceTempView("ms_filter")
ms_filter.cache()
print(ms_filter.count())

ms.unpersist()

#transform tobacco and smoking info from the observation table into vitals format
obs_fr_query = """
    select distinct observation_id, observation_concept_id,observation_source_value,person_id, obs.value_as_concept_id, visit_occurrence_id, qualifier_source_value, observation_date, observation_datetime, provider_id,
    value_as_string, coalesce(m1.target_concept,'OT') as tobacco, f.fact_id_2, qualifier_concept_id,obs.site
	from obs
	left join pedsnet_pcornet_valueset_map m1 on cast(obs.value_as_concept_id as varchar(20)) = m1.value_as_concept_id
	left join fact_rel f on obs.observation_id = f.fact_id_1
	where observation_concept_id = 4005823 and m1.source_concept_class = 'tobacco'
"""
print('transforming obs_fr (3/10)')
obs_fr = spark.sql(obs_fr_query)
obs_fr.createOrReplaceTempView("obs_fr")
obs_fr.cache()
print(obs_fr.count())

obs_tobacco_query = """
    select distinct visit_occurrence_id, observation_concept_id, observation_source_value,obs.value_as_concept_id, observation_date, observation_datetime, coalesce(m2.target_concept,'OT') as tobacco_type, qualifier_source_value,observation_id, qualifier_concept_id,obs.site,provider_id
	from obs 
	left join pedsnet_pcornet_valueset_map m2 on cast(obs.value_as_concept_id as varchar(20)) = m2.value_as_concept_id
	where observation_concept_id = 4219336 and m2.source_concept_class = 'tobacco type'
	and EXTRACT(YEAR FROM observation_date) >= 2001 and
      person_id in (select person_id from pvs) and
      visit_occurrence_id in (select visit_id from pvs)
"""
print('transforming obs_tobacco (4/10)')
obs_tobacco = spark.sql(obs_tobacco_query)
obs_tobacco.createOrReplaceTempView("obs_tobacco")
obs_tobacco.cache()
print(obs_tobacco.count())

obs_smoking_query = """
    select distinct observation_id, observation_source_value, visit_occurrence_id, obs.value_as_concept_id, observation_date, observation_datetime, coalesce(m3.target_concept,'OT') as smoking, observation_concept_id, qualifier_concept_id,obs.site,qualifier_source_value,provider_id
	from obs 
    left join pedsnet_pcornet_valueset_map m3 on cast(obs.value_as_concept_id as varchar(20)) = m3.value_as_concept_id
	where observation_concept_id = 4275495 and m3.source_concept_class = 'smoking'
	and EXTRACT(YEAR FROM observation_date) >= 2001 and
    person_id in (select person_id from pvs) and
    visit_occurrence_id in (select visit_id from pvs)
"""
print('transforming obs_smoking (5/10)')
obs_smoking = spark.sql(obs_smoking_query)
obs_smoking.createOrReplaceTempView("obs_smoking")
obs_smoking.cache()
print(obs_smoking.count())

obs.unpersist()

obs_final_query = """
    select obs_fr.person_id,obs_fr.visit_occurrence_id, obs_fr.provider_id, obs_fr.observation_date, obs_fr.value_as_concept_id, obs_tobacco.observation_concept_id as obs_concept_id_typ, 
	obs_smoking.observation_concept_id as obs_concept_id_smk,obs_fr.observation_concept_id, obs_fr.observation_datetime, obs_fr.tobacco, obs_tobacco.tobacco_type, obs_smoking.smoking, obs_fr.value_as_string,
	obs_fr.observation_id, obs_fr.observation_source_value,obs_fr.qualifier_concept_id,obs_fr.site, obs_fr.qualifier_source_value
	from obs_fr
	left join obs_tobacco on  obs_fr.fact_id_2 = obs_tobacco.observation_id
    left join obs_smoking on obs_fr.fact_id_2 = obs_smoking.observation_id
"""
print('transforming obs_final (6/10)')
obs_final = spark.sql(obs_final_query)
obs_final.createOrReplaceTempView("obs_final")
obs_final.cache()
print(obs_final.count())

obs_fr.unpersist()
obs_tobacco.unpersist()
obs_smoking.unpersist()

# --- Union final DFs, add id field, and format before loading to HDFS ---
ms_all_query = """select distinct
ms_filter.person_id, ms_filter.visit_occurrence_id, ms_filter.measurement_date, ms_filter.measurement_datetime,  
ms_ht.value_as_number as value_as_number_ht,  ms_wt.value_as_number as  value_as_number_wt,
ms_bmi.value_as_number as value_as_number_original_bmi,
ms_dia.value_as_number as value_as_number_diastolic,
ms_sys.value_as_number as value_as_number_systolic,
ms_sys.measurement_concept_id as measurement_concept_id_sys,
ms_sys.measurement_source_value as measurement_source_value_sys, 
ms_dia.measurement_source_value as measurement_source_value_dia, 
ms_filter.site
FROM ms_filter
left join 
    ms_filter ms_ht 
    on ms_filter.visit_occurrence_id = ms_ht.visit_occurrence_id
    and ms_filter.measurement_datetime = ms_ht.measurement_datetime 
    and ms_ht.measurement_concept_id = 3023540
left join 
    ms_filter ms_wt 
    on ms_filter.visit_occurrence_id = ms_wt.visit_occurrence_id
    and ms_filter.measurement_datetime = ms_wt.measurement_datetime 
    and ms_wt.measurement_concept_id = 3013762
left join 
    ms_filter ms_bmi 
    on ms_filter.visit_occurrence_id = ms_bmi.visit_occurrence_id
    and ms_filter.measurement_datetime = ms_bmi.measurement_datetime
    and ms_bmi.measurement_concept_id = 3038553
left join 
    ms_filter ms_sys 
    on ms_filter.visit_occurrence_id = ms_sys.visit_occurrence_id
    and ms_filter.measurement_datetime = ms_sys.measurement_datetime
    and ms_sys.measurement_concept_id in (3018586,3035856,3009395,3004249)
left join 
    fact_rel fr1 
    on fr1.fact_id_1 = ms_sys.measurement_id 
    AND fr1.domain_concept_id_1=21 
    AND fr1.domain_concept_id_2=21
left join 
    ms_filter ms_dia 
    on ms_filter.visit_occurrence_id = ms_dia.visit_occurrence_id
    and ms_dia.measurement_concept_id in (3034703,3019962,3013940,3012888)
    and ms_dia.measurement_id= fr1.fact_id_2
"""
print('transforming ms_all (7/10)')
ms_all = spark.sql(ms_all_query)
ms_all.createOrReplaceTempView("ms_all")
ms_all.cache()
print(ms_all.count())

fact_rel.unpersist()
ms_filter.unpersist()

vital_all_query = """
select distinct
ms_all.person_id, ms_all.visit_occurrence_id, ms_all.measurement_date, ms_all.measurement_datetime,  
value_as_number_ht, value_as_number_wt, value_as_number_diastolic,value_as_number_systolic,value_as_number_original_bmi,
measurement_concept_id_sys,tobacco,tobacco_type,smoking,measurement_source_value_sys, measurement_source_value_dia, 
ms_all.site
FROM ms_all
left join obs_final on ms_all.visit_occurrence_id = obs_final.visit_occurrence_id
and ms_all.measurement_datetime = obs_final.observation_datetime
where coalesce(value_as_number_ht, value_as_number_wt, value_as_number_diastolic, value_as_number_systolic, value_as_number_original_bmi) is not null
and ms_all.person_id in (select person_id from pvs)
and (ms_all.visit_occurrence_id in (select visit_id from pvs) or ms_all.visit_occurrence_id is null)
"""
print('transforming vital_all (8/10)')
vital_all = spark.sql(vital_all_query)
vital_all.createOrReplaceTempView("vital_all")
vital_all.cache()
print(vital_all.count())

pvs.unpersist()
obs_final.unpersist()

vital_query = """
SELECT distinct
cast(coalesce(m.target_concept,'OT') as varchar(2)) as bp_position, 
cast(value_as_number_diastolic as Double) as diastolic,
cast(visit_occurrence_id as varchar(20)) as encounterid,
cast((value_as_number_ht*0.393701) as Double) as ht,
cast(cast(date_part('year', measurement_date) as varchar(20))||'-'||lpad(cast(date_part('month', measurement_date) as varchar(20)),2,'0')||'-'||lpad(cast(date_part('day', measurement_date) as varchar(20)),2,'0') as date) as measure_date,
lpad(cast(date_part('hour', measurement_datetime) as varchar(20)),2,'0')||':'||lpad(cast(date_part('minute', measurement_datetime) as varchar(20)),2,'0') as measure_time,
cast(value_as_number_original_bmi as Double) as original_bmi,
cast(person_id as varchar(20)) as patid,
cast(null as varchar(200)) as raw_bp_position,
cast(measurement_source_value_dia as varchar(200))  as raw_diastolic,
cast(smoking as varchar(200)) as raw_smoking,
cast(measurement_source_value_sys as varchar(200))  as raw_systolic,
cast(tobacco as varchar(200)) as raw_tobacco,
cast(tobacco_type as varchar(200)) as raw_tobacco_type,
cast(smoking as varchar(2)) as smoking,
cast(value_as_number_systolic as Double) as systolic,
cast(tobacco as varchar(2)) as tobacco,
cast(tobacco_type as varchar(2)) as tobacco_type,
'HC' as vital_source,
cast((value_as_number_wt*2.20462) as Double) as wt,
site as site
FROM vital_all
left join pedsnet_pcornet_valueset_map m on cast(measurement_concept_id_sys as varchar(20)) = m.source_concept_id
	AND m.source_concept_class='BP Position'
"""
print('transforming vital (9/10)')
vital = spark.sql(vital_query)
vital.createOrReplaceTempView("vital")
vital.repartition(1000)
vital.cache()
print(vital.count())

pedsnet_pcornet_valueset_map.unpersist()
vital_all.unpersist()

print('format and add id (10/10)')
vital = vital.withColumn('vitalid', monotonically_increasing_id())
vital = vital.select( 
    'bp_position',
    'diastolic',
    'encounterid',
    'ht',
    'measure_date',
    'measure_time',
    'original_bmi',
    'patid',
    'raw_bp_position',
    'raw_diastolic',
    'raw_smoking',
    'raw_systolic',
    'raw_tobacco',
    'raw_tobacco_type',
    'smoking',
    'systolic',
    'tobacco',
    'tobacco_type',
    'vital_source',
    'vitalid',
    'wt',
    'site'
)
vital.createOrReplaceTempView("vital")
vital.repartition(1000)
vital.cache()
print(vital.count())

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
print("writing to hdfs")
vital.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_vital.csv".format(site))
