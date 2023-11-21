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
# person_visit_start2001
print("Extracting person_visit_start2001 from postgres into spark (1/10)")
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

#measurement
print("Extracting measurement from postgres into spark (2/10)")
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

# vocabulary.concept
print("Extracting concept from postgres into spark (3/10)")
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

#person
print("Extracting person from postgres into spark (4/10)")
query = "SELECT MIN(person_id), MAX(person_id) FROM {}_pedsnet.person".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
person = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.person'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","person_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
person.createOrReplaceTempView("person")
person.cache()
print(person.count())

#pedsnet_pcornet_valueset_map
print("Extracting pedsnet_pcornet_valueset_map from postgres into spark (5/10)")
pedsnet_pcornet_valueset_map = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'pcornet_maps.pedsnet_pcornet_valueset_map')\
    .option("user",user)\
    .option("password",password)\
    .load()
pedsnet_pcornet_valueset_map.createOrReplaceTempView("pedsnet_pcornet_valueset_map")
pedsnet_pcornet_valueset_map.cache()
print(pedsnet_pcornet_valueset_map.count())

#adt_occurrence
print("Extracting adt_occurrence from postgres into spark (6/10)")
query = "SELECT MIN(adt_occurrence_id), MAX(adt_occurrence_id) FROM {}_pedsnet.adt_occurrence".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
adt_occurrence = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.adt_occurrence'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","adt_occurrence_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
adt_occurrence.createOrReplaceTempView("adt_occurrence")
adt_occurrence.cache()
print(adt_occurrence.count())

#visit_occurrence
print("Extracting visit_occurrence from postgres into spark (7/10)")
query = "SELECT MIN(visit_occurrence_id), MAX(visit_occurrence_id) FROM {}_pedsnet.visit_occurrence".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
visit_occurrence = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.visit_occurrence'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","visit_occurrence_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
visit_occurrence.createOrReplaceTempView("visit_occurrence")
visit_occurrence.cache()
print(visit_occurrence.count())

#device_exposure
print("Extracting device_exposure from postgres into spark (8/10)")
query = "SELECT MIN(device_exposure_id), MAX(device_exposure_id) FROM {}_pedsnet.device_exposure".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
device_exposure = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.device_exposure'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","device_exposure_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
device_exposure.createOrReplaceTempView("device_exposure")
device_exposure.cache()
print(device_exposure.count())

#location_history
print("Extracting location_history from postgres into spark (9/10)")
query = "SELECT MIN(location_history_id), MAX(location_history_id) FROM {}_pedsnet.location_history".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lh_lowerBound, lh_upperBound = min_max_df.collect()[0]
if(lh_lowerBound is not None):
    location_history = spark.read.format("jdbc")\
        .option("url",url)\
        .option("dbtable",'{}_pedsnet.location_history'.format(site))\
        .option("user",user)\
        .option("password",password)\
        .option("partitionColumn","location_history_id")\
        .option("numPartitions",200)\
        .option("lowerBound",lh_lowerBound)\
        .option("upperBound", lh_upperBound)\
        .load()
    location_history.createOrReplaceTempView("location_history")
    location_history.cache()
    print(location_history.count())

#location_fips
print("Extracting location_fips from postgres into spark (10/10)")
query = "SELECT MIN(geocode_id), MAX(geocode_id) FROM {}_pedsnet.location_fips".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lf_lowerBound, lf_upperBound = min_max_df.collect()[0]
if(lf_lowerBound is not None):
    location_fips = spark.read.format("jdbc")\
        .option("url",url)\
        .option("dbtable",'{}_pedsnet.location_fips'.format(site))\
        .option("user",user)\
        .option("password",password)\
        .option("partitionColumn","geocode_id")\
        .option("numPartitions",200)\
        .option("lowerBound",lf_lowerBound)\
        .option("upperBound", lf_upperBound)\
        .load()
    location_fips.createOrReplaceTempView("location_fips")
    location_fips.cache()
    print(location_fips.count())

#------------------------------ 2. Transform extracted data into finalized Spark Dataframe ------------------------------
# --- Transform adt_occurrence data ---
filter_adt_query = """
    select 
        adt_occurrence_id,
        visit_occurrence_id,
        person_id,
        adt_date,
        adt_datetime,
        service_concept_id,
        site
    from 
        adt_occurrence
    where 
        service_concept_id in (2000000079,2000000080,2000000078)
"""

print('transforming filter_adt (1/8)')
filter_adt = spark.sql(filter_adt_query)
filter_adt.createOrReplaceTempView("filter_adt")
filter_adt.cache()
print(filter_adt.count())

adt_obs_query = """
select 
    'a'|| cast(adt.adt_occurrence_id as varchar(256)) as obsgenid,
    cast(adt.person_id as varchar(256)) as patid,
    cast(enc.visit_occurrence_id as varchar(256)) as encounterid,
    cast(enc.provider_id as varchar(256)) as obsgen_providerid,
    cast(adt.adt_date as date) as obsgen_start_date,
    LPAD(cast(date_part('hour',adt.adt_datetime) as varchar(256)),2,'0') || ':' || LPAD(cast(date_part('minute',adt.adt_datetime) as varchar(256)),2,'0') as obsgen_start_time,
    'PC_COVID' as obsgen_type,
    '2000' as obsgen_code,
    null as obsgen_result_qual,
    null as obsgen_abn_ind,
    case 
        when adt.service_concept_id in (2000000079,2000000080,2000000078) then 'Y' 
        else 'N' 
    end as obsgen_result_text,
    null as obsgen_result_num,
    null as obsgen_result_modifier,
    null as obsgen_result_unit,
    null as obsgen_table_modified,
    null as obsgen_id_modified,
    'DR' as obsgen_source,
    null as raw_obsgen_name,
    null as raw_obsgen_type,
    null as raw_obsgen_code,
    null as raw_obsgen_result,
    null as raw_obsgen_unit,
    case 
        when enc.visit_end_datetime is not null then LPAD(cast(date_part('hour',enc.visit_end_datetime) as varchar(256)),2,'0') || ':' || LPAD(cast(date_part('minute',enc.visit_end_datetime) as varchar(256)),2,'0')
        else null
    end as obsgen_stop_time,
    cast(enc.visit_end_date as date) as obsgen_stop_date,
    adt.site
from 
    filter_adt adt 
inner join 
    visit_occurrence enc 
    on cast(enc.visit_occurrence_id as integer) = adt.visit_occurrence_id 
    and enc.visit_start_date = adt.adt_date
"""

print('transforming adt_obs (2/8)')
adt_obs = spark.sql(adt_obs_query)
adt_obs.createOrReplaceTempView("adt_obs")
adt_obs.cache()
print(adt_obs.count())

adt_occurrence.unpersist()
filter_adt.unpersist()

# --- Transform measurement data ---
meas_obs_filt_query = """
select 
    'm'|| cast(meas.measurement_id as varchar(256)) as obsgenid,
    cast(meas.person_id as varchar(256)) as patid,
    cast(meas.visit_occurrence_id as varchar(256)) as encounterid,
    cast(meas.provider_id as varchar(256)) as obsgen_providerid,
    cast(meas.measurement_date as date) as obsgen_start_date,
    LPAD(cast(date_part('hour',measurement_datetime) as varchar(256)),2,'0') || ':' || LPAD(cast(date_part('minute',measurement_datetime) as varchar(256)),2,'0') as obsgen_start_time,
    'PC_COVID' as obsgen_type,
    '1000' as obsgen_code,
    meas.value_source_value,
    meas.value_as_concept_id,
    case 
        when meas.measurement_concept_id in (2000001422) then 'Y' 
        else 'N' 
        end as obsgen_result_text,
    meas.value_as_number as obsgen_result_num,
    meas.operator_concept_id,
    meas.unit_concept_id, 
    meas.unit_source_value,
    null as obsgen_table_modified,
    null as obsgen_id_modified,
    'HC' as obsgen_source,
    null as raw_obsgen_name,
    null as raw_obsgen_type,
    null as raw_obsgen_code,
    meas.value_as_number as raw_obsgen_result,
    meas.unit_concept_name || meas.unit_source_value as raw_obsgen_unit,
    meas.site,
    null as obsgen_stop_time,
    null as obsgen_stop_date
from 
    measurement meas 
where 
    meas.measurement_concept_id in (2000001422,4353936)
"""

print('transforming meas_obs_filt (3/8)')
meas_obs_filt = spark.sql(meas_obs_filt_query)
meas_obs_filt.createOrReplaceTempView("meas_obs_filt")
meas_obs_filt.cache()
print(meas_obs_filt.count())

measurement.unpersist()

meas_obs_query = """
select 
    obsgenid,
    patid,
    encounterid,
    obsgen_providerid, 
    obsgen_start_date,
    obsgen_start_time,
    obsgen_type,
    obsgen_code,
    coalesce(
        map_qual.target_concept, 
        case
            when lower(value_source_value) like '%1+%' then '1+'
            when lower(value_source_value) like '%2+%' then '2+'
            when lower(value_source_value) like '%3+%' then '3+'
            when lower(value_source_value) like '%a neg%' then 'A NEG'
            when lower(value_source_value) like '%a pos%' then 'A POS'
            when lower(value_source_value) like '%ab negative%' then 'AB NEG'
            when lower(value_source_value) like '%ab not detected%' then 'AB NOT DETECTED'
            when lower(value_source_value) like '%ab positive%' then 'AB POS'
            when lower(value_source_value) like '%abbnormal%' then 'ABNORMAL'
            when lower(value_source_value) like '%abnormal%' then 'ABNORMAL'
            when lower(value_source_value) like '%absent%' then 'ABSENT'
            when lower(value_source_value) like '%acanthocytes%' then 'ACANTHOCYTES'
            when lower(value_source_value) like '%adequate%' then 'ADEQUATE'
            when lower(value_source_value) like '%amber%' then 'AMBER'
            when lower(value_source_value) like '%amniotic fluid%' then 'AMNIOTIC FLUID'
            when lower(value_source_value) like '%anisocytosis%' then 'ANISOCYTOSIS'
            when lower(value_source_value) like '%arterial%' then 'ARTERIAL'
            when lower(value_source_value) like '%arterial line%' then 'ARTERIAL LINE'
            when lower(value_source_value) like '%b neg%' then 'B NEG'
            when lower(value_source_value) like '%b pos%' then 'B POS'
            when lower(value_source_value) like '%basophilic stippling%' then 'BASOPHILIC STIPPLING'
            when lower(value_source_value) like '%bite cells%' then 'BITE CELLS'
            when lower(value_source_value) like '%bizarre%' then 'BIZARRE CELLS'
            when lower(value_source_value) like '%black%' then 'BLACK'
            when lower(value_source_value) like '%blister cells%' then 'BLISTER CELLS'
            when lower(value_source_value) like '%blood%' then 'BLOOD'
            when lower(value_source_value) like '%bone marrow%' then 'BONE MARROW'
            when lower(value_source_value) like '%brown%' then 'BROWN'
            when lower(value_source_value) like '%burr cells%' then 'BURR CELLS'
            when lower(value_source_value) like '%cerebrospinal fluid%' then 'CEREBROSPINAL FLUID (CSF)'
            when lower(value_source_value) like '%clean catch%' then 'CLEAN CATCH'
            when lower(value_source_value) like '%clear%' then 'CLEAR'
            when lower(value_source_value) like '%cloudy%' then 'CLOUDY'
            when lower(value_source_value) like '%colorless%' then 'COLORLESS'
            when lower(value_source_value) like '%dacrocytes%' then 'DACROCYTES'
            when lower(value_source_value) like '%detected%' then 'DETECTED'
            when lower(value_source_value) like '%elliptocytes%' then 'ELLIPTOCYTES'
            when lower(value_source_value) like '%equivocal%' then 'EQUIVOCAL'
            when lower(value_source_value) like '%few%' then 'FEW'
            when lower(value_source_value) like '%green%' then 'GREEN'
            when lower(value_source_value) like '%hair%' then 'HAIR'
            when lower(value_source_value) like '%hazy%' then 'HAZY'
            when lower(value_source_value) like '%helmet%' then 'HELMET CELLS'
            when lower(value_source_value) like '%heterozygous%' then 'HETEROZYGOUS'
            when lower(value_source_value) like '%howelljolly%' then 'HOWELL-JOLLY BODIES'
            when lower(value_source_value) like '%howell jolly%' then 'HOWELL-JOLLY BODIES'
            when lower(value_source_value) like '%howell-jolly%' then 'HOWELL-JOLLY BODIES'
            when lower(value_source_value) like '%immune%' then 'IMMUNE'
            when lower(value_source_value) like '%inconclusive%' then 'INCONCLUSIVE'
            when lower(value_source_value) like '%increased%' then 'INCREASED'
            when lower(value_source_value) like '%indeterminate%' then 'INDETERMINATE'
            when lower(value_source_value) like '%influenza A virus%' then 'INFLUENZA A VIRUS'
            when lower(value_source_value) like '%influenza B virus%' then 'INFLUENZA B VIRUS'
            when lower(value_source_value) like '%invalid%' then 'INVALID'
            when lower(value_source_value) like '%large%' then 'LARGE'
            when lower(value_source_value) like '%left arm%' then 'LEFT ARM'
            when lower(value_source_value) like '%low%' then 'LOW'
            when lower(value_source_value) like '%macrocytes%' then 'MACROCYTES'
            when lower(value_source_value) like '%many%' then 'MANY'
            when lower(value_source_value) like '%microcytes%' then 'MICROCYTES'
            when lower(value_source_value) like '%moderate%' then 'MODERATE'
            when lower(value_source_value) like '%nasopharyngeal%' then 'NASOPHARYNGEAL'
            when lower(value_source_value) like '%neg%' then 'NEGATIVE'
            when lower(value_source_value) like '%tnp%' then 'NI'
            when lower(value_source_value) like '%no growth%' then 'NO GROWTH'
            when lower(value_source_value) like '%none%' then 'NONE'
            when lower(value_source_value) like '%nonreactive%' then 'NONREACTIVE'
            when lower(value_source_value) like '%normal%' then 'NORMAL'
            when lower(value_source_value) like '%none detected.%' then 'NOT DETECTED'
            when lower(value_source_value) like '%not detected%' then 'NOT DETECTED'
            when lower(value_source_value) like '%o negative%' then 'O NEG'
            when lower(value_source_value) like '%o positive%' then 'O POS'
            when lower(value_source_value) like '%occasional%' then 'OCCASIONAL'
            when lower(value_source_value) like '%@%' then 'OT'
            when lower(value_source_value) like '%see Comment%' then 'OT'
            when lower(value_source_value) like '%ovalocytes%' then 'OVALOCYTES'
            when lower(value_source_value) like '%pappenheimer bodies%' then 'PAPPENHEIMER BODIES'
            when lower(value_source_value) like '%peritoneal fluid%' then 'PERITONEAL FLUID'
            when lower(value_source_value) like '%pink%' then 'PINK'
            when lower(value_source_value) like '%plasma%' then 'PLASMA'
            when lower(value_source_value) like '%pos%' then 'POSITIVE'
            when lower(value_source_value) like '%rare%' then 'RARE'
            when lower(value_source_value) like '%reactive%' then 'REACTIVE'
            when lower(value_source_value) like '%right arm%' then 'RIGHT ARM'
            when lower(value_source_value) like '%sars coronavirus 2%' then 'SARS CORONAVIRUS 2'
            when lower(value_source_value) like '%slight%' then 'SLIGHT'
            when lower(value_source_value) like '%slightly Cloudy%' then 'SLIGHTLY CLOUDY'
            when lower(value_source_value) like '%small%' then 'SMALL'
            when lower(value_source_value) like '%specimen unsatisfactory for evaluation%' then 'SPECIMEN UNSATISFACTORY FOR EVALUATION'
            when lower(value_source_value) like '%stomatocytes%' then 'STOMATOCYTES'
            when lower(value_source_value) like '%stool%' then 'STOOL'
            when lower(value_source_value) like '%straw%' then 'STRAW'
            when lower(value_source_value) like '%suspect%' then 'SUSPECTED'
            when lower(value_source_value) like '%synovial fluid%' then 'SYNOVIAL FLUID'
            when lower(value_source_value) like '%trace%' then 'TRACE'
            when lower(value_source_value) like '%turbid%' then 'TURBID'
            when lower(value_source_value) like '%unknown%' then 'UN'
            when lower(value_source_value) like '%undetected%' then 'UNDETECTABLE'
            when lower(value_source_value) like '%inconclusive%' then 'UNDETERMINED'
            when lower(value_source_value) like '%urine%' then 'URINE'
            when lower(value_source_value) like '%white%' then 'WHITE'
            when lower(value_source_value) like '%yellow%' then 'YELLOW'
	    end,
        'OT') as obsgen_result_qual, 
    value_source_value,
    obsgen_result_text, 
    obsgen_result_num,
    map_mod.target_concept as obsgen_result_modifier,
    coalesce(
        map.target_concept,
        '{ratio}'
        ) as obsgen_result_unit,    
    coalesce(
        abn.target_concept, 
        'NI'
        ) as obsgen_abn_ind,
    obsgen_table_modified,
    obsgen_id_modified,
    obsgen_source,
    raw_obsgen_name,
    raw_obsgen_type,
    raw_obsgen_code,
    raw_obsgen_result,
    raw_obsgen_unit,
    meas.site, 
    obsgen_stop_time, 
    obsgen_stop_date, 
    meas.value_as_concept_id
from 
    meas_obs_filt meas 
left join 
    pedsnet_pcornet_valueset_map map_qual 
    on cast(meas.value_as_concept_id as varchar(256)) = map_qual.source_concept_id 
    and map_qual.source_concept_class = 'Result qualifier'
left join 
    pedsnet_pcornet_valueset_map abn 
    on cast(abn.source_concept_id as integer) = meas.value_as_concept_id 
    and abn.source_concept_class = 'abnormal_indicator'
left join 
    pedsnet_pcornet_valueset_map map 
    on map.source_concept_id = cast(meas.unit_concept_id as varchar(256)) 
    and map.source_concept_class = 'Result unit'
left join 
    pedsnet_pcornet_valueset_map map_mod 
    on map.source_concept_id = cast(meas.operator_concept_id as varchar(256)) 
    and map_mod.source_concept_class = 'Result modifier'
"""

print('transforming meas_obs (4/8)')
meas_obs = spark.sql(meas_obs_query)
meas_obs.createOrReplaceTempView("meas_obs")
meas_obs.cache()
print(meas_obs.count())

pedsnet_pcornet_valueset_map.unpersist()
meas_obs_filt.unpersist()

#--- Transform device_exposure data ---
device_obs_query = """
select 
    'd' || cast(device_exposure_id as varchar(256))as obsgenid,
    cast(dev.person_id as varchar(256)) as patid,
    cast(dev.visit_occurrence_id as varchar(256)) as encounterid,
    cast(dev.provider_id as varchar(256)) as obsgen_providerid,
    cast(dev.device_exposure_start_date as date) as obsgen_start_date,
    LPAD(cast(date_part('hour',dev.device_exposure_start_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',dev.device_exposure_start_datetime) as varchar(256)),2,'0') as obsgen_start_time,
    'PC_COVID' as obsgen_type,
    '3000' as obsgen_code,
    null as obsgen_result_qual,
    null as obsgen_abn_ind,
    case 
        when dev.device_exposure_start_date <= enc.visit_start_date and dev.device_exposure_end_date > enc.visit_start_date then 'Y' 
        else 'N' 
    end as obsgen_result_text,
    null as obsgen_result_num,
    null as obsgen_result_modifier,
    null as obsgen_result_unit,
    null as obsgen_table_modified,
    null as obsgen_id_modified,
    'DR' as obsgen_source,
    dev.device_exposure_start_date,
    dev.device_exposure_end_date,
    snomed.concept_name as raw_obsgen_name,
    snomed.vocabulary_id as raw_obsgen_type,
    snomed.concept_code as raw_obsgen_code,
    null as raw_obsgen_result,
    null as raw_obsgen_unit,
    cast(dev.device_exposure_end_date as date) as obsgen_stop_date,
    LPAD(cast(date_part('hour',dev.device_exposure_end_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',dev.device_exposure_end_datetime) as varchar(256)),2,'0') as obsgen_stop_time,
    dev.site
from 
    (
    select *
    from device_exposure
    where device_concept_id in (4044008,4097216,4138614,45761494,4224038,4139525,45768222,4222966,40493026)
    ) as dev
inner join 
    visit_occurrence enc 
    on enc.visit_occurrence_id = dev.visit_occurrence_id
left join 
    concept snomed 
    on snomed.concept_id = dev.device_concept_id 
    and snomed.vocabulary_id = 'SNOMED'
    and snomed.domain_id = 'Device'
"""

print('transforming device_obs (5/8)')
device_obs = spark.sql(device_obs_query)
device_obs.createOrReplaceTempView("device_obs")
device_obs.cache()
print(device_obs.count())

concept.unpersist()
visit_occurrence.unpersist()
device_exposure.unpersist()

# --- Transform location_FIPS (census block group) data ---
if(lh_lowerBound is not None and lf_lowerBound is not None):
    census_block_group_query = """
        with person_with_location_history as (
            select 
                entity_id as person_id,
                location_id,
                site,
                start_date,
                end_date
            from 
                location_history
            where 
                trim(lower(domain_id))='person'
        ),
        person_without_location_history as (
            select distinct
                person_id,
                location_id,
                site,
                cast(null as date) as start_date,
                cast(null as date) as end_date
            from 
                person p
            where 
                not exists 
                    (
                        select null 
                        from person_with_location_history loc
                        where loc.person_id = p.person_id
                        and loc.location_id=p.location_id
                    )
        )
        select
        'L' || cast(fips.geocode_id as varchar(256)) || cast(fips.location_id as varchar(256)) || cast(person_locations.person_id as varchar(256)) || cast(row_number() over(order by person_locations.person_id,person_locations.location_id) as varchar(256)) as obsgenid,
        cast(person_locations.person_id as varchar(256)) as patid,
        null as encounterid,
        null as obsgen_abn_ind,
        '49084-7' as obsgen_code,
        cast(fips.location_id as varchar(256)) as obsgen_id_modified,
        null as obsgen_providerid,
        null as obsgen_result_modifier,
        null as obsgen_result_num,
        null as obsgen_result_qual,
        cast(fips.geocode_state as varchar(256)) || cast(fips.geocode_county as varchar(256)) || cast(fips.geocode_tract as varchar(256)) || cast(fips.geocode_group as varchar(256)) || cast(fips.geocode_block as varchar(256)) as obsgen_result_text,
        null as obsgen_result_unit,
        null as obsgen_source,
        cast(coalesce(start_date, current_date) as date) as obsgen_start_date,
        null as obsgen_start_time,
        cast(end_date as date) as obsgen_stop_date,
        null as obsgen_stop_time,
        'LDS' as obsgen_table_modified,
        'LC' as obsgen_type,
        null as raw_obsgen_code,
        'Census Block Group' as raw_obsgen_name,
        cast(fips.location_id as varchar(256))||' - '|| cast(fips.geocode_state as varchar(256)) || cast(fips.geocode_county as varchar(256)) || cast(fips.geocode_tract as varchar(256))|| cast(fips.geocode_group as varchar(256)) || cast(fips.geocode_block as varchar(256)) as raw_obsgen_result,
        cast(geocode_year as varchar(256)) as raw_obsgen_type,
        cast(geocode_shapefile as varchar(256)) as raw_obsgen_unit,
        person_locations.site
    from
        (
        select * from person_with_location_history
        union
        select * from person_without_location_history
        ) as person_locations 
    inner join 
        location_fips fips 
        on person_locations.location_id = fips.location_id
    where
        person_locations.person_id in (
            select 
                person_id 
            from 
                person_visit_start2001
            )
    """

    print('transforming census_block_group (6/8)')
    census_block_group = spark.sql(census_block_group_query)
    census_block_group.createOrReplaceTempView("census_block_group")
    census_block_group.cache()
    print(census_block_group.count())

    location_history.unpersist()
    location_fips.unpersist()
else:
    print('transforming census_block_group (6/8)')
    print('no data in location_history or location_fips table')
    schema = adt_obs.schema
    census_block_group = spark.createDataFrame([], schema)
    census_block_group.createOrReplaceTempView("census_block_group")
    census_block_group.cache()
    print(census_block_group.count())

person_visit_start2001.unpersist()
person.unpersist()

# --- Union final DFs, drop duplicates, and format before loading to HDFS ---
obs_gen_union_query = """
    select 
        encounterid, 
        obsgen_abn_ind, 
        obsgen_code, 
        obsgen_id_modified, 
        obsgen_providerid, 
        case 
            when obsgen_result_modifier = 'NO' then 'UN'
            else obsgen_result_modifier
        end as obsgen_result_modifier, 
        obsgen_result_num, 
        obsgen_result_qual, 
        obsgen_result_text, 
        obsgen_result_unit, 
        obsgen_source, 
        obsgen_start_date, 
        obsgen_start_time, 
        obsgen_stop_date, 
        obsgen_stop_time, 
        obsgen_table_modified, 
        obsgen_type, 
        obsgenid, 
        patid, 
        raw_obsgen_code, 
        raw_obsgen_name, 
        raw_obsgen_result, 
        raw_obsgen_type, 
        raw_obsgen_unit, 
        site
    from 
        adt_obs
    union 
    select 
        encounterid, 
        obsgen_abn_ind, 
        obsgen_code, 
        obsgen_id_modified, 
        obsgen_providerid, 
        case 
            when obsgen_result_modifier = 'NO' then 'UN'
            else obsgen_result_modifier
        end as obsgen_result_modifier, 
        obsgen_result_num, 
        obsgen_result_qual, 
        obsgen_result_text, 
        obsgen_result_unit, 
        obsgen_source, 
        obsgen_start_date, 
        obsgen_start_time, 
        obsgen_stop_date, 
        obsgen_stop_time, 
        obsgen_table_modified, 
        obsgen_type, 
        obsgenid, 
        patid, 
        raw_obsgen_code, 
        raw_obsgen_name, 
        raw_obsgen_result, 
        raw_obsgen_type, 
        raw_obsgen_unit, 
        site
    from 
        meas_obs
    union 
    select 
        encounterid, 
        obsgen_abn_ind, 
        obsgen_code, 
        obsgen_id_modified, 
        obsgen_providerid, 
        case 
            when obsgen_result_modifier = 'NO' then 'UN'
            else obsgen_result_modifier
        end as obsgen_result_modifier, 
        obsgen_result_num, 
        obsgen_result_qual, 
        obsgen_result_text, 
        obsgen_result_unit, 
        obsgen_source, 
        obsgen_start_date, 
        obsgen_start_time, 
        obsgen_stop_date, 
        obsgen_stop_time, 
        obsgen_table_modified, 
        obsgen_type, 
        obsgenid, 
        patid, 
        raw_obsgen_code, 
        raw_obsgen_name, 
        raw_obsgen_result, 
        raw_obsgen_type, 
        raw_obsgen_unit, 
        site
    from 
        device_obs
    union
    select 
        encounterid, 
        obsgen_abn_ind, 
        obsgen_code, 
        obsgen_id_modified, 
        obsgen_providerid, 
        case 
            when obsgen_result_modifier = 'NO' then 'UN'
            else obsgen_result_modifier
        end as obsgen_result_modifier,
        obsgen_result_num, 
        obsgen_result_qual, 
        obsgen_result_text, 
        obsgen_result_unit, 
        obsgen_source, 
        obsgen_start_date, 
        obsgen_start_time, 
        obsgen_stop_date, 
        obsgen_stop_time, 
        obsgen_table_modified, 
        obsgen_type, 
        obsgenid, 
        patid, 
        raw_obsgen_code, 
        raw_obsgen_name, 
        raw_obsgen_result, 
        raw_obsgen_type, 
        raw_obsgen_unit, 
        site
    from 
        census_block_group
"""

print('transforming obs_gen_union (7/8)')
obs_gen_union = spark.sql(obs_gen_union_query)
obs_gen_union.createOrReplaceTempView("obs_gen_union")
obs_gen_union.cache()
print(obs_gen_union.count())

adt_obs.unpersist()
meas_obs.unpersist()
device_obs.unpersist()
census_block_group.unpersist()

obs_gen_query = """
    select 
        cast(first(encounterid) as VARCHAR(256)) as encounterid, 
        cast(first(obsgen_abn_ind) as VARCHAR(2)) as obsgen_abn_ind, 
        cast(first(obsgen_code) as VARCHAR(50)) as obsgen_code, 
        cast(first(obsgen_id_modified) as VARCHAR(256)) as obsgen_id_modified, 
        cast(first(obsgen_providerid) as VARCHAR(256)) as obsgen_providerid, 
        cast(first(obsgen_result_modifier) as VARCHAR(2)) as obsgen_result_modifier, 
        cast(first(obsgen_result_num) as NUMERIC(15, 8)) as obsgen_result_num, 
        cast(first(obsgen_result_qual) as VARCHAR(256)) as obsgen_result_qual, 
        cast(first(obsgen_result_text) as VARCHAR(256)) as obsgen_result_text, 
        cast(first(obsgen_result_unit) as VARCHAR(256)) as obsgen_result_unit, 
        cast(first(obsgen_source) as VARCHAR(2)) as obsgen_source, 
        cast(first(obsgen_start_date) as DATE) as obsgen_start_date, 
        cast(first(obsgen_start_time) as VARCHAR(5)) as obsgen_start_time, 
        cast(first(obsgen_stop_date) as DATE) as obsgen_stop_date, 
        cast(first(obsgen_stop_time) as VARCHAR(5)) as obsgen_stop_time, 
        cast(first(obsgen_table_modified) as VARCHAR(3)) as obsgen_table_modified, 
        cast(first(obsgen_type) as VARCHAR(30)) as obsgen_type, 
        cast(obsgenid as VARCHAR(256)) as obsgenid, 
        cast(first(patid) as VARCHAR(256)) as patid, 
        cast(first(raw_obsgen_code) as VARCHAR(256)) as raw_obsgen_code, 
        cast(first(raw_obsgen_name) as VARCHAR(256)) as raw_obsgen_name, 
        cast(first(raw_obsgen_result) as VARCHAR(256)) as raw_obsgen_result, 
        cast(first(raw_obsgen_type) as VARCHAR(256)) as raw_obsgen_type, 
        cast(first(raw_obsgen_unit) as VARCHAR(256)) as raw_obsgen_unit
    from 
        obs_gen_union
    group by 
        obsgenid
"""

print('transforming obs_gen (8/8)')
obs_gen = spark.sql(obs_gen_query)
obs_gen.createOrReplaceTempView("obs_gen")
obs_gen.cache()
print(obs_gen.count())

obs_gen_union.unpersist()

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
print("writing obs_gen to hdfs")
obs_gen.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_obs_gen.csv".format(site))