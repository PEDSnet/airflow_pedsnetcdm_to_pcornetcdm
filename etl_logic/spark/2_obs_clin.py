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
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, when, isnull, array_contains, to_date, col, to_timestamp, trim, date_format, concat, lit, coalesce, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType, DecimalType, TimestampType, LongType, DoubleType
from typing import Dict

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
print("Extracting person_visit_start2001 from postgres into spark (1/6)")
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
print("Extracting measurement from postgres into spark (2/6)")
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
print("Extracting concept from postgres into spark (3/6)")
query = "SELECT MIN(concept_id), MAX(concept_id) FROM vocabulary.concept"
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
vocab = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'vocabulary.concept')\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","concept_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
vocab.createOrReplaceTempView("vocab")
vocab.cache()
print(vocab.count())

#observation
print("Extracting observation from postgres into spark (4/6)")
query = "SELECT MIN(observation_id), MAX(observation_id) FROM {}_pedsnet.observation".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
observation = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'{}_pedsnet.observation'.format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","observation_id")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
observation.createOrReplaceTempView("observation")
observation.cache()
print(observation.count())

#person
print("Extracting person from postgres into spark (5/6)")
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
print("Extracting pedsnet_pcornet_valueset_map from postgres into spark (6/6)")
pedsnet_pcornet_valueset_map = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'pcornet_maps.pedsnet_pcornet_valueset_map')\
    .option("user",user)\
    .option("password",password)\
    .load()
pedsnet_pcornet_valueset_map.createOrReplaceTempView("pedsnet_pcornet_valueset_map")
pedsnet_pcornet_valueset_map.cache()
print(pedsnet_pcornet_valueset_map.count())

#------------------------------ 2. Transform extracted data into finalized Spark Dataframe ------------------------------
# --- Transform observation data ---
filter_obs_query = """
select * 
from 
    observation obs
where 
    EXTRACT(YEAR FROM obs.observation_date) >= 2001
    and obs.person_id in (select person_id from person_visit_start2001)
    and (
        obs.visit_occurrence_id in (select visit_id from person_visit_start2001)
	    or obs.visit_occurrence_id is null
        )
"""

print('transforming filter_obs (1/7)')
filter_obs = spark.sql(filter_obs_query)
filter_obs.createOrReplaceTempView("filter_obs")
filter_obs.cache()
print(filter_obs.count())

observation.unpersist()

# --- Transform measurement data ---
meas_obsclin_loinc_query = """
select 
    ('m'|| cast(meas.measurement_id as varchar(256))) as obsclinid,
    cast(meas.person_id as varchar(256)) as patid,
    cast(meas.visit_occurrence_id as varchar(256)) as encounterid,
    cast(meas.provider_id as varchar(256)) as obsclin_providerid,
    cast(meas.measurement_date as date) as obsclin_start_date,
    coalesce(abn.target_concept, 'NI') as obsclin_abn_ind,
    LPAD(cast(date_part('hour',measurement_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',measurement_datetime) as varchar(256)),2,'0') as obsclin_start_time,
    case 
        when measurement_concept_id = 4353936 then 'SM' 
        else 'LC' 
    end as obsclin_type,
    case 
        when meas.measurement_concept_id = 4353936 then '250774007' 
        else loinc.concept_code 
    end as obsclin_code,
    meas.value_source_value, 
    meas.value_as_concept_id,
    case 
        when meas.measurement_concept_id = 4353936 then '250774007' 
        else null 
    end as obsclin_result_snomed, 
    cast(meas.value_as_number as varchar(256)) as obsclin_result_text,
    meas.operator_concept_id,
    meas.unit_concept_id,
    meas.unit_source_value,
    'HC' as obsclin_source,
    null as raw_obsclin_name,
    null as raw_obsclin_type,
    null as raw_obsclin_code,
    null as raw_obsclin_modifier,
    cast(meas.value_as_number as varchar(256)) as raw_obsclin_result,
    meas.unit_concept_name as raw_obsclin_unit,
    meas.site
from 
    measurement meas 
left join 
    vocab loinc 
    on loinc.concept_id = meas.measurement_concept_id 
    and loinc.vocabulary_id = 'LOINC'
left join 
    pedsnet_pcornet_valueset_map abn 
    on cast(abn.source_concept_id as integer) = meas.value_as_concept_id 
    and abn.source_concept_class = 'abnormal_indicator'
where 
    meas.measurement_concept_id in (3020891,3024171,40762499,3027018,4353936,21490852,21492241,3020158,3037879,3001668,3024653,3005025,3023550,42868460,42868461,42868462,3023329)
"""

print('transforming meas_obsclin_loinc (2/7)')
meas_obsclin_loinc = spark.sql(meas_obsclin_loinc_query)
meas_obsclin_loinc.createOrReplaceTempView("meas_obsclin_loinc")
meas_obsclin_loinc.repartition(1000)
meas_obsclin_loinc.cache()
print(meas_obsclin_loinc.count())

measurement.unpersist()

meas_obsclin_qual_query = """
select 
    obsclinid,
    patid,
    encounterid,
    obsclin_providerid,
    obsclin_start_date,
    obsclin_start_time,
    obsclin_type,
    obsclin_code,
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
            when lower(value_source_value) like '%Clear%' then 'CLEAR'
            when lower(value_source_value) like '%CLEAR%' then 'CLEAR'
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
            when lower(value_source_value) like '%Inconclusive%' then 'INCONCLUSIVE'
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
            when lower(value_source_value) like '%Slightly Cloudy%' then 'SLIGHTLY CLOUDY'
            when lower(value_source_value) like '%small%' then 'SMALL'
            when lower(value_source_value) like '%Specimen unsatisfactory for evaluation%' then 'SPECIMEN UNSATISFACTORY FOR EVALUATION'
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
        'OT') as obsclin_result_qual,
    obsclin_abn_ind, 
    meas.value_source_value,
    obsclin_result_snomed, 
    obsclin_result_text,
    meas.operator_concept_id,
    meas.unit_concept_id, 
    meas.unit_source_value,
    obsclin_source,
    raw_obsclin_name,
    raw_obsclin_type,
    raw_obsclin_code,
    raw_obsclin_modifier,
    raw_obsclin_result,
    raw_obsclin_unit,
    meas.site
from 
    meas_obsclin_loinc meas 
left join 
   pedsnet_pcornet_valueset_map map_qual 
   on cast(meas.value_as_concept_id as varchar(256)) = map_qual.source_concept_id 
   and map_qual.source_concept_class = 'Result qualifier'
"""

print('transforming meas_obsclin_qual (3/7)')
meas_obsclin_qual = spark.sql(meas_obsclin_qual_query)
meas_obsclin_qual.createOrReplaceTempView("meas_obsclin_qual")
meas_obsclin_qual.repartition(1000)
meas_obsclin_qual.cache()
print(meas_obsclin_qual.count())

meas_obsclin_loinc.unpersist()

meas_obsclin_query = """
select 
    obsclinid,
    patid,
    encounterid,
    obsclin_providerid,
    obsclin_start_date,
    obsclin_start_time,
    obsclin_type,
    obsclin_code,
    obsclin_result_qual,
    obsclin_result_snomed, 
    obsclin_abn_ind, 
    obsclin_result_text,
    cast(null as date) as obsclin_stop_date,
    null as obsclin_stop_time,
    coalesce(map_mod.target_concept,'OT') as obsclin_result_modifier,
    map.target_concept as obsclin_result_unit,
    obsclin_source,
    raw_obsclin_name,
    raw_obsclin_type,
    raw_obsclin_code,
    raw_obsclin_modifier,
    raw_obsclin_result,
    raw_obsclin_unit,
    meas.site
from 
    meas_obsclin_qual meas 
left join 
    pedsnet_pcornet_valueset_map map 
    on map.source_concept_id = cast(meas.unit_concept_id as varchar(256))
    and map.source_concept_class = 'Result unit'
left join 
    pedsnet_pcornet_valueset_map map_mod 
    on map.source_concept_id = cast(meas.operator_concept_id as varchar(256))
    and map_mod.source_concept_class = 'Result modifier'
where
    cast(patid as integer) in (select person_id from person_visit_start2001)
    and 
    (
    encounterid is null
    or cast(encounterid as integer) in (select visit_id from person_visit_start2001)
    )
"""

print('transforming meas_obsclin (4/7)')
meas_obsclin = spark.sql(meas_obsclin_query)
meas_obsclin.createOrReplaceTempView("meas_obsclin")
meas_obsclin.repartition(1000)
meas_obsclin.cache()
print(meas_obsclin.count())

meas_obsclin_qual.unpersist()

# --- Transform vaping data ---
obs_vaping_query = """
select 
    cast(('o'||obs.observation_id) as varchar(256)) as obsclinid,
    cast(obs.person_id as varchar(256)) as patid,
    cast(obs.visit_occurrence_id as varchar(256)) as encounterid,
    cast(obs.provider_id as varchar(256)) as obsclin_providerid,
    coalesce(abn.target_concept, 'NI') as obsclin_abn_ind,
    cast(obs.observation_date as date) as obsclin_start_date,
    LPAD(cast(date_part('hour',obs.observation_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',obs.observation_datetime) as varchar(256)),2,'0') as obsclin_start_time,
    'SM' as obsclin_type,
    snomed.concept_code as obsclin_code,
    coalesce(qual.target_concept,'NI') as obsclin_result_qual,
    snomed.concept_code as obsclin_result_snomed,
    cast(obs.value_as_string as varchar(256)) as obsclin_result_text,
    null as obsclin_result_modifier,
    null as obsclin_result_unit,
    'HC' as obsclin_source,
    null as raw_obsclin_name,
    null as raw_obsclin_type,
    null as raw_obsclin_code,
    null as raw_obsclin_modifier,
    null as raw_obsclin_result,
    null as raw_obsclin_unit,
    cast(null as date) as obsclin_stop_date,
    null as obsclin_stop_time,
    obs.site
from 
    filter_obs obs
left join 
    pedsnet_pcornet_valueset_map qual 
    on qual.source_concept_id = cast(obs.qualifier_concept_id as varchar(256))
    and qual.source_concept_class = 'Result qualifier'
left join 
    vocab snomed 
    on snomed.concept_id = cast(obs.value_as_string as integer)
    and snomed.vocabulary_id = 'SNOMED'
left join 
    pedsnet_pcornet_valueset_map abn 
    on cast(abn.source_concept_id as integer) = obs.value_as_concept_id 
    and abn.source_concept_class = 'abnormal_indicator'
where 
    observation_concept_id = 4219336 
    and obs.value_as_concept_id in (42536422,42536421,36716478)
    and obs.person_id in (select person_id from person_visit_start2001)
    and 
    (
    obs.visit_occurrence_id is not null
    or obs.visit_occurrence_id in (select visit_id from person_visit_start2001)
    )
"""
print('transforming obs_vaping (5/7)')
obs_vaping = spark.sql(obs_vaping_query)
obs_vaping.createOrReplaceTempView("obs_vaping")
obs_vaping.repartition(1000)
obs_vaping.cache()
print(obs_vaping.count())

vocab.unpersist()
pedsnet_pcornet_valueset_map.unpersist()
filter_obs.unpersist()

# --- Transform gestational_age data ---
gestational_age_query = """
select 
    cast(null as integer) as encounterid, 
    '18185-9' as obsclin_code, 
    'NI' as obsclin_abn_ind,
    p.birth_date as obsclin_start_date, 
    p.provider_id as obsclin_providerid, 
    'NI' as obsclin_result_modifier, 
    null as obsclin_result_snomed, 
    'NI' as obsclin_result_qual, 
    cast(p.pn_gestational_age as varchar(256)) as obsclin_result_text,
    'wk' as obsclin_result_unit, 
    'HC' as obsclin_source,
    LPAD(cast(date_part('hour',p.birth_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',p.birth_datetime) as varchar(256)),2,'0') as obsclin_start_time,
    'LC' as obsclin_type, 
    cast(('p'||p.person_id) as varchar(256)) as obsclinid, 
    cast(p.person_id as varchar(256)) as patid, 
    '18185-9' as raw_obsclin_code, 
    null as raw_obsclin_modifier, 
    'Gestational age' as raw_obsclin_name, 
    cast(pn_gestational_age as varchar(256)) as raw_obsclin_result, 
    'LOINC' as raw_obsclin_type, 
    'week' as raw_obsclin_unit, 
    cast(null as date) as obsclin_stop_date, 
    null as obsclin_stop_time,
    site as site
from 
    person p
where 
    pn_gestational_age is not null
    and person_id in (select person_id from person_visit_start2001)
"""

print('transforming gestational_age (6/7)')
gestational_age = spark.sql(gestational_age_query)
gestational_age.createOrReplaceTempView("gestational_age")
gestational_age.repartition(1000)
gestational_age.cache()
print(gestational_age.count())

person_visit_start2001.unpersist()
person.unpersist()

# --- Union final DFs and format before loading to HDFS ---
meas_obsclin_final_query = """
select 
    cast(first(encounterid) as varchar(256)) as encounterid,
    cast(first(left(obsclin_abn_ind,2)) as varchar(2)) as obsclin_abn_ind, 
    cast(first(left(obsclin_code,18)) as varchar(18)) as obsclin_code,
    cast(first(obsclin_providerid) as varchar(256)) as obsclin_providerid,
    cast(first(left(case when obsclin_result_modifier = 'NO' then 'UN' else obsclin_result_modifier end,2)) as varchar(2)) as obsclin_result_modifier,
    cast(null as varchar(15)) as obsclin_result_num,
    cast(first(obsclin_result_qual) as varchar(256)) as obsclin_result_qual,
    cast(first(left(obsclin_result_snomed,18)) as varchar(18)) as obsclin_result_snomed, 
    cast(first(obsclin_result_text) as varchar(256)) as obsclin_result_text, 
	cast(first(obsclin_result_unit) as varchar(256)) as obsclin_result_unit,
    cast(first(left(obsclin_source,2)) as varchar(2)) as obsclin_source,
    cast(first(obsclin_start_date) as date) as obsclin_start_date,
    cast(first(obsclin_start_time) as varchar(256)) as obsclin_start_time, 
    cast(first(obsclin_stop_date) as date) as obsclin_stop_date, 
    cast(first(obsclin_stop_time) as varchar(256)) as obsclin_stop_time, 
    cast(first(left(obsclin_type,2)) as varchar(2)) as obsclin_type, 
    cast(obsclinid as varchar(256)) as obsclinid, 
    cast(first(patid) as varchar(256)) as patid, 
    cast(first(raw_obsclin_code) as varchar(256)) as raw_obsclin_code, 
    cast(first(raw_obsclin_modifier) as varchar(256)) as raw_obsclin_modifier, 
    cast(first(raw_obsclin_name) as varchar(256)) as raw_obsclin_name, 
    cast(first(raw_obsclin_result) as varchar(256)) as raw_obsclin_result, 
    cast(first(raw_obsclin_type) as varchar(256)) as raw_obsclin_type,
    cast(first(raw_obsclin_unit) as varchar(256)) as raw_obsclin_unit,
    cast(first(site) as varchar(256)) as site
from 
    meas_obsclin
group by
    obsclinid
UNION
select 
    cast(first(encounterid) as varchar(256)) as encounterid,
    cast(first(left(obsclin_abn_ind,2)) as varchar(2)) as obsclin_abn_ind, 
    cast(first(left(obsclin_code,18)) as varchar(18)) as obsclin_code,
    cast(first(obsclin_providerid) as varchar(256)) as obsclin_providerid,
    cast(first(left(case when obsclin_result_modifier = 'NO' then 'UN' else obsclin_result_modifier end,2)) as varchar(2)) as obsclin_result_modifier,
    cast(null as varchar(15)) as obsclin_result_num,
    cast(first(obsclin_result_qual) as varchar(256)) as obsclin_result_qual,
    cast(first(left(obsclin_result_snomed,18)) as varchar(18)) as obsclin_result_snomed, 
    cast(first(obsclin_result_text) as varchar(256)) as obsclin_result_text, 
	cast(first(obsclin_result_unit) as varchar(256)) as obsclin_result_unit,
    cast(first(left(obsclin_source,2)) as varchar(2)) as obsclin_source,
    cast(first(obsclin_start_date) as date) as obsclin_start_date,
    cast(first(obsclin_start_time) as varchar(256)) as obsclin_start_time, 
    cast(first(obsclin_stop_date) as date) as obsclin_stop_date, 
    cast(first(obsclin_stop_time) as varchar(256)) as obsclin_stop_time, 
    cast(first(left(obsclin_type,2)) as varchar(2)) as obsclin_type, 
    cast(obsclinid as varchar(256)) as obsclinid, 
    cast(first(patid) as varchar(256)) as patid, 
    cast(first(raw_obsclin_code) as varchar(256)) as raw_obsclin_code, 
    cast(first(raw_obsclin_modifier) as varchar(256)) as raw_obsclin_modifier, 
    cast(first(raw_obsclin_name) as varchar(256)) as raw_obsclin_name, 
    cast(first(raw_obsclin_result) as varchar(256)) as raw_obsclin_result, 
    cast(first(raw_obsclin_type) as varchar(256)) as raw_obsclin_type,
    cast(first(raw_obsclin_unit) as varchar(256)) as raw_obsclin_unit,
    cast(first(site) as varchar(256)) as site
from 
    obs_vaping
group by
    obsclinid
UNION
select 
    cast(first(encounterid) as varchar(256)) as encounterid,
    cast(first(left(obsclin_abn_ind,2)) as varchar(2)) as obsclin_abn_ind, 
    cast(first(left(obsclin_code,18)) as varchar(18)) as obsclin_code,
    cast(first(obsclin_providerid) as varchar(256)) as obsclin_providerid,
    cast(first(left(case when obsclin_result_modifier = 'NO' then 'UN' else obsclin_result_modifier end,2)) as varchar(2)) as obsclin_result_modifier,
    cast(null as varchar(15)) as obsclin_result_num,
    cast(first(obsclin_result_qual) as varchar(256)) as obsclin_result_qual,
    cast(first(left(obsclin_result_snomed,18)) as varchar(18)) as obsclin_result_snomed, 
    cast(first(obsclin_result_text) as varchar(256)) as obsclin_result_text, 
	cast(first(obsclin_result_unit) as varchar(256)) as obsclin_result_unit,
    cast(first(left(obsclin_source,2)) as varchar(2)) as obsclin_source,
    cast(first(obsclin_start_date) as date) as obsclin_start_date,
    cast(first(obsclin_start_time) as varchar(256)) as obsclin_start_time, 
    cast(first(obsclin_stop_date) as date) as obsclin_stop_date, 
    cast(first(obsclin_stop_time) as varchar(256)) as obsclin_stop_time, 
    cast(first(left(obsclin_type,2)) as varchar(2)) as obsclin_type, 
    cast(obsclinid as varchar(256)) as obsclinid, 
    cast(first(patid) as varchar(256)) as patid, 
    cast(first(raw_obsclin_code) as varchar(256)) as raw_obsclin_code, 
    cast(first(raw_obsclin_modifier) as varchar(256)) as raw_obsclin_modifier, 
    cast(first(raw_obsclin_name) as varchar(256)) as raw_obsclin_name, 
    cast(first(raw_obsclin_result) as varchar(256)) as raw_obsclin_result, 
    cast(first(raw_obsclin_type) as varchar(256)) as raw_obsclin_type,
    cast(first(raw_obsclin_unit) as varchar(256)) as raw_obsclin_unit,
    cast(first(site) as varchar(256)) as site
from 
    gestational_age
group by
    obsclinid
"""

print('transforming meas_obsclin_final (7/7)')
meas_obsclin_final = spark.sql(meas_obsclin_final_query)
meas_obsclin_final.createOrReplaceTempView("meas_obsclin_final")
meas_obsclin_final.repartition(1000)
meas_obsclin_final.cache()
print(meas_obsclin_final.count())

meas_obsclin.unpersist()
obs_vaping.unpersist()
gestational_age.unpersist()

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
print("writing to hdfs")
meas_obsclin_final.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_obs_clin.csv".format(site))