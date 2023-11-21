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
#drug_exposure
print("Extracting drug_exposure from postgres into spark (1/5)")
query = "SELECT MIN(drug_exposure_id), MAX(drug_exposure_id) FROM {}_pedsnet.drug_exposure".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
drug_exposure = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable","{}_pedsnet.drug_exposure".format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","drug_exposure_id")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
drug_exposure.createOrReplaceTempView("drug_exposure")
drug_exposure.cache()
print(drug_exposure.count())

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

#vocabulary.concept_relationship
print("Extracting concept_relationship from postgres into spark (3/5)")
query = "SELECT MIN(concept_id_1), MAX(concept_id_1) FROM vocabulary.concept_relationship"
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
concept_relationship = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable",'vocabulary.concept_relationship')\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","concept_id_1")\
    .option("numPartitions",2000)\
    .option("lowerBound",lowerBound)\
    .option("upperBound", upperBound)\
    .load()
concept_relationship.createOrReplaceTempView("concept_relationship")
concept_relationship.cache()
print(concept_relationship.count())

# person_visit_start2001
print("Extracting person_visit_start2001 from postgres into spark (4/5)")
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

#pcornet_maps.pedsnet_pcornet_valueset_map
print("Extracting pedsnet_pcornet_valueset_map from postgres into spark (5/5)")
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
# --- Transform drug_exposure data into prescribing DDL ---
rx_dose_form_data_query = """
    with de as (
        select 
            distinct drug_concept_id
        from 
            drug_exposure
        where 
            drug_type_concept_id IN (38000177, 38000180,581373) 
            and drug_concept_id > 0 
    ),
    cr as (
        select *
        from concept_relationship
        where relationship_id = 'RxNorm has dose form'
    )
    select  
        de.drug_concept_id, 
        c.concept_id as rx_dose_form_concept_id, 
        c.concept_name as rx_dose_form_concept_name
    from 
        de
    inner join 
        cr
        on de.drug_concept_id = cr.concept_id_1
    inner join 
        concept c  
        on cr.concept_id_2 = c.concept_id
"""

print('transforming rx_dose_form_data (1/4)')
rx_dose_form_data = spark.sql(rx_dose_form_data_query)
rx_dose_form_data.createOrReplaceTempView("rx_dose_form_data")
rx_dose_form_data.cache()
print(rx_dose_form_data.count())

concept_relationship.unpersist()

filter_prescribed_query = """
    select 
        *
    from
        drug_exposure
    where
        drug_type_concept_id in (38000177,581373)
        and person_id IN (select person_id from person_visit_start2001)
        and (visit_occurrence_id is null or visit_occurrence_id in (select visit_id from person_visit_start2001))
        and EXTRACT(YEAR FROM drug_exposure_start_date) >= 2001
        and lower(drug_source_value) not like '%undiluted diluent%'
        and lower(drug_source_value) not like '%kcal/oz%'
        and lower(drug_source_value) not like '%breastmilk%'
        and lower(drug_source_value) not like '%kit%'
        and lower(drug_source_value) not like '%item%'
        and lower(drug_source_value) not like '%formula%'
        and lower(drug_source_value) not like '%tpn%'
        and lower(drug_source_value) not like '%custom%'
        and lower(drug_source_value) not like '%parenteral nutrition%'
        and lower(drug_source_value) not like '%breast milk%'
        and lower(drug_source_value) not like '%fat emul%'
        and lower(drug_source_value) not like '%human milk%'
        and lower(drug_source_value) not like '%tpn%'
        and lower(drug_source_value) not like '%similac%'
        and lower(drug_source_value) not like '%formula%'
        and lower(drug_source_value) not like '%fish oil%'
        and lower(drug_source_value) not like '%omega 3%'
        and lower(drug_source_value) not like '%omega3%'
        and lower(drug_source_value) not like '%omega-3%'
        and lower(drug_source_value) not like '%empty bag%'
        and lower(drug_source_value) not like '%unable to find%'
        and lower(drug_source_value) not like '%ims template%'
        and lower(drug_source_value) not like '%extemporaneous template%'
        and lower(drug_source_value) not like '%iv infusion builder%'
        and lower(drug_source_value) not like '%patient supplied medication%'
        and lower(drug_source_value) not like '%misc med%'
        and lower(drug_source_value) not like '%drug formulationzor%'
        and lower(drug_source_value) not like '%water for injection%'
"""

print('transforming filter_prescribed (2/4)')
filter_prescribed = spark.sql(filter_prescribed_query)
filter_prescribed.createOrReplaceTempView("filter_prescribed")
filter_prescribed.cache()
print(filter_prescribed.count())

drug_exposure.unpersist()
person_visit_start2001.unpersist()

prescribing_query = """
    select 
        cast(drug_exposure_id as varchar(256)) as prescribingid,
        cast(de.person_id as varchar(256)) as patid,
        cast(de.visit_occurrence_id as varchar(256)) as encounterid,
        cast(de.provider_id as varchar(256)) as rx_providerid,
        cast(de.drug_exposure_start_date as date) as rx_order_date,
        LPAD(cast(date_part('hour',drug_exposure_start_datetime) as varchar(256)),2,'0')||':'||LPAD(cast(date_part('minute',drug_exposure_start_datetime) as varchar(256)),2,'0') as rx_order_time,
        cast(drug_exposure_start_date as date)  as rx_start_date,
        cast(drug_exposure_end_date as date) as rx_end_date,
        cast(effective_drug_dose as Decimal(15,8))  as rx_dose_ordered,
        coalesce(m1.target_concept,'OT') as rx_dose_ordered_unit,
        coalesce(m2.target_concept,'OT') as rx_dose_form,
        cast(round(quantity,2) as Decimal(15,8)) as rx_quantity,
        cast(refills as Decimal(15,8)) as rx_refills,
        cast(days_supply as Decimal(15,8)) as rx_days_supply,
        cast(coalesce (m3.target_concept,'OT') as varchar(2)) as rx_frequency,
        cast(case 
            when trim(lower(frequency)) like '%as needed%' or trim(lower(frequency)) like '%prn%' then 'Y' 
            else 'N'
        end as varchar(1))	as rx_prn_flag, 
        cast(coalesce(m4.target_concept,'OT') as varchar(256)) as rx_route, 
        cast(case 
            when drug_type_concept_id = '38000177' then '01' 
            when drug_type_concept_id = '581373' then '02' 
            else 'NI' 
        end as varchar(2)) as rx_basis,
        CAST(nullif(c1.concept_code, '') as varchar(8)) as rxnorm_cui,
        'OD' as rx_source, 
        cast(coalesce(m5.target_concept,'OT') as varchar(2)) as rx_dispense_as_written,
        cast(case 
            when c1.concept_name is null then replace(split_part(drug_source_value,'|',1),'"','')
            else c1.concept_name
        end as varchar(256)) as raw_rx_med_name,
        cast(de.frequency as varchar(256)) as raw_rx_frequency,
        cast(c2.concept_code as varchar(256)) as raw_rxnorm_cui,
        cast(eff_drug_dose_source_value as varchar(256)) as raw_rx_dose_ordered,
        cast(dose_unit_source_value as varchar(256)) as raw_rx_dose_ordered_unit,
        cast(route_source_value as varchar(256)) as raw_rx_route,
        cast(de.refills as varchar(256)) as raw_rx_refills,
        cast('{}' as varchar(256)) as site
    from 
        filter_prescribed de
    left join 
        concept c1 
        on de.drug_concept_id = c1.concept_id 
        AND vocabulary_id = 'RxNorm'
    left join 
        concept c2 
        on de.drug_source_concept_id = c2.concept_id
    left join 
        rx_dose_form_data rdf 
        on de.drug_concept_id =  rdf.drug_concept_id
    left join 
        pedsnet_pcornet_valueset_map m1 
        on cast(dose_unit_concept_id as varchar(256)) = m1.source_concept_id 
        and m1.source_concept_class='Dose unit'
    left join 
        pedsnet_pcornet_valueset_map m2 
        on cast(rdf.rx_dose_form_concept_id as varchar(256)) = m2.source_concept_id 
        and m2.source_concept_class='Rx Dose Form'
    left join 
        pedsnet_pcornet_valueset_map m3 
        on cast(trim(lower(de.frequency)) as varchar(256)) = m3.source_concept_id 
        and m3.source_concept_class='Rx Frequency'
    left join 
        pedsnet_pcornet_valueset_map m4 
        on cast(de.route_concept_id as varchar(256)) = m4.source_concept_id 
        and m4.source_concept_class='Route'
    left join 
        pedsnet_pcornet_valueset_map m5 
        on cast(de.dispense_as_written_concept_id as varchar(256)) = m5.source_concept_id  
        and m5.source_concept_class='dispense written'
    """.format(site)

print('transforming prescribing (3/4)')
prescribing = spark.sql(prescribing_query)
prescribing.createOrReplaceTempView("prescribing")
prescribing.cache()
print(prescribing.count())

concept.unpersist()
pedsnet_pcornet_valueset_map.unpersist()
rx_dose_form_data.unpersist()
filter_prescribed.unpersist()

# --- Format for loading to HDFS ---
prescribing_format_query = """
    select
        first(encounterid) as encounterid,
        first(patid) as patid,
        prescribingid as prescribingid,
        first(raw_rx_dose_ordered) as raw_rx_dose_ordered,
        first(raw_rx_dose_ordered_unit) as raw_rx_dose_ordered_unit,
        first(raw_rx_frequency) as raw_rx_frequency,
        first(raw_rx_med_name) as raw_rx_med_name,
        first(cast(NULL as varchar(256))) as raw_rx_ndc,
        first(cast(NULL as varchar(256))) as raw_rx_quantity,
        first(raw_rx_refills) as raw_rx_refills,
        first(raw_rx_route) as raw_rx_route,
        first(raw_rxnorm_cui) as raw_rxnorm_cui,
        first(rx_basis) as rx_basis,
        first(rx_days_supply) as rx_days_supply,
        first(rx_dispense_as_written) as rx_dispense_as_written,
        first(rx_dose_form) as rx_dose_form,
        first(rx_dose_ordered) as rx_dose_ordered,
        first(rx_dose_ordered_unit) as rx_dose_ordered_unit,
        first(rx_end_date) as rx_end_date,
        first(rx_frequency) as rx_frequency,
        first(rx_order_date) as rx_order_date,
        first(rx_order_time) as rx_order_time,
        first(rx_prn_flag) as rx_prn_flag,
        first(rx_providerid) as rx_providerid,
        first(rx_quantity) as rx_quantity,
        first(cast(NULL as varchar(256))) as rx_quantity_unit,
        first(rx_refills) as rx_refills,
        first(rx_route) as rx_route,
        first(rx_source) as rx_source,
        first(rx_start_date) as rx_start_date,
        first(rxnorm_cui) as rxnorm_cui,
        first(site) as site
    from
        prescribing
    group by
        prescribingid
"""

print('transforming prescribing_format (4/4)')
prescribing_format = spark.sql(prescribing_format_query)
prescribing_format.createOrReplaceTempView("prescribing_format")
prescribing_format.cache()
print(prescribing_format.count())

prescribing.unpersist()

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
print("writing to hdfs")
prescribing_format.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_prescribing.csv".format(site))