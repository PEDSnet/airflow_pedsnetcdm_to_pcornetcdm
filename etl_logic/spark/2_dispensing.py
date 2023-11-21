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
#drug_exposure
print("Extracting drug_exposure from postgres into spark (1/5)")
query = "SELECT MIN(drug_exposure_id), MAX(drug_exposure_id) FROM {}_pedsnet.drug_exposure".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
print(lowerBound)
print(upperBound)
if(lowerBound is not None):
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
    print("Extracting vocabulary.concept from postgres into spark (2/5)")
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
    print("Extracting vocabulary.concept_relationship from postgres into spark (3/5)")
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
    # --- Get vocabulary crosswalks ---

    rxnorm_ndc_crosswalk_query = """
    select 
		min(ndc_codes.concept_code) as min_ndc_code, 
		rxnorm_codes.concept_id as rxnorm_concept_id
    from 
		concept ndc_codes
	inner join 
		concept_relationship cr 
		on concept_id_1 = ndc_codes.concept_id and relationship_id='Maps to'
	inner join 
		concept rxnorm_codes 
		on concept_id_2 = rxnorm_codes.concept_id
    where 
		ndc_codes.vocabulary_id = 'NDC' 
		and rxnorm_codes.vocabulary_id = 'RxNorm' 
		and ndc_codes.concept_class_id = '11-digit NDC'
    group by 
		rxnorm_codes.concept_id
    """

    print('transforming rxnorm_ndc_crosswalk (1/5)')
    rxnorm_ndc_crosswalk = spark.sql(rxnorm_ndc_crosswalk_query)
    rxnorm_ndc_crosswalk.createOrReplaceTempView("rxnorm_ndc_crosswalk")
    rxnorm_ndc_crosswalk.cache()
    print(rxnorm_ndc_crosswalk.count())

    concept_relationship.unpersist()

    ndc_concepts_query = """
    select 
        concept_code, 
        concept_id
    from 
        concept
    where 
        vocabulary_id = 'NDC'
    """

    print('transforming ndc_concepts (2/5)')
    ndc_concepts = spark.sql(ndc_concepts_query)
    ndc_concepts.createOrReplaceTempView("ndc_concepts")
    ndc_concepts.cache()
    print(ndc_concepts.count())

    concept.unpersist()

    # --- ETL drug_exposure date into dispensing DDL ---
    dispensed_filter_query = """
    select 
        *
    from 
        drug_exposure
    where 
        drug_type_concept_id = 38000175 
        and person_id in (select person_id from person_visit_start2001)
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

    print('Filtering drug_exposure (3/5)')
    dispensed_filter = spark.sql(dispensed_filter_query)
    dispensed_filter.createOrReplaceTempView("dispensed_filter")
    dispensed_filter.cache()
    print(dispensed_filter.count())
    
    drug_exposure.unpersist()
    person_visit_start2001.unpersist()

    dispensing_query = """
    select
        cast(de.drug_exposure_id as varchar(256)) as dispensingid,
        cast(de.person_id as varchar(256)) as patid,
        cast(null as varchar(256)) as prescribingid,
        cast(de.drug_exposure_start_date as date) as dispense_date,
        cast(COALESCE(
            ndc.concept_code, 
            rxnorm_ndc_crosswalk.min_ndc_code, 
            left(split(drug_source_value,'|')[0],11)
        ) as varchar(11)) as ndc,
        cast(case 
            when de.days_supply = 0 then null
            else de.days_supply
        end as varchar(256)) as dispense_sup,
        cast(de.quantity as varchar(256)) as dispense_amt,
        cast(cast(de.effective_drug_dose as Decimal(15,3)) as Double) as dispense_dose_disp, 
        cast(coalesce(m1.target_concept,'OT') as varchar(256)) as dispense_dose_disp_unit,
        cast(coalesce(m2.target_concept,'OT') as varchar(256)) as dispense_route,
        cast('PM' as varchar(256)) as dispense_source,
        cast(drug_source_value as varchar(256)) as raw_ndc,
        cast(eff_drug_dose_source_value as varchar(256)) as raw_dispense_dose_disp, 
        cast(dose_unit_source_value as varchar(256)) as raw_dispense_dose_disp_unit,
        cast(route_source_value as varchar(256)) as raw_dispense_route,
        cast('{}' as varchar(256)) as site
    from
        dispensed_filter de
    left join 
        rxnorm_ndc_crosswalk 
        on drug_concept_id = rxnorm_concept_id
    left join 
        ndc_concepts ndc 
        on concept_id = drug_source_concept_id
    left join 
        pedsnet_pcornet_valueset_map m1 
        on cast(dose_unit_concept_id as varchar(256)) = m1.source_concept_id 
        and m1.source_concept_class='Dose unit'
    left join 
        pedsnet_pcornet_valueset_map m2 
        on cast(route_concept_id as varchar(256)) = m2.source_concept_id 
        and m2.source_concept_class='Route'
    where
        rxnorm_ndc_crosswalk.min_ndc_code is not null
        or ndc.concept_id is not null
        or split_part(drug_source_value,'|',1) in (select concept_code from ndc_concepts)
    """.format(site)

    print('transforming dispensing (4/5)')
    dispensing = spark.sql(dispensing_query)
    dispensing.createOrReplaceTempView("dispensing")
    dispensing.cache()
    print(dispensing.count())

    pedsnet_pcornet_valueset_map.unpersist()
    rxnorm_ndc_crosswalk.unpersist()
    ndc_concepts.unpersist()
    dispensed_filter.unpersist()

    # --- Take Distinct and Format for loading ---
    print("Taking Distinct (5/5)")
    dispensing = dispensing.distinct()
    dispensing = dispensing.select( 
        'dispense_amt',
        'dispense_date',
        'dispense_dose_disp',
        'dispense_dose_disp_unit',
        'dispense_route',
        'dispense_source',
        'dispense_sup',
        'dispensingid',
        'ndc',
        'patid',
        'prescribingid',
        'raw_dispense_dose_disp',
        'raw_dispense_dose_disp_unit',
        'raw_dispense_route',
        'raw_ndc',
        'site'
    )
    dispensing.createOrReplaceTempView("dispensing")
    dispensing.repartition(1000)
    dispensing.cache()
    print(dispensing.count())

#------------------------------ 3. Load finalized Dataframe to HDFS ------------------------------
    #write to HDFS template
    print("writing to hdfs")
    dispensing.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_dispensing.csv".format(site))

else:
    print("No Dispensing Data to ETL")