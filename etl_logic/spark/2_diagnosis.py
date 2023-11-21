####################
#                  #
#     Not          #
#     Migrated     #
#     Yet          #
#                  #
####################

import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, when, isnull, array_contains, to_date, col, to_timestamp
from pyspark.sql.functions import trim, date_format, concat, lit, coalesce, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType, DecimalType, TimestampType, LongType, DoubleType
import re
from io import StringIO
import time
import json
import psycopg2
import csv
import traceback
from typing import Dict
import pandas

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

#load to spark template
print("Reading person into spark")
query = "SELECT MIN(person_id), MAX(person_id) FROM {}_pedsnet.person".format(site)
min_max_df = spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("query",query).load()
lowerBound, upperBound = min_max_df.collect()[0]
person = spark.read.format("jdbc")\
    .option("url",url)\
    .option("dbtable","{}_pedsnet.person".format(site))\
    .option("user",user)\
    .option("password",password)\
    .option("partitionColumn","person_id")\
    .option("numPartitions",200)\
    .option("lowerBound",lowerBound)\
    .option("upperBound",upperBound)\
    .load()
person.createOrReplaceTempView("person_visit_start2001")
person.cache()
print(person.count())

#ETL template
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

print('ETLing filter_obs')
filter_obs = spark.sql(filter_obs_query)
filter_obs.createOrReplaceTempView("filter_obs")
filter_obs.cache()
print(filter_obs.count())

#write to HDFS template
print("writing to hdfs")
meas_obsclin_final.write.option("numPartitions",1000).option('header','true').csv("/user/spark/{}_pcornet_airflow_obs_clin.csv".format(site))