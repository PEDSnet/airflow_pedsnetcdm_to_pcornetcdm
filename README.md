# airflow_pedsnetcdm_to_pcornetcdm
Migration of Old pedsnetcdm_to_pcornetcdm into Airflow utilizing spark

Currently defined to ETL PEDSnet Version 5.2 into PCORnet Version 6.1

Details of contents:
- pedsnet_to_pcornet_ETL.py contains the DAG definition and it's corresponding tasks
- ETL_logic directory contains
  - /data directory containing .csv files used to generate mapping tables in postgres
  - /sql directory containing .sql files executed in the pedsnetcdm_to_pcornetcdm DAG by SQL Operator Tasks
  - /spark directory containing .py PySpark files executed in the pedsnetcdm_to_pcornetcdm DAG by Spark Operator Tasks. Logic was migrated out .sql and into Pyspark to enhance runtime performance
