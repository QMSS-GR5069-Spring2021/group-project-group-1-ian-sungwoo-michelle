# Databricks notebook source
# MAGIC %md 
# MAGIC ### Prep Data
# MAGIC ### Michelle A. Zee

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StructType, StructField, DecimalType

# COMMAND ----------

# MAGIC %md #### Load data

# COMMAND ----------

# load pitstop data
df_pitstops = spark.read.csv('s3://columbia-gr5069-main/raw/pit_stops.csv', header=True, inferSchema = True)
display(df_pitstops)

# COMMAND ----------

# load results data
df_results = spark.read.csv('s3://columbia-gr5069-main/raw/results.csv', header=True, inferSchema = True)
display(df_results)

# COMMAND ----------

# MAGIC %md #### Transform data

# COMMAND ----------

# change duration schema type
df_pitstops = df_pitstops.withColumn('duration', df_pitstops['duration'].cast(DecimalType(5,0)))
df_pitstops.printSchema

# COMMAND ----------

# change position schema type
df_results = df_results.withColumn('position', df_results['position'].cast(IntegerType()))
df_results.printSchema

# COMMAND ----------

# change miliseconds schema type
df_results = df_results.withColumn('milliseconds', df_results['milliseconds'].cast(IntegerType()))
df_results.printSchema

# COMMAND ----------

# change rank schema type
df_results = df_results.withColumn('rank', df_results['rank'].cast(IntegerType()))
df_results.printSchema

# COMMAND ----------

# pivot table wider
df_pitstops_wide = df_pitstops.groupBy("driverId", "raceId").pivot("stop").max("duration")
display(df_pitstops_wide)

# COMMAND ----------

# join pitstop and position data
df_position_pitstop = df_results.select('driverId', 'raceId', 'position').join(df_pitstops_wide, on=['driverId', 'raceId'])
display(df_position_pitstop)
df_position_pitstop.count()

# COMMAND ----------

# remove obs where position is null for df_position_pitstop
df_position_pitstop = df_position_pitstop.filter('position IS NOT NULL')
df_position_pitstop.count()


# COMMAND ----------

# remove obs where position is null for df_results
df_results = df_results.filter('position IS NOT NULL')
df_results.count()

# COMMAND ----------

# change null duration to 0
df_position_pitstop = df_position_pitstop.cast
display(df_position_pitstop)

# COMMAND ----------

# MAGIC %md #### Store data in S3

# COMMAND ----------

df_position_pitstop.write.option("header", "true").csv('s3://group1-gr5069/processed/position_pitstop.csv')

# COMMAND ----------

df_results.write.option("header", "true").csv('s3://group1-gr5069/processed/results.csv')

# COMMAND ----------

df_pitstops = spark.read.csv('s3://group1-gr5069/processed/position_pitstop.csv', header=True, inferSchema = True)
display(df_pitstops)

# COMMAND ----------

df_results = spark.read.csv('s3://group1-gr5069/processed/results.csv', header=True, inferSchema = True)
display(df_results)