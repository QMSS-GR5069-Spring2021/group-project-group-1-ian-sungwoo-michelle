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

# load races data
df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header=True, inferSchema = True)
display(df_races)

# COMMAND ----------

# load constructors data
df_constructors = spark.read.csv('s3://columbia-gr5069-main/raw/constructors.csv', header=True, inferSchema = True)
display(df_constructors)

# COMMAND ----------

# load drivers data
df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True, inferSchema = True)
display(df_drivers)

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

# change null duration to 0
df_position_pitstop = df_position_pitstop.na.fill(0)
display(df_position_pitstop)

# COMMAND ----------

#change year schema type
df_races = df_races.withColumn('year', df_races['year'].cast(IntegerType()))
df_races.printSchema

# COMMAND ----------

#Combine df_race and df_results
df_races_results = df_races.select('raceId','year','round','circuitId','name','date').join(df_results,on=['raceId'])
display(df_races_results)

# COMMAND ----------

#Combine df_races_results and df_constructors
df_constructors = df_constructors.withColumnRenamed('name','Constructor Name')
df_races_results = df_constructors.select('constructorId','Constructor Name').join(df_races_results,on=['constructorId'])
display(df_races_results)

# COMMAND ----------

#Combine df_races_results and df_drivers
df_races_results = df_drivers.select('driverId','driverRef','dob','nationality').join(df_races_results,on='driverId')
display(df_races_results)

# COMMAND ----------

# remove obs where position is null for df_position_pitstop
df_races_results = df_races_results.filter('position IS NOT NULL')
df_races_results.count()

# COMMAND ----------

# MAGIC %md #### Store data in S3

# COMMAND ----------

df_position_pitstop.write.option("header", "true").csv('s3://group1-gr5069/interim/position_pitstop.csv',mode="overwrite")

# COMMAND ----------

df_races_results.write.option("header", "true").csv('s3://group1-gr5069/interim/races_results.csv',mode="overwrite")

# COMMAND ----------

df_pitstops = spark.read.csv('s3://group1-gr5069/interim/position_pitstop.csv', header=True, inferSchema = True)
display(df_pitstops)

# COMMAND ----------

df_races_results = spark.read.csv('s3://group1-gr5069/interim/races_results.csv', header=True, inferSchema = True)
display(df_races_results)