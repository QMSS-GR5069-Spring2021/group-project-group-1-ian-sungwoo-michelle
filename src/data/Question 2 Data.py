# Databricks notebook source
import boto3
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

#import data
bucket = "group1-gr5069"
data = "interim/races_results.csv/part-00000-tid-4925966825258030189-2b8c8ceb-1b86-40da-95b3-db85366bd567-60-1-c000.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
df = pd.read_csv(obj_laps['Body'])

# COMMAND ----------

display(df)
df.info(verbose=True)

# COMMAND ----------

#drop unnecessary columns
df_orig = df
df = df[["year", "raceId", "driverId", "driverRef", "circuitId", "dob", "nationality", "constructorId", "grid", "position", "laps"]]

# COMMAND ----------

display(df)
df.info(verbose=True)

# COMMAND ----------

#save only second place finishes 
df=df[df['position']==2]
display(df)

# COMMAND ----------

# MAGIC %md #### Save to S3

# COMMAND ----------

#convert to spark dataframe
df = spark.createDataFrame(df)

# COMMAND ----------

df.write.option("header", "true").csv('s3://group1-gr5069/interim/results_second_place.csv',mode="overwrite")