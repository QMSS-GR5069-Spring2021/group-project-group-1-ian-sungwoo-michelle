# Databricks notebook source
import boto3
import pandas as pd

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

#import data
df = spark.read.load("s3n://group1-gr5069/processsed/preds_second_place.csv")
df = df.toPandas()

# COMMAND ----------

display(df)

# COMMAND ----------

df['pred_correct'] = (df['predictions']==df['driverRef']).astype(int)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md #### Save to RDS

# COMMAND ----------

#convert to spark dataframe
predictions_final = spark.createDataFrame(df)

# COMMAND ----------

predictions_final.write.format('jdbc').options(
      url='jdbc:mysql://sp-gr5069.ccqalx6jsr2n.us-east-1.rds.amazonaws.com/sp_test',
      driver='com.mysql.jdbc.Driver',
      dbtable='second_place_preds',
      user='admin',
      password='VgCrEPeYKWaaIZZhoYHt').mode('overwrite').save()