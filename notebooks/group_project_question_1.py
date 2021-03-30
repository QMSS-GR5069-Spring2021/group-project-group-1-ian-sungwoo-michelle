# Databricks notebook source
# MAGIC %md ### Group Project - Question 1

# COMMAND ----------

# MAGIC %md ##### Install MLFlow

# COMMAND ----------

dbutils.library.installPyPI("mlflow", "1.14.0")

# COMMAND ----------

# MAGIC %md ##### Import functions

# COMMAND ----------

import boto3
import pandas as pd
from datetime import datetime

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

# MAGIC %md ##### Read in Dataset

# COMMAND ----------

bucket = "columbia-gr5069-main"
f1_data = "raw/results.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= f1_data) 
df_results = pd.read_csv(obj_laps['Body'])

# COMMAND ----------

display(df_results)

# COMMAND ----------

