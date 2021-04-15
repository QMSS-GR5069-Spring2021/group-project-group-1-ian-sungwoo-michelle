# Databricks notebook source
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, current_date, avg

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

# load data
df = spark.read.csv('s3://group1-gr5069/interim/q5_joined_df.csv', header=True, inferSchema = True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create features

# COMMAND ----------

# lag position to create position in last race
df = df.withColumn("position_prev", lag("position").over(Window.partitionBy("driverid").orderBy("date")))
display(df)

# COMMAND ----------

# age variable
df = df.withColumn("age", datediff(df.date, df.dob)/365)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to S3

# COMMAND ----------

df.write.option("header", "true").csv('s3://group1-gr5069/interim/q5_features.csv')