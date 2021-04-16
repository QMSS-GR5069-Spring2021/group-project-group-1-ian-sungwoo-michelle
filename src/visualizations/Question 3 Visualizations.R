# Databricks notebook source
from pyspark.sql.types import DoubleType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType
predDF = spark.read.csv('s3://group1-gr5069/processed/constructorMergeModels/part-00000-tid-932161557840838493-6910be16-f59b-495a-9846-6216092d48a8-90-1-c000.csv', header=True)
predDF.write.format('jdbc').options(
      url='jdbc:mysql://il-gr5069.ccqalx6jsr2n.us-east-1.rds.amazonaws.com/ian_test',
      driver='com.mysql.jdbc.Driver',
      dbtable='predDF',
      user='admin',
      password='v0lleyball').mode('overwrite').save()

# COMMAND ----------

