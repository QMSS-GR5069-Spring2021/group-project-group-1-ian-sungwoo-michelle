# Databricks notebook source
library(ggeffects)
library(MASS)
library(caret)
library(e1071)
library(randomForest)
library(SparkR)
library(carrier)
library(sparklyr)
library(dplyr)
library(lubridate)
library(mlflow)
install_mlflow()

SparkR::sparkR.session()
sc <- spark_connect(method = "databricks")

# COMMAND ----------

constructors <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/constructors.csv")
constructors <- as.data.frame(constructors)

# COMMAND ----------

constructorResults <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/constructor_results.csv")
constructorResults <- as.data.frame(constructorResults)

# COMMAND ----------

constructorStandings <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/constructor_standings.csv")
constructorStandings <- as.data.frame(constructorStandings)

# COMMAND ----------

races <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/races.csv")
races <- as.data.frame(races)

# COMMAND ----------

