# Databricks notebook source
install.packages("ggeffects")
install.packages("SparkR")
install.packages("e1071")
install.packages("carrier")
install.packages("mlflow")
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

# Merge & tidy up datasets
constructorMerge <- left_join(constructors, constructorResults, by = "constructorId")
constructorMerge <- left_join(constructorMerge, constructorStandings, by = "constructorId")
constructorMerge <- constructorMerge %>%
  select(-raceId.x, -points.x) %>%
  rename(raceId = raceId.y,
        points = points.y)
constructorMerge <- left_join(constructorMerge, races, by = "raceId")
constructorMerge <- constructorMerge %>%
  filter(year >= 1950, year <= 2010) %>%
  select(constructorId, wins, position, points, nationality)
constructorMerge <- constructorMerge[complete.cases(constructorMerge), ]

# COMMAND ----------

tbl <- copy_to(sc, constructorMerge, OVERWRITE=TRUE)
spark_write_csv(tbl, 's3://group1-gr5069/interim/constructorMergeData')

# COMMAND ----------

