# Databricks notebook source
library(sparklyr)
library(SparkR)
library(dplyr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

joined <- sparklyr::spark_read_csv(sc, path = 's3://group1-gr5069/interim/q5_joined_df.csv', inferSchema = TRUE)
joined

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create variables

# COMMAND ----------

# age variable
joined <- joined %>%
  dplyr::mutate(age = date - dob) %>%
  dplyr::select(-dob)
joined

# COMMAND ----------

# add previous position column
joined <- joined %>% 
  dplyr::mutate(position_prev = position) %>%
  dplyr::select(driverId, position, position_prev, points, wins, date, nationality, age)
joined

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to S3

# COMMAND ----------

tbl <- copy_to(sc, joined, OVERWRITE=TRUE)

# COMMAND ----------

spark_write_csv(tbl, 's3://group1-gr5069/interim/q5_features.csv', header = TRUE)