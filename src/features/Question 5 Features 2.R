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

joined <- sparklyr::spark_read_csv(sc, path = 's3://group1-gr5069/interim/q5_features.csv', inferSchema = TRUE)
joined

# COMMAND ----------

# MAGIC %md
# MAGIC ### Edit variables

# COMMAND ----------

# add previous position column
joined <- joined %>%
  na.omit()
joined

# COMMAND ----------

# create dv improve -- improvement from prev position variable
joined <- joined %>% mutate(improve = ifelse(position < position_prev, 1, 0))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to S3

# COMMAND ----------

tbl <- copy_to(sc, joined, OVERWRITE=TRUE)

# COMMAND ----------

spark_write_csv(tbl, 's3://group1-gr5069/interim/q5_features_w_dv.csv', header = TRUE)