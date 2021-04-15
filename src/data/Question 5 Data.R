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

# drivers standing, e.g. position and wins for each driver/ race
standing <- sparklyr::spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/driver_standings.csv", infer_schema = TRUE)
standing

# COMMAND ----------

# race info for race date
races <- sparklyr::spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/races.csv", infer_schema = TRUE)
races <- races %>% 
  dplyr::mutate(date = to_date(date, "yyyy-MM-dd")) %>% 
  dplyr::select(raceId, date)
races

# COMMAND ----------

# driver info, with birthdate
drivers <- sparklyr::spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/drivers.csv", infer_schema = TRUE)
drivers <- drivers %>% 
  dplyr::mutate(dob = to_date(dob, "yyyy-MM-dd")) %>% 
  dplyr::select(driverId, dob, nationality)
drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join dataframes

# COMMAND ----------

joined <- standing %>% 
  dplyr::left_join(races, by = "raceId") %>%
  dplyr::left_join(drivers, by = "driverId") %>%
  dplyr::select(-positionText, -driverStandingsId, - raceId) %>%
  dplyr::arrange(driverId, desc(date))
joined

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to S3

# COMMAND ----------

#spark_write_csv(joined, 's3://group1-gr5069/interim/q5_joined_df.csv', header = TRUE)