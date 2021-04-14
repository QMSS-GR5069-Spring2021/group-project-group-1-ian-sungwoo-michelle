# Databricks notebook source
library(sparklyr)
require(sparkR)
library(dplyr)

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

standing <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/driver_standings.csv", infer_schema = TRUE)
standing

# COMMAND ----------

races <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/races.csv", infer_schema = TRUE)
races <- races %>% mutate(date = to_date(date, "yyyy-MM-dd")) %>% select(raceId, date)
races

# COMMAND ----------

drivers <- spark_read_csv(sc, path = "s3://columbia-gr5069-main/raw/drivers.csv", infer_schema = TRUE)
drivers <- drivers %>% mutate(dob = to_date(dob, "yyyy-MM-dd")) %>% select(driverId, dob, nationality)
drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join dataframes

# COMMAND ----------

joined <- standing %>% 
  left_join(races, by = "raceId") %>%
  left_join(drivers, by = "driverId") %>%
  select(-positionText, -driverStandingsId, - raceId) %>%
  arrange(driverId, desc(date))
joined

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create variables

# COMMAND ----------

# age variable
joined <- joined %>%
  mutate(age = date - dob) %>%
  select(-dob)
joined

# COMMAND ----------

# add previous position column
joined <- joined %>% mutate(position_prev = position) %>%
  select(driverId, position, position_prev, points, wins, date, nationality, age)
joined

# COMMAND ----------

# shift position_prev values up one row
shift <- function(x, n){ # function that shifts values in column
  c(x[-(seq(n))], rep(NA, n))
}

joined$position_prev <- shift(joined$position_prev, 1)
joined