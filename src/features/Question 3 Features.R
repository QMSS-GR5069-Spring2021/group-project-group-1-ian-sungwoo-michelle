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

constructorMerge <- spark_read_csv(sc, path = "s3://group1-gr5069/interim/constructorMergeData/part-00000-tid-6940441156180946538-cedc5516-09bc-41d5-9358-137f85c8a006-420-1-c000.csv")
constructorMerge <- as.data.frame(constructorMerge)

# COMMAND ----------

# Group data by constructor Id and wins, take mean points and positions
constructorMerge <- constructorMerge %>%
  group_by(constructorId, wins) %>%
  summarise(points = mean(points),
           nationality = nationality[1],
           position = mean(position))
constructorMerge <- as.data.frame(constructorMerge)

# COMMAND ----------

#Add continent variable
constructorMerge$continent <- rep("blank", nrow(constructorMerge))

asia <- c("Japanese", "Indian", "Hong Kong", "Malaysian")
africa <- c("South African", "Rhodesian")
americas <- c("Brazilian", "Canadian", "Mexican", "American")
australia <- c("Australian", "New Zealand")
europe <- c("British", "German", "French", "Italian", "Austrian", "Dutch", "Russian", "Swiss", "Irish", "Belgium", "Spanish")

for(i in 1:nrow(constructorMerge)){
  if(constructorMerge$nationality[i] %in% asia){
    constructorMerge$continent[i] <- "Asia"
  }
  else if(constructorMerge$nationality[i] %in% africa){
    constructorMerge$continent[i] <- "Africa"
  }
  else if(constructorMerge$nationality[i] %in% americas){
    constructorMerge$continent[i] <- "Americas"
  }
  else if(constructorMerge$nationality[i] %in% australia){
    constructorMerge$continent[i] <- "Australia"
  }
  else if(constructorMerge$nationality[i] %in% europe){
    constructorMerge$continent[i] <- "Europe"
  }
}

# COMMAND ----------

tbl <- copy_to(sc, constructorMerge, OVERWRITE=TRUE)
spark_write_csv(tbl, 's3://group1-gr5069/interim/constructorMergeFeatures')

# COMMAND ----------

