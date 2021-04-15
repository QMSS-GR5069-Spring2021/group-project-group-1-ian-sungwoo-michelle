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

constructorMerge <- spark_read_csv(sc, path = "s3://group1-gr5069/interim/constructorMergeFeatures/part-00000-tid-7375351131602280794-c1c7d446-43bd-4001-92ba-14a28d296dd3-70-1-c000.csv")
constructorMerge <- as.data.frame(constructorMerge)

# COMMAND ----------

# Create & run linear model
linearM <- lm(wins ~ position + points + factor(continent), data = constructorMerge)
summary(linearM)

# COMMAND ----------

# Dispplay most important variable
importance <- varImp(linearM, scale = FALSE)
importance <- importance[order(-importance$Overall), , drop = FALSE] %>%
  rename(Importance = Overall)
head(importance)

# COMMAND ----------

# Marginal effect shown clearer
ggpredict(linearM, "points")

# COMMAND ----------

# Create prediction variable
pred <- predict(linearM, newdata=constructorMerge)
constructorMerge$pred <- pred

# COMMAND ----------

funA3 <- function(ntree, mtry, data){
  with(mlflow_start_run(nested = TRUE), {
    
    # Create model
    rf <- randomForest(wins ~ position + points + nationality, data=data, ntree=ntree, mtry=mtry)
    
    # Use the model to make predictions on the test dataset
    predrf <- round(predict(rf, newdata=constructorMergeFac), 0)
    
    # Log the model parameters used for this run
    mlflow_log_param("ntree", ntree)
    mlflow_log_param("mtry", mtry)
  
    # Define metrics to evaluate the model
    cm <- confusionMatrix(factor(predrf), reference = factor(data[,"wins"]))
    sensitivity <- cm[["byClass"]][1,1]
    specificity <- cm[["byClass"]][1,2]
  
    # Log the value of the metrics 
    mlflow_log_metric("sensitivity", sensitivity)
    mlflow_log_metric("specificity", specificity)
  
    # Log the model
    predictor <- crate(function(x) stats::predict(rf,.x))
    mlflow_log_model(predictor, "modelrf")
  })
}

# COMMAND ----------

constructorMergeFac <- constructorMerge %>%
  mutate_if(is.character, as.factor)

# Run ML Flow 9 times with different parameters
funA3(50, 1, constructorMergeFac)
funA3(50, 2, constructorMergeFac) # Best (based on sensitivity & specificity)
funA3(50, 3, constructorMergeFac)
funA3(75, 1, constructorMergeFac)
funA3(75, 2, constructorMergeFac)
funA3(75, 3, constructorMergeFac)
funA3(100, 1, constructorMergeFac)
funA3(100, 2, constructorMergeFac)
funA3(100, 3, constructorMergeFac)

# COMMAND ----------

rf <- randomForest(wins ~ position + points + nationality, data=constructorMergeFac, ntree=50, mtry=2)
predrf <- predict(rf, newdata=constructorMergeFac)

# COMMAND ----------

# Compare mean differences in prediction/fit ability
diffrf <- mean(predrf - constructorMerge$wins)
difflm <- mean(constructorMerge$pred - constructorMerge$wins)
cbind(diffrf, difflm)

# COMMAND ----------

tbl <- copy_to(sc, constructorMerge, OVERWRITE=TRUE)
spark_write_csv(tbl, 's3://group1-gr5069/processed/constructorMergeModels')

# COMMAND ----------

