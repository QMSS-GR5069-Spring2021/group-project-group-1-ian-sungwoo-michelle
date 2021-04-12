# Databricks notebook source
linearM <- lm(wins ~ position + points + factor(nationality), data = constructorMerge)
summary(linearM)

# COMMAND ----------

importance <- varImp(linearM, scale = FALSE)
importance <- importance[order(-importance$Overall), , drop = FALSE] %>%
  rename(Importance = Overall)
head(importance)

# COMMAND ----------

ggpredict(linearM, "points")

# COMMAND ----------

