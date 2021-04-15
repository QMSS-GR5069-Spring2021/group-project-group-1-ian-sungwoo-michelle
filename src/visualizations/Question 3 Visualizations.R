# Databricks notebook source
library(ggeffects)
library(ggplot2)
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

constructorMerge <- spark_read_csv(sc, path = "s3://group1-gr5069/processed/constructorMergeModels/part-00000-tid-932161557840838493-6910be16-f59b-495a-9846-6216092d48a8-90-1-c000.csv")
constructorMerge <- as.data.frame(constructorMerge)

# COMMAND ----------

# Wins by Position
ggplot(constructorMerge) +
  geom_jitter(stat = "identity", aes(x = position, y = wins), col = "red") +
  xlab("Average Starting Position") +
  ylab("Number of Wins") +
  ggtitle("Wins by Average Position") +
  theme(plot.title = element_text(hjust = 0.5)) +
  theme(plot.title = element_text(face = "bold"))

# COMMAND ----------

# Wins by Points
ggplot(constructorMerge) +
  geom_jitter(aes(x = points, y = wins), col = "darkmagenta") +
  xlab("Average Points") +
  ylab("Number of Wins") +
  ggtitle("Wins by Average Points") +
  theme(plot.title = element_text(hjust = 0.5)) +
  theme(plot.title = element_text(face = "bold"))

# COMMAND ----------

# Wins by Nationality
constructorMergeNat <- constructorMerge %>%
  group_by(nationality, continent) %>%
  summarise(wins = sum(wins)) %>%
  filter(wins > 0)
ggplot(constructorMergeNat) +
  geom_bar(stat="identity",aes(x = reorder(nationality, -wins), y = wins, fill = reorder(continent, -wins))) +
  theme(axis.text.x = element_text(angle=45, vjust = 0.5, size = 10)) +
  scale_fill_manual(values=c("darkmagenta", "blue", "red")) +
  xlab("Nationality") +
  ylab("Number of Wins") +
  ggtitle("Wins by Nationality") +
  theme(plot.title = element_text(hjust = 0.5)) +
  theme(plot.title = element_text(face = "bold")) +
  labs(fill = "Continent")

# COMMAND ----------

# Predicted Wins vs. Actual Wins
ggplot(constructorMerge) +
  geom_jitter(aes(x = wins, y = pred), col = "blue") +
  geom_abline(data = NULL) +
  xlab("Number of Wins") +
  ylab("Predicted Number of Wins") +
  ggtitle("Predicted Wins vs. Actual Wins") +
  theme(plot.title = element_text(hjust = 0.5)) +
  theme(plot.title = element_text(face = "bold"))

# COMMAND ----------

