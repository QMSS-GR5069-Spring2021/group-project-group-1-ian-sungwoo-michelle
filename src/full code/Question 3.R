# Databricks notebook source
install.packages("ggeffects")
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
constructors <- constructors %>%
  filter()

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

# MAGIC %md
# MAGIC After reading in our datasets, we will join them together based on Constructor ID. Next, we will merge them by Race ID in order to obtain our final dataset. Once we have this, we will filter out any years that are outside of our 1950 to 2010 range and select only our variables of interest. Once we only have our variables of interest in our dataset, we will delete observations that contain NAs or missing values because they will not contribute fully to our model.

# COMMAND ----------

constructorMerge <- left_join(constructors, constructorResults, by = "constructorId")
constructorMerge <- left_join(constructorMerge, constructorStandings, by = "constructorId")
constructorMerge <- constructorMerge %>%
  select(-raceId.x, -points.x) %>%
  rename(raceId = raceId.y,
        points = points.y)
constructorMerge <- left_join(constructorMerge, races, by = "raceId")
constructorMerge <- constructorMerge %>%
  filter(year >= 1950, year <= 2010) %>%
  select(wins, position, points, nationality)
constructorMerge <- constructorMerge[complete.cases(constructorMerge), ]

# COMMAND ----------

# MAGIC %md
# MAGIC We have chosen number of wins as the dependent variable, and position, points, and nationality as the independent variables. We will run a linear model on this data to analyze the most important factors.

# COMMAND ----------

linearM <- lm(wins ~ position + points + factor(nationality), data = constructorMerge)
summary(linearM)

# COMMAND ----------

# MAGIC %md
# MAGIC We chose to use points, position, and nationality because these seemed to be the most telling factors of why a constructor would have more wins. Both points and position are statistically significant at the 0.05 significance level, and almost all of the nationalities also show to be significant. The R-squared value is 0.6468, which means that about 65% of the variability in wins can be explained my the three explanatory variables. This is a surprisingly high number for the social sciences. 

# COMMAND ----------

importance <- varImp(linearM, scale = FALSE)
importance <- importance[order(-importance$Overall), , drop = FALSE] %>%
  rename(Importance = Overall)
head(importance)

# COMMAND ----------

# MAGIC %md
# MAGIC The varImp function uses the absolute value of the t-values in the linear model to determine the importance of our independent variables. As we can see from this output, the most important variable in our model is points. Next, we will look at the marginal effects of points on number of wins for constructors. 

# COMMAND ----------

ggpredict(linearM, "points")

# COMMAND ----------

# MAGIC %md
# MAGIC We can see in the above output the predicted number of wins based on points when controlled for position and nationality. Constructors are expected to have slightly over two wins when they have 50 points, versus close to 18 wins when they have 500 points. 
# MAGIC 
# MAGIC Looking at the marginal effect of the number of points on the number of wins, the t-statistics and p-values, as well as the R-squared for our model, it makes sense that points is a huge determining factor in the number of wins that a constructor has. While it may not explain 100% of the variability in number of wins, points definitely shows to be a very influential variable in the process.

# COMMAND ----------

