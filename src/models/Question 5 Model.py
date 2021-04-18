# Databricks notebook source
# MAGIC %md #### Random Forest Model

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt

from numpy import savetxt

from sklearn.model_selection import train_test_split

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

# load data
df = spark.read.csv('s3://group1-gr5069/interim/q5_features_w_dv.csv', header=True, inferSchema = True)
display(df)

# COMMAND ----------

df = df.toPandas()
display(df)

# COMMAND ----------

# split training/ test data
X = df.drop(['improve', 'position', 'date', 'dob', 'nationality'], axis = 1)
y = df.improve
X_train, X_test, y_train, y_test = train_test_split(X, y)

# COMMAND ----------

# enable autoblog
mlflow.sklearn.autolog()

with mlflow.start_run(run_name = "Basic RF Experiment") as run:
  
  # Set the model parameters. 
  n_estimators = 100
  max_depth = 6
  max_features = 3
  
  # Create and train model.
  rf = RandomForestRegressor(n_estimators = n_estimators, max_depth = max_depth, max_features = max_features)
  rf.fit(X_train, y_train)
  predictions = rf.predict(X_test)

  # Log model
  mlflow.sklearn.log_model(rf, "random-forest-model")
  
  # Create metrics
  mse = mean_squared_error(y_test, predictions)
  print("  mse: {}".format(mse))
  
  # Log metrics
  mlflow.log_metric("mse", mse)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print("Inside MLflow Run with run_id {} and experiment_id {}".format(runID, experimentID))


# COMMAND ----------

def log_rf(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
  import tempfile

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(**params)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")

    # Log params
    [mlflow.log_param(param, value) for param, value in params.items()]

    # Create metrics
    mse = mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    print("  mse: {}".format(mse))
    print("  mae: {}".format(mae))
    print("  R2: {}".format(r2))

    # Log metrics
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("mae", mae)  
    mlflow.log_metric("r2", r2)  
    
    # Create feature importance
    importance = pd.DataFrame(list(zip(df.columns, rf.feature_importances_)), 
                                columns=["Feature", "Importance"]
                              ).sort_values("Importance", ascending=False)
    
    # Log importances using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="feature-importance-", suffix=".csv")
    temp_name = temp.name
    try:
      importance.to_csv(temp_name, index=False)
      mlflow.log_artifact(temp_name, "feature-importance.csv")
    finally:
      temp.close() # Delete the temp file
    
    # Create plot
    fig, ax = plt.subplots()

    sns.residplot(predictions, y_test, lowess=True)
    plt.xlabel("Predicted Race Ranking Position")
    plt.ylabel("Residual")
    plt.title("Residual Plot")

    # Log residuals using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="residuals-", suffix=".png")
    temp_name = temp.name
    try:
      fig.savefig(temp_name)
      mlflow.log_artifact(temp_name, "residuals.png")
    finally:
      temp.close() # Delete the temp file
      
    display(fig)
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md #### Run with Different Metrics

# COMMAND ----------

# run with new parameters
params = {
  "n_estimators": 100,
  "max_depth": 5,
  "random_state": 42
}

log_rf(experimentID, "Second Run", params, X_train, X_test, y_train, y_test)


# COMMAND ----------

# third run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 10,
  "random_state": 42
}

log_rf(experimentID, "Third Run", params_1000_trees, X_train, X_test, y_train, y_test)


# COMMAND ----------

# 4th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 15,
  "random_state": 42
}

log_rf(experimentID, "Fourth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 5th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 100,
  "random_state": 42
}

log_rf(experimentID, "Fifth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 6th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 50,
  "random_state": 42
}

log_rf(experimentID, "Sixth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 7th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 20,
  "random_state": 42
}

log_rf(experimentID, "Seventh Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 8th run
params_1000_trees = {
  "n_estimators": 500,
  "max_depth": 20,
  "random_state": 42
}

log_rf(experimentID, "Eighth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 9th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 18,
  "random_state": 42
}

log_rf(experimentID, "Nineth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 10th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 16,
  "random_state": 42
}

log_rf(experimentID, "Tenth Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# 11th run
params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 17,
  "random_state": 30
}

log_rf(experimentID, "Eleventh Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md #### Final Model

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC The metrics that imply the best fitting model is from the Tenth Run. Using "n_estimators": 1000, "max_depth": 11, "random_state": 42. 
# MAGIC 
# MAGIC This set of parameters seemed to give the lowest MSE and the highest R-squared, indicating the best fit.

# COMMAND ----------

rf = RandomForestRegressor(n_estimators = 1000, max_depth = 16, max_features = 3)
rf.fit(X_train, y_train)
predictions = rf.predict(X)

# COMMAND ----------

df['predictions'] = predictions

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.createDataFrame(df)

# COMMAND ----------

df.write.format('jdbc').options(
      url='jdbc:mysql://maz-gr5069.ccqalx6jsr2n.us-east-1.rds.amazonaws.com/michelle_test',
      driver='com.mysql.jdbc.Driver',
      dbtable='project_q5_preds',
      user='admin',
      password='').mode('overwrite').save()