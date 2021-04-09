# Databricks notebook source
# MAGIC %md #### Import F1 datasets

# COMMAND ----------

import boto3
import pandas as pd

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

bucket = "group1-gr5069"
data = "processed/results.csv/part-00000-tid-3171662921362886721-50724c08-b406-4336-982c-b2080862b27b-28-1-c000.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
df = pd.read_csv(obj_laps['Body'])

# COMMAND ----------

display(df)

# COMMAND ----------

df = df[["raceId", "driverId", "constructorId", "grid", "position", "laps"]]

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md #### Perform a train/test split

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(df.drop(["position"], axis=1), df[["position"]].values.ravel(), random_state=42)

#Replace NaN values in X_test and X_train with the mean

X_test = X_test.fillna(X_test.mean())
X_train = X_train.fillna(X_train.mean())


# COMMAND ----------

# MAGIC %md #### Perform a test run

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="Basic RF Experiment") as run:
  # Create model, train it, and create predictions
  rf = RandomForestRegressor()
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

# MAGIC %md #### Create function to log parameters, metrics, and other artifacts

# COMMAND ----------

def log_rf(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, r2_score, explained_variance_score
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
    r2 = r2_score(y_test, predictions)
    evs = explained_variance_score(y_test, predictions)  
    
    print("  mse: {}".format(mse))
    print("  r2: {}".format(r2))
    print("  evs: {}".format(evs))
 
    # Log metrics
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("r2", r2)  
    mlflow.log_metric("evs", evs)  
    
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
    plt.xlabel("Predicted position")
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

# MAGIC %md #### First Run

# COMMAND ----------

params = {
  "n_estimators": 1000,
  "max_depth": 10,
  "random_state": 42
}

log_rf(experimentID, "First Run", params, X_train, X_test, y_train, y_test)