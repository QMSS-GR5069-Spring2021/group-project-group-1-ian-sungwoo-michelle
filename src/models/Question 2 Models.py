# Databricks notebook source
# MAGIC %md #### Import datasets

# COMMAND ----------

import boto3
import pandas as pd
import numpy as np
import io
import pickle

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

#import and load Y pickles

y_test_data = io.BytesIO()
s3.download_fileobj('group1-gr5069', 'interim/y_test_data.pkl', y_test_data)
y_test_data.seek(0)
y_test = pickle.load(y_test_data)

y_train_data = io.BytesIO()
s3.download_fileobj('group1-gr5069', 'interim/y_train_data.pkl', y_train_data)
y_train_data.seek(0)
y_train = pickle.load(y_train_data)

# COMMAND ----------

#import X datasets

bucket = "group1-gr5069"
data = "interim/OH_X_test.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
OH_X_test = pd.read_csv(obj_laps['Body'], index_col=0)

data = "interim/OH_X_train.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
OH_X_train = pd.read_csv(obj_laps['Body'], index_col=0)

data = "interim/X_test.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
X_test = pd.read_csv(obj_laps['Body'], index_col=0)

# COMMAND ----------

#import df_orig

bucket = "group1-gr5069"
data = "interim/df_orig.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
df_orig = pd.read_csv(obj_laps['Body'], index_col=0)

# COMMAND ----------

display(OH_X_test)
display(OH_X_train)

# COMMAND ----------

# MAGIC %md #### Predict second place drivers - basic run

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment basic") as run:
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train,y_train)
  predictions = dt.predict(OH_X_test)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # Log metrics
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Inside MLflow Run with run_id {} and experiment_id {}".format(runID, experimentID))
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 2

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no grid") as run:
  
  OH_X_train_temp = OH_X_train.drop('grid',1)
  OH_X_test_temp = OH_X_test.drop('grid',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 3

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no laps") as run:
  
  OH_X_train_temp = OH_X_train.drop('laps',1)
  OH_X_test_temp = OH_X_test.drop('laps',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 4

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no driverId") as run:
  
  OH_X_train_temp = OH_X_train.drop('driverId',1)
  OH_X_test_temp = OH_X_test.drop('driverId',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 5

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no raceId") as run:
  
  OH_X_train_temp = OH_X_train.drop('raceId',1)
  OH_X_test_temp = OH_X_test.drop('raceId',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 6

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no circuitId") as run:
  
  OH_X_train_temp = OH_X_train.drop('circuitId',1)
  OH_X_test_temp = OH_X_test.drop('circuitId',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 7

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no constructorId") as run:
  
  OH_X_train_temp = OH_X_train.drop('constructorId',1)
  OH_X_test_temp = OH_X_test.drop('constructorId',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 8

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no position") as run:
  
  OH_X_train_temp = OH_X_train.drop('position',1)
  OH_X_test_temp = OH_X_test.drop('position',1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

# MAGIC %md #### Run 9

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score

with mlflow.start_run(run_name="DT Experiment - no grid, laps, circuitId, position") as run:
  
  OH_X_train_temp = OH_X_train.drop(['grid', 'laps', 'circuitId', 'position'],1)
  OH_X_test_temp = OH_X_test.drop(['grid', 'laps', 'circuitId', 'position'],1)
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train_temp, y_train)
  predictions = dt.predict(OH_X_test_temp)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

#Add predictions to dataset

OH_X_test['predictions']=predictions
display(OH_X_test)

# COMMAND ----------

#Merge datasets back into original format

X_test_preds = pd.merge(X_test,OH_X_test[['driverId','raceId','predictions']],on=['driverId','raceId'],how='left')
X_test_preds = pd.merge(X_test_preds,df_orig[['raceId','driverId','driverRef']],on=['driverId','raceId'],how='left')
display(X_test_preds)

# COMMAND ----------

# MAGIC %md #### Save to S3

# COMMAND ----------

#convert to spark dataframe
predictions_final = spark.createDataFrame(X_test_preds)

# COMMAND ----------

predictions_final.write.option("header", "true").save('s3://group1-gr5069/processsed/preds_second_place.csv',mode="overwrite")