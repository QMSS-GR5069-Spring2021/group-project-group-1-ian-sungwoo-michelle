# Databricks notebook source
# MAGIC %md #### Import F1 datasets

# COMMAND ----------

import boto3
import pandas as pd
import numpy as np

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

bucket = "group1-gr5069"
data = "processed/races_results.csv/part-00000-tid-368662250573169207-9474530c-8de3-4295-8ae1-4d436993538b-146-1-c000.csv"

obj_laps = s3.get_object(Bucket= bucket, Key= data) 
df = pd.read_csv(obj_laps['Body'])

# COMMAND ----------

display(df)
df.count()

# COMMAND ----------

#drop unnecessary columns
df_orig = df
df = df[["year", "raceId", "driverId", "driverRef", "dob", "nationality", "constructorId", "grid", "position", "laps"]]

# COMMAND ----------

display(df)
df.info(verbose=True)

# COMMAND ----------

#save only second place finishes 
df=df[df['position']==2]
display(df)

# COMMAND ----------

# MAGIC %md #### Perform a train/test split

# COMMAND ----------

#filter between test and training data

X_train = df.drop(["driverRef"],axis=1).loc[df['year']<= 2010]
X_test = df.drop(["driverRef"],axis=1).loc[df['year'] > 2010]

y_train = df["driverRef"].loc[df['year']<= 2010]
y_test = df["driverRef"].loc[df['year']> 2010]

y_train = np.array(y_train)
y_test = np.array(y_test)

#Replace NaN values in X_test and X_train with the mean

X_test = X_test.fillna(X_test.mean())
X_train = X_train.fillna(X_train.mean())


# COMMAND ----------

display(X_train)
display(X_test)

# COMMAND ----------

# MAGIC %md #### Preprocessing - one hot encoder

# COMMAND ----------

# Get list of categorical variables
s = (X_train.dtypes == 'object')
object_cols = list(s[s].index)

print(object_cols)

# COMMAND ----------

# Apply one-hot encoder to each column with categorical data
OH_encoder = OneHotEncoder(handle_unknown='ignore', sparse=False)

OH_cols_train = pd.DataFrame(OH_encoder.fit_transform(X_train[object_cols]))
OH_cols_test = pd.DataFrame(OH_encoder.transform(X_test[object_cols]))

# One-hot encoding removed index; put it back
OH_cols_train.index = X_train.index
OH_cols_test.index = X_test.index

# Remove categorical columns (will replace with one-hot encoding)
num_X_train = X_train.drop(object_cols, axis=1)
num_X_test = X_test.drop(object_cols, axis=1)

# Add one-hot encoded columns to numerical features
OH_X_train = pd.concat([num_X_train, OH_cols_train], axis=1)
OH_X_test = pd.concat([num_X_test, OH_cols_test], axis=1)


# COMMAND ----------

OH_X_train.info(verbose=True)
OH_X_test.info(verbose=True)

# COMMAND ----------

# MAGIC %md #### Predict second place finishes

# COMMAND ----------

import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier 
from sklearn.metrics import accuracy_score


with mlflow.start_run(run_name="Basic DT Experiment") as run:
  
  # Create model, train it
  dt = DecisionTreeClassifier()
  dt = dt.fit(OH_X_train,y_train)
  predictions = dt.predict(OH_X_test)
  
  # Log model
  mlflow.sklearn.log_model(dt, "decision-tree-model")
  
  # report the model performance
  accuracy_score(y_test, predictions)
  
  print("Accuracy:", accuracy_score(y_test, predictions))

# COMMAND ----------

OH_X_test['predictions']=predictions
display(OH_X_test)

# COMMAND ----------

X_test_preds = pd.merge(X_test,OH_X_test[['driverId','raceId','predictions']],on=['driverId','raceId'],how='left')
X_test_preds = pd.merge(X_test_preds,df_orig[['raceId','driverId','driverRef']],on=['driverId','raceId'],how='left')
display(X_test_preds)

# COMMAND ----------

# MAGIC %md #### Save to S3

# COMMAND ----------

predictions_final = spark.createDataFrame(X_test_preds)
display(predictions_final)

# COMMAND ----------

predictions_final.write.format('jdbc').options(
      url='jdbc:mysql://sp-gr5069.ccqalx6jsr2n.us-east-1.rds.amazonaws.com/sp_test',
      driver='com.mysql.jdbc.Driver',
      dbtable='second_place_preds',
      user='admin',
      password='VgCrEPeYKWaaIZZhoYHt').mode('overwrite').save()