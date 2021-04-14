# Databricks notebook source
import boto3
import pandas as pd
import numpy as np

# COMMAND ----------

s3 = boto3.client('s3')

# COMMAND ----------

#import data
df = spark.read.load("s3n://group1-gr5069/interim/results_second_place.csv")
df = df.toPandas()

# COMMAND ----------

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

from sklearn.preprocessing import OneHotEncoder

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

# MAGIC %md #### Save to S3

# COMMAND ----------

#convert to spark dataframe
OH_X_train = spark.createDataFrame(OH_X_train)
OH_X_test = spark.createDataFrame(OH_X_test)

# COMMAND ----------

#convert numpy to pandas then to spark

y_train = pd.DataFrame(y_train)
y_test = pd.DataFrame(y_test)

y_train = spark.createDataFrame(y_train)
y_test = spark.createDataFrame(y_test)

# COMMAND ----------

OH_X_train.write.option("header", "true").save('s3://group1-gr5069/interim/OH_X_train.csv',mode="overwrite")
OH_X_test.write.option("header", "true").save('s3://group1-gr5069/interim/OH_X_test.csv',mode="overwrite")

y_train.write.option("header", "true").save('s3://group1-gr5069/interim/y_train.csv',mode="overwrite")
y_test.write.option("header", "true").save('s3://group1-gr5069/interim/y_test.csv',mode="overwrite")