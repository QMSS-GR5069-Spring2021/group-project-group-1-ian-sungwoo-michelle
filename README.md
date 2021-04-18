# Group 1 Project Submission

**Team members**: Ian Henry Lightfoot, Sungwoo Park, Michelle A. Zee

Our project answers Questions 2, 3, 5. 

Final project deliverables can be found:
* Final report [link](https://docs.google.com/document/d/1r9--d895eDUfna46R_dUtVNab29V3w1jxeD70dXCdtU/edit?usp=sharing).
* Visualization dashboard [link](http://ec2-3-84-157-243.compute-1.amazonaws.com:8088/r/33).
* Presentation slides (updated!) [link](https://docs.google.com/presentation/d/1N9R94djlO4Y3IC7WHYJ2YJnBG_CjNYiH5ZXTs52JPcY/edit?usp=sharing)

The folders are organized as follows:


| -- [src](/src)

|     |-- [data](/src/date)            <- Code to read/munge raw data.

|     |-- [features](/src/features)        <- Code to transform/append data.

|     |-- [models](/src/models)          <- Code to analyze the data.

|     |-- [visualizations](/src/models)  <- Code to generate visualizations.

|

| -- [reports](/reports)

|     |-- [documents](/reports/documents)       <- Documents synthesizing the analysis.

|     |-- [model tracking](/reports/model_tracking) <- MLFlow model tracking summary.

|     |-- [figures](reports/figures)         <- Images from Superset dashboard.







# Project Instructions
##QUESTIONS WE ANSWERED

The F1 dataset contains a number of features available for you to use. Construct your dataset from all available datasets, and select the features that make the most sense to use to answer the questions below.

 

(2) [25pts] Now we move on to prediction. Fit a model using data from 1950:2010, and predict drivers that come in second place between 2011 and 2017. [Remember, this is a predictive model where variables are selected as the subset that is best at predicting the target variable and not for theoretical reasons. This means that your model should not overfit and most likely be different from the model in (1).]

From your fitted model:

describe your model, and explain how you selected the features that were selected
provide statistics that show how good your model is at predicting, and how well it performed predicting second places in races between 2011 and 2017
the most important variable in (1) is bound to also be included in your predictive model. Provide marginal effects or some metric of importance for this variable and make an explicit comparison of this value with the values that you obtained in (1). How different are they? Why are they different?
 


(3) [25pts] This task is inferential. You are going to try to explain why a constructor wins a season between 1950 and 2010. Fit a model using features that make theoretical sense to describe F1 racing between 1950 and 2010. Clean the data, and transform it as necessary, including dealing with missing data. [Remember, this will almost necessarily be an overfit model where variables are selected because they make sense to explain F1 races between 1950 and 2010, and not based on algorithmic feature selection]

From your fitted model:

describe your model, and explain why each feature was selected
provide statistics that show how well the model fits the data
what is the most important variable in your model? How did you determine that?
provide some marginal effects for the variable that you identified as the most important in the model, and interpret it in the context of F1 races: in other words, give us the story that the data is providing you about constructors that win seasons
does it make sense to think of it as an "explanation" for why a constructor wins a season? or is it simply an association we observe in the data?
 


(5) [25pts] This task is inferential. You are going to try to explain why a driver's performance improves between 1950 and 2010. Fit a model using features that make theoretical sense to describe F1 racing between 1950 and 2010. Clean the data, and transform it as necessary, including dealing with missing data. [Remember, this will almost necessarily be an overfit model where variables are selected because they make sense to explain F1 races between 1950 and 2010, and not based on algorithmic feature selection]

From your fitted model:

describe your model, and explain why each feature was selected
provide statistics that show how well the model fits the data
what is the most important variable in your model? How did you determine that?
provide some marginal effects for the variable that you identified as the most important in the model, and interpret it in the context of F1 races: in other words, give us the story that the data is providing you about a drivers performance improvements
does it make sense to think of it as an "explanation" for why a driver's performance improved? or is it simply an association we observe in the data?
 
 

## WHAT SHOULD YOUR REPO LOOK LIKE

What should be in your repo and your AWS S3 Bucket? [50pts]

# Github Repo- a well-structured project:

project\

|

| -- src

|     |-- data            <- Code to read/munge raw data.

|     |-- features        <- Code to transform/append data.

|     |-- models          <- Code to analyze the data.

|     |-- visualizations  <- Code to generate visualizations.

|

| -- reports

|     |-- documents       <- Documents synthesizing the analysis.

|     |-- figures         <- Images generated by the code.

|

| -- references           <- Data dictionaries, explanatory materials.

|

| -- README.md

 (Links to an external site.)

           <- Project description.

Make sure to have:

- a very informative landing page that guides you through the project

- well structured and modularized code

- well-commented code

- well commented commits

Above all, any person should be able to pick up your project and run it / build on it seamlessly

Also, please provide documentation of your model tracking with screenshots of your model experiments and explaining how you selected your best model as part of your explanation and story
