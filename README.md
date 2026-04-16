# Databricks Player Churn Prediction

End-to-end churn prediction project built in Azure Databricks.

## What I built
- Generated a 5,000 player dataset and saved it as a **Delta table**
- Explored churn patterns using **SQL** directly in Databricks notebooks
- Trained and compared two **Random Forest** models using **MLflow** experiment tracking
- Used MLflow run comparison to select the optimal model based on F1 score and parsimony

## Key finding
Engagement volume alone did not predict churn — confirming the need for a 
multivariate ML approach over simple segmentation.

## Stack
Azure Databricks · Delta Lake · PySpark · Python · scikit-learn · MLflow
