# Online Player Analytics — Data Science Portfolio

End-to-end data science project covering the full analytics stack 
for an online gaming platform.

## Projects in this repo

### 1. Player Churn Prediction — Azure Databricks & MLflow
- Generated a 5,000 player dataset saved as a **Delta table**
- Explored churn patterns using **SQL** in Databricks notebooks
- Trained and compared two **Random Forest** models using **MLflow**
- Selected optimal model based on F1 score and parsimony principle

**Key finding:** Engagement volume alone did not predict churn — 
confirming the need for a multivariate ML approach.

### 2. GA Funnel Analysis — BigQuery & Google Analytics
- Queried real GA data from Google Merchandise Store
- Analysed conversion rates by traffic source
- Built page-level funnel using UNNEST on nested GA event data

**Key finding:** YouTube drove 162K sessions but near-zero conversion — 
direct traffic converted at 2.45x higher rate.

### 3. Player Analytics Dashboard — Power BI
- Built interactive dashboard with DAX measures
- KPI cards: Total Players, Churn Rat
