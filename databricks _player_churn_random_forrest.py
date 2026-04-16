# Databricks notebook source
import mlflow
print(mlflow.__version__)

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(42)
n = 5000

df = pd.DataFrame({
    "player_id":        range(1, n+1),
    "sessions_30d":     np.random.poisson(8, n),
    "avg_session_mins": np.random.exponential(25, n).round(1),
    "total_deposits":   np.random.exponential(150, n).round(2),
    "days_since_last":  np.random.randint(1, 90, n),
    "game_types":       np.random.randint(1, 8, n),
    "churned":          (np.random.rand(n) > 0.72).astype(int)
})

print(df.shape)
df.head()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").saveAsTable("player_churn")
print("Saved to Delta table ✓")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   churned,
# MAGIC   COUNT(*) AS players,
# MAGIC   ROUND(AVG(sessions_30d), 1)       AS avg_sessions,
# MAGIC   ROUND(AVG(days_since_last), 1)    AS avg_days_inactive,
# MAGIC   ROUND(AVG(total_deposits), 2)     AS avg_deposits
# MAGIC FROM player_churn
# MAGIC GROUP BY churned
# MAGIC ORDER BY churned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN sessions_30d = 0           THEN 'Inactive'
# MAGIC     WHEN sessions_30d BETWEEN 1 AND 4  THEN 'Low'
# MAGIC     WHEN sessions_30d BETWEEN 5 AND 12 THEN 'Medium'
# MAGIC     ELSE 'High'
# MAGIC   END AS engagement_segment,
# MAGIC   COUNT(*) AS players,
# MAGIC   ROUND(AVG(churned) * 100, 1) AS churn_rate_pct
# MAGIC FROM player_churn
# MAGIC GROUP BY 1
# MAGIC ORDER BY churn_rate_pct DESC

# COMMAND ----------

from sklearn.model_selection import train_test_split

df_model = spark.table("player_churn").toPandas()

features = ["sessions_30d", "avg_session_mins", 
            "total_deposits", "days_since_last", "game_types"]

X = df_model[features]
y = df_model["churned"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"Train: {len(X_train)} | Test: {len(X_test)}")

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, accuracy_score
import mlflow
import mlflow.sklearn

mlflow.set_experiment("/player-churn-prediction")

with mlflow.start_run(run_name="random-forest-v1"):
    params = {"n_estimators": 100, "max_depth": 6, "random_state": 42}
    mlflow.log_params(params)

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mlflow.log_metric("f1_score",  round(f1_score(y_test, preds), 4))
    mlflow.log_metric("accuracy",  round(accuracy_score(y_test, preds), 4))

    mlflow.sklearn.log_model(model, "rf-churn-model")
    print("Run 1 logged ✓")

# COMMAND ----------

with mlflow.start_run(run_name="random-forest-v2"):
    params = {"n_estimators": 200, "max_depth": 10, "random_state": 42}
    mlflow.log_params(params)

    model2 = RandomForestClassifier(**params)
    model2.fit(X_train, y_train)

    preds2 = model2.predict(X_test)
    mlflow.log_metric("f1_score",  round(f1_score(y_test, preds2), 4))
    mlflow.log_metric("accuracy",  round(accuracy_score(y_test, preds2), 4))

    mlflow.sklearn.log_model(model2, "rf-churn-model-v2")
    print("Run 2 logged ✓")