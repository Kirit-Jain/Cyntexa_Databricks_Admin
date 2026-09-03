# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC select * from dev.gold.kpi_total_transaction_value

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.silver.silver_accounts_quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.silver.silver_branches_quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.kpi_account_outflow_inflow

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.kpi_total_transaction_count

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.kpi_total_transaction_value

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.gold.kpi_transaction_by_month

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 