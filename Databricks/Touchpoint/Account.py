# Databricks notebook source
df=spark.read.format("delta").load("abfss://raw@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/touchpoint/account/2023-10-25/10")
df.createOrReplaceTempView("df_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_v