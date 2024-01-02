# Databricks notebook source
from pyspark.sql.functions import current_timestamp

#df=spark.read.format("delta").load("abfss://raw@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/touchpoint/call/2023-10-25/10")
df=spark.read.format("delta").load("abfss://raw@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/touchpoint/call/2023-10-25/10")
#df = df.withColumn("batchno", current_timestamp().cast("long"))

df.createOrReplaceTempView("df_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_v

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
pwdstring = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="dwhchinaadmin-secret")
connection_string = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="connection-dwhchina-secret")
connection_string = connection_string.replace("{password}",pwdstring)
#for char in connection_string:
#    print(char, end="\u200B")

df.write.jdbc(connection_string, "[stg_touchpoint].[call]", mode="append")


# COMMAND ----------

df.write.format("delta").mode('append').option("header", "true").save("abfss://transform@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/Touchpoint/call")

df.write.format("csv").mode('append').option("header", "true").save("abfss://transform@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/Touchpoint/test/2023-10-25/10")


# COMMAND ----------

df1=spark.read.format("csv").load("abfss://transform@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/Touchpoint/test/*")


dbutils.fs.rm("abfss://transform@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/Touchpoint/test", True)

