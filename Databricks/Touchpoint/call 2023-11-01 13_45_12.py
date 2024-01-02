# Databricks notebook source
# MAGIC %run "/Shared/config_load"

# COMMAND ----------

from delta.tables import *
from datetime import datetime

#global properties from %run "/Shared/config_load"
#print(storage_account)

# get current date
current_date = datetime.now().date()
# formate date
formatted_date = current_date.strftime("%Y-%m-%d")
formatted_date = "2023-10-25"

incremental_path = "abfss://raw@"+storage_account+"/touchpoint/call/"+formatted_date+"/10"
#print(incremental_path)

delta_table_path = "abfss://transform@"+storage_account+"/Touchpoint/call"
#print(delta_table_path)

if DeltaTable.isDeltaTable(spark, delta_table_path):
    # delta loading...if deltatable exist then load, otherwise create one in transform layer
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.alias("oldData").merge(spark.read.format("delta").load(incremental_path).alias("newData"), "oldData.id = newData.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    df=spark.read.format("delta").load(incremental_path)
    df.write.format("delta").mode('append').option("header", "true").save(delta_table_path)


# COMMAND ----------

df = delta_table.toDF()
df.createOrReplaceTempView("df_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from df_v

# COMMAND ----------

from pyspark.sql.functions import length
# 获取字符串类型的列
string_columns = [col_name for col_name, col_type in df.dtypes if col_type == "string"]

# 计算每个字符串列的最大长度并标注超过255的列
max_lengths = {}
for col in string_columns:
    max_length = df.select(length(col)).agg({"length(" + col + ")": "max"}).collect()[0][0]
    if max_length is not None and max_length > 255:
       max_lengths[col] = max_length

# 输出最大长度
print("最大字符串长度:")
for col, length in max_lengths.items():
    print(f"{col}: {length}")

#print("\n建表语句:")
#for col_name, col_type in df.dtypes:
#    if col_name in max_lengths:
#        col_type += f"({max_lengths[col_name]})"
#    print(f"{col_name} {col_type}")

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
pwdstring = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="dwhchinaadmin-secret")
connection_string = dbutils.secrets.get(scope="ddpcn-key-vault-secrets", key="connection-dwhchina-secret")
connection_string = connection_string.replace("{password}",pwdstring)

df.write.jdbc(connection_string, "[ods_touchpoint].[call]", mode="append")

