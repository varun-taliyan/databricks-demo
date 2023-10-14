# Databricks notebook source
dbutils.fs.mount(
    source = 'wasbs://inputdatasetcontainer@azurelearningstacc.blob.core.windows.net',
    mount_point = '/mnt/retaildb',
    extra_configs={'fs.azure.account.key.azurelearningstacc.blob.core.windows.net':'DA/nPrIu0vNqUbeImfp354cNuxtJEAGoqqQzMr7HcBdLDSxUBfS2ryiiUvlUsRvets27ENmpuZKm+AStZynX/Q=='}
)

dbutils.fs.mounts()


# COMMAND ----------

df = spark.read.csv("/mnt/retaildb/orders.csv", header=True)
df.display()

# COMMAND ----------

display(df)
count = df.count()
print(count)

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("parquet").save("/mnt/retaildb/parquet/orders.parquet")

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("delta").save("/mnt/retaildb/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists retaildb;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table retaildb.ordersparquet using parquet location "/mnt/retaildb/parquet/orders.parquet/*";

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retaildb.ordersdelta using delta location "/mnt/retaildb/delta/orders.delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from retaildb.ordersparquet;
# MAGIC Select * from retaildb.ordersdelta;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retaildb.ordersdelta

# COMMAND ----------

df.write.mode("overwrite").partitionBy("order_status").format("delta").option("path", "/mnt/retaildb/deltalatest/orders.delta").saveAsTable("retaildb.ordersdeltalatest")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into retaildb.ordersdelta VALUES('111111111','2013-07-25 00:00:00.0','222222222','CLOSED')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retaildb.ordersdelta

# COMMAND ----------

dfnew = spark.read.csv("/mnt/retaildb/ordersappend.csv", header=True)
display(dfnew)

# COMMAND ----------

dfnew.write.mode("append").format("delta").partitionBy("order_status").save("/mnt/retaildb/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into retaildb.ordersdelta from "/mnt/retaildb/ordersappend.csv" fileformat=CSV format_options('header'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into retaildb.ordersdelta from "/mnt/retaildb/ordersnew.csv" fileformat=CSV format_options('header'='true')

# COMMAND ----------

newdf = spark.read.csv("/mnt/retaildb/ordersnew.csv", header = True)

# COMMAND ----------

display(newdf)

# COMMAND ----------

newdf.write.mode("append").partitionBy("order_status").format("delta").option("mergeSchema", True).save("/mnt/retaildb/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retaildb.ordersdelta where order_id = 400000

# COMMAND ----------

# MAGIC %sql
# MAGIC update retaildb.ordersdelta set order_status='COMPLETE' where order_id=400000

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retaildb.ordersdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retaildb.ordersdelta version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retaildb.ordersdelta timestamp as of '2023-10-14T12:15:51.000+0000'

# COMMAND ----------


