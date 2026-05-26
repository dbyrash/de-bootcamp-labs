# Databricks notebook source
from pyspark.sql.functions import col
from delta import DeltaTable

table_path = "/Volumes/tabular/dataexpert/delta_demo/web_events"
print(spark.read.format("delta").load(table_path).count())

df = spark.read.format("delta").load(table_path)
df.show()

#Let's do some simple queries:




# COMMAND ----------

from pyspark.sql.functions import col, lit
from delta import DeltaTable

table_path = "/Volumes/tabular/dataexpert/delta_demo/web_events_time_travel_rashi" 

columns = ["event_id", "event_type","url_path","country","region","user_id", "browser", "device"] 

data_v0 = [
    ("E001", "page_view", "/lessons/python-into", "US", "NA", 1001, "Chrome", "desktop"),
    ("E002", "page_view", "/lessons/python-into", "US", "NA", 1002, "Chrome", "desktop"),
    ("E003", "download", "/resources/cheatsheet", "US", "NA", 1003, "Safari", "Mobile"),
    ("E004", "page_view", "/quiz/sql-quiz-2", "IN", "APAC", 1001, "Chrome", "Desktop"),
    ("E005", "video_start", "/lessons/spark-basics", "CA", "NA", 1004, "Edge", "Desktop"),
    ("E006", "download", "/resources/slides", "DE", "EU", 1002, "Chrome", "Desktop"),
    ("E007", "page_view", "/quiz/delta-quiz-1", "US", "NA", 1005, "Firefox", "Mobile"),
    ("E008", "page_view", "/lessons/kafka-intro", "AU", "APAC", 1003, "Safari", "Desktop"),

]
df_v0 = spark.createDataFrame(data_v0, columns) 
df_v0.write.format("delta").mode("overwrite").save(table_path)

print("version 0 is written")
df_v0.show()


# COMMAND ----------

# version 1: reclassify page_view events on /quiz path -> quiz_start 

delta_table = DeltaTable.forPath(spark, table_path) 

delta_table.update(
    condition="url_path LIKE '%quiz%' AND event_type = 'page_view'", 
    set={"event_type": lit("quiz_start")}
)

print("version 1 written - quiz page_views reclassified") 
spark.read.format("delta").load(table_path).display()


# COMMAND ----------

