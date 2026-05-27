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

# Version 2: Add 2 new events
new_events = [
    ("E009", "video_complete", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),
    ("E010", "quiz_submit", "/quiz/python-quiz-1", "DE", "EU", 1001, "Chrome", "Desktop"),
]

df_new = spark.createDataFrame(new_events, columns) 
df_new.write.format("delta").mode("append").save(table_path)

print("version 2 written - 2 new events added") 
spark.read.format("delta").load(table_path).display()

# COMMAND ----------

from delta import DeltaTable

table_path = "/Volumes/tabular/dataexpert/delta_demo/web_events_time_travel_rashi"

delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().select("version", "timestamp", "operation").orderBy("version").display()

delta_table = DeltaTable.forPath(spark, table_path)
delta_table.delete(condition="event_type = 'download'")

print("✅ Version 3 — download events deleted")
spark.read.format("delta").load(table_path).display()

# COMMAND ----------

# Version 4: Normalize country codes UK → GB
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.update(
    condition="country = 'UK'",
    set={"country": lit("GB")}
)
print("Version 4 — country codes UK → GB")
spark.read.format("delta").load(table_path).display()


# COMMAND ----------

delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().select("version", "timestamp", "operation").orderBy("version").display()

# COMMAND ----------

df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 8) \
    .load(table_path)

print("Version 8 row count:", df_v0.count())
df_v0.display()

# COMMAND ----------

# What changed between Version 8 and current?
v_old = spark.read.format("delta").option("versionAsOf", 8).load(table_path)
v_current = spark.read.format("delta").load(table_path)

# Rows that got deleted
deleted = v_old.join(v_current, on="event_id", how="left_anti")
print("Deleted rows:")
deleted.display()

# Rows that got added
added = v_current.join(v_old, on="event_id", how="left_anti")
print("Added rows:")
added.display()

# COMMAND ----------

from delta import DeltaTable

table_path = "/Volumes/tabular/dataexpert/delta_demo/web_events_time_travel_rashi"

# Run OPTIMIZE
spark.sql(f"OPTIMIZE delta.`{table_path}`")
print("OPTIMIZE complete")

# Check history — you'll see OPTIMIZE as a new version
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().select("version", "timestamp", "operation").orderBy("version").display()

# COMMAND ----------

# Z-ORDER by the columns you filter most often
spark.sql(f"""
    OPTIMIZE delta.`{table_path}`
    ZORDER BY (event_type, country)
""")

print("Z-ORDER complete")
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().select("version", "timestamp", "operation").orderBy("version").display()
delta_table.toDF().show()

# COMMAND ----------

spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS DRY RUN").display()
