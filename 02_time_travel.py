"""
Delta Lake Time Travel
======================

Time Travel is one of Delta Lake's most powerful features. It allows you to:
- Query historical versions of your data
- Audit changes over time
- Recover from accidental deletes or updates
- Reproduce experiments with specific data versions

How it works:
- Every write operation creates a new version
- Versions are stored in the _delta_log directory
- Data files are preserved until VACUUM is run
- Default retention is 30 days

Data Source: Web events from an online learning platform
"""

from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from delta import DeltaTable

from web_events_generator import generate_initial_web_events, get_web_events_columns
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local

def main():
    spark = get_spark()
    
    table_path = get_base_path("web_events_time_travel")
    cleanup_path(spark, table_path)
    
    print("=" * 70)
    print("TIME TRAVEL DEMONSTRATION")
    print("=" * 70)
    
    # =========================================================================
    # VERSION 0: Initial Data Load
    # =========================================================================
    print("\n" + "-" * 70)
    print("VERSION 0: Initial Web Events Data Load")
    print("-" * 70)
    
    initial_data = generate_initial_web_events(spark)
    columns = get_web_events_columns()
    
    df_v0 = spark.createDataFrame(initial_data, columns)
    
    df_v0.write.format("delta").mode("overwrite").save(table_path)
    print("\n✓ Version 0 created - Initial web events loaded")
    df_v0.show()
    
    # =========================================================================
    # VERSION 1: Update - Fix misclassified events
    # =========================================================================
    print("\n" + "-" * 70)
    print("VERSION 1: Reclassify page_view events on /quiz paths as quiz_start")
    print("-" * 70)
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    delta_table.update(
        condition="url_path LIKE '%/quiz/%' AND event_type = 'page_view'",
        set={"event_type": lit("quiz_start")}
    )
    
    print("\n✓ Version 1 created - Events reclassified")
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # VERSION 2: Insert - New Events
    # =========================================================================
    print("\n" + "-" * 70)
    print("VERSION 2: Adding New Events")
    print("-" * 70)
    
    new_events = [
        ("E009", "video_complete", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),
        ("E010", "quiz_submit", "/quiz/python-quiz-1", "DE", "EU", 1001, "Chrome", "Desktop"),
    ]
    
    df_new = spark.createDataFrame(new_events, columns)
    df_new.write.format("delta").mode("append").save(table_path)
    
    print("\n✓ Version 2 created - 2 new events added")
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # VERSION 3: Delete - Remove bot traffic (APAC region test)
    # =========================================================================
    print("\n" + "-" * 70)
    print("VERSION 3: Removing suspected bot traffic (download events)")
    print("-" * 70)
    
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.delete(condition="event_type = 'download'")
    
    print("\n✓ Version 3 created - Download events removed (suspected bots)")
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # VERSION 4: Update - Normalize country codes
    # =========================================================================
    print("\n" + "-" * 70)
    print("VERSION 4: Normalize country codes (UK → GB)")
    print("-" * 70)
    
    delta_table.update(
        condition=col("country") == "UK",
        set={"country": lit("GB")}
    )
    
    print("\n✓ Version 4 created - Country codes normalized")
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # VIEW COMPLETE HISTORY
    # =========================================================================
    print("\n" + "=" * 70)
    print("COMPLETE TABLE HISTORY")
    print("=" * 70)
    
    delta_table = DeltaTable.forPath(spark, table_path)
    history = delta_table.history()
    
    print("\nAll versions with operations:")
    history.select(
        "version", 
        "timestamp", 
        "operation",
        "operationMetrics"
    ).orderBy("version").show(truncate=False)
    
    # =========================================================================
    # TIME TRAVEL QUERIES
    # =========================================================================
    print("\n" + "=" * 70)
    print("TIME TRAVEL QUERIES")
    print("=" * 70)
    
    print("\n--- Method 1: Query by Version Number ---")
    print("\nVersion 0 (Original data with 8 events):")
    df_version_0 = spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(table_path)
    df_version_0.show()
    
    print("\nVersion 2 (After adding new events - 10 total):")
    df_version_2 = spark.read.format("delta") \
        .option("versionAsOf", 2) \
        .load(table_path)
    df_version_2.show()
    
    print("\n--- Method 2: Query Using SQL Syntax ---")
    
    print("\nUsing VERSION AS OF in SQL:")
    spark.sql(f"""
        SELECT * FROM delta.`{table_path}` VERSION AS OF 1
    """).show()
    
    # =========================================================================
    # COMPARE VERSIONS
    # =========================================================================
    print("\n" + "=" * 70)
    print("COMPARING VERSIONS - Change Detection")
    print("=" * 70)
    
    print("\nComparing Version 0 vs Version 4 (current):")
    print("Finding events that were deleted:")
    
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
    v4 = spark.read.format("delta").load(table_path)
    
    deleted = v0.join(v4, on="event_id", how="left_anti")
    print("\nDeleted events:")
    deleted.show()
    
    added = v4.join(v0, on="event_id", how="left_anti")
    print("\nAdded events:")
    added.show()
    
    print("\nCountry code changes (UK → GB):")
    v0_countries = v0.select("event_id", col("country").alias("old_country"))
    v4_countries = v4.select("event_id", col("country").alias("new_country"))
    
    country_changes = v0_countries.join(v4_countries, on="event_id", how="inner") \
        .filter(col("old_country") != col("new_country"))
    
    country_changes.show()
    
    # =========================================================================
    # RESTORE TO PREVIOUS VERSION
    # =========================================================================
    print("\n" + "=" * 70)
    print("RESTORE TO PREVIOUS VERSION")
    print("=" * 70)
    
    print("\nCurrent data (Version 4):")
    spark.read.format("delta").load(table_path).show()
    
    print("\nRestoring to Version 2 (before deletions)...")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.restoreToVersion(2)
    
    print("\n✓ Table restored to Version 2!")
    print("\nData after restore:")
    spark.read.format("delta").load(table_path).show()
    
    print("\nNote: Restore creates a NEW version (Version 5) that matches Version 2's data")
    print("The history is preserved - nothing is lost!")
    
    delta_table.history().select(
        "version", "timestamp", "operation"
    ).orderBy("version").show()
    
    # =========================================================================
    # TIMESTAMP-BASED TIME TRAVEL
    # =========================================================================
    print("\n" + "=" * 70)
    print("TIMESTAMP-BASED TIME TRAVEL")
    print("=" * 70)
    
    print("""
    You can also query by timestamp:
    
    # Query as of a specific timestamp
    df = spark.read.format("delta") \\
        .option("timestampAsOf", "2024-01-15 10:30:00") \\
        .load(table_path)
    
    # In SQL
    SELECT * FROM delta.`/path/to/table` TIMESTAMP AS OF '2024-01-15 10:30:00'
    
    This is useful for:
    - Reproducing analytics reports from specific dates
    - Debugging issues at specific times
    - Audit compliance for web tracking data
    """)
    
    history_df = delta_table.history().select("version", "timestamp").orderBy("version")
    print("\nAvailable timestamps for this table:")
    history_df.show(truncate=False)
    
    stop_spark_if_local(spark)
    print("\n✓ Time Travel demonstration completed!")

if __name__ == "__main__":
    main()
