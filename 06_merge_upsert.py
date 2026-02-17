"""
Delta Lake MERGE (Upsert) Operations
====================================

MERGE is one of the most powerful features of Delta Lake, enabling:
- Upserts (Update + Insert)
- Slowly Changing Dimensions (SCD)
- Change Data Capture (CDC)
- Deduplication

The MERGE command atomically matches source data against target table
and executes UPDATE, INSERT, or DELETE based on match conditions.

Data Source: Web events and user activity from an online learning platform
"""

from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta import DeltaTable

from web_events_generator import generate_initial_web_events, get_web_events_columns
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local

def main():
    spark = get_spark()
    
    base_path = get_base_path()
    
    print("=" * 70)
    print("MERGE (UPSERT) OPERATIONS DEMONSTRATION")
    print("=" * 70)
    
    # =========================================================================
    # BASIC UPSERT
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 1: Basic Upsert (Update existing, Insert new)")
    print("-" * 70)
    
    table_path = get_base_path("web_events_merge")
    cleanup_path(spark, table_path)
    
    # Create target table with web events
    target_data = [
        ("E001", "page_view", "/courses/python-101", "US", "NA", 1001, "Chrome", "Desktop"),
        ("E002", "click", "/lessons/intro-to-sql", "UK", "EU", 1002, "Firefox", "Desktop"),
        ("E003", "video_play", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),
        ("E004", "quiz_start", "/quiz/python-quiz-1", "DE", "EU", 1001, "Chrome", "Desktop"),
    ]
    columns = get_web_events_columns()
    
    df_target = spark.createDataFrame(target_data, columns)
    df_target.write.format("delta").mode("overwrite").save(table_path)
    
    print("\nTarget table (initial web events):")
    spark.read.format("delta").load(table_path).orderBy("event_id").show()
    
    # Source data with updates and new records
    source_data = [
        ("E001", "page_view", "/courses/python-101", "US", "NA", 1001, "Chrome", "Mobile"),  # Update: device changed
        ("E003", "video_complete", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),  # Update: event type changed
        ("E005", "lesson_complete", "/lessons/delta-lake", "FR", "EU", 1002, "Firefox", "Desktop"),  # New event
        ("E006", "course_enroll", "/courses/data-engineering", "AU", "APAC", 1005, "Chrome", "Mobile"),  # New event
    ]
    
    df_source = spark.createDataFrame(source_data, columns)
    
    print("\nSource data (incoming changes):")
    df_source.show()
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\nExecuting MERGE...")
    delta_table.alias("target").merge(
        df_source.alias("source"),
        "target.event_id = source.event_id"
    ).whenMatchedUpdate(set={
        "event_type": col("source.event_type"),
        "url_path": col("source.url_path"),
        "device_type": col("source.device_type")
    }).whenNotMatchedInsert(values={
        "event_id": col("source.event_id"),
        "event_type": col("source.event_type"),
        "url_path": col("source.url_path"),
        "country": col("source.country"),
        "region": col("source.region"),
        "user_id": col("source.user_id"),
        "browser": col("source.browser"),
        "device_type": col("source.device_type")
    }).execute()
    
    print("\n✓ MERGE completed!")
    print("\nTarget table (after MERGE):")
    spark.read.format("delta").load(table_path).orderBy("event_id").show()
    
    # =========================================================================
    # CONDITIONAL UPSERT
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 2: Conditional Upsert (Update only if conditions met)")
    print("-" * 70)
    
    print("""
    Common use case: Only update if source data meets criteria
    - Update only if event_type is different
    - Update only if from a specific region
    - Update only if user_id is not null
    """)
    
    conditional_source = [
        ("E001", "click", "/courses/python-101", "US", "NA", 1001, "Chrome", "Mobile"),  # Type changed - should update
        ("E002", "click", "/lessons/intro-to-sql", "UK", "EU", 1002, "Firefox", "Desktop"),  # Type same - skip
        ("E007", "download", "/certificates", "JP", "APAC", 1003, "Safari", "Desktop"),  # New event
    ]
    
    df_conditional = spark.createDataFrame(conditional_source, columns)
    
    print("\nConditional source data:")
    df_conditional.show()
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\nExecuting conditional MERGE (update only if event_type changed)...")
    delta_table.alias("target").merge(
        df_conditional.alias("source"),
        "target.event_id = source.event_id"
    ).whenMatchedUpdate(
        condition="source.event_type != target.event_type",
        set={
            "event_type": col("source.event_type"),
            "url_path": col("source.url_path"),
            "device_type": col("source.device_type")
        }
    ).whenNotMatchedInsertAll().execute()
    
    print("\n✓ Conditional MERGE completed!")
    print("\nTarget table (E002 unchanged because event_type was same):")
    spark.read.format("delta").load(table_path).orderBy("event_id").show()
    
    # =========================================================================
    # DELETE ON MATCH
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 3: Merge with Delete (Remove bot/spam events)")
    print("-" * 70)
    
    print("""
    Use whenMatchedDelete() to remove events that match certain conditions.
    Common use case: Remove events flagged as bot traffic or spam.
    """)
    
    delete_source = [
        ("E004", "quiz_start", "/quiz/python-quiz-1", "DE", "EU", None, "Chrome", "Desktop"),  # user_id=None means bot
        ("E005", "lesson_complete", "/lessons/delta-lake", "FR", "EU", 1002, "Firefox", "Desktop"),  # Update
    ]
    
    df_delete = spark.createDataFrame(delete_source, columns)
    
    print("\nSource with delete signals (user_id=None means bot, delete it):")
    df_delete.show()
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\nExecuting MERGE with delete (remove where user_id is null)...")
    delta_table.alias("target").merge(
        df_delete.alias("source"),
        "target.event_id = source.event_id"
    ).whenMatchedDelete(
        condition="source.user_id IS NULL"
    ).whenMatchedUpdate(
        set={
            "event_type": col("source.event_type"),
            "url_path": col("source.url_path")
        }
    ).execute()
    
    print("\n✓ MERGE with delete completed!")
    print("\nTarget table (E004 deleted as bot traffic):")
    spark.read.format("delta").load(table_path).orderBy("event_id").show()
    
    # =========================================================================
    # SCD TYPE 2 (SLOWLY CHANGING DIMENSIONS)
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 4: SCD Type 2 (User Profile History)")
    print("-" * 70)
    
    print("""
    SCD Type 2 preserves history by:
    1. Marking existing record as inactive (end_date = today)
    2. Inserting new record as active (end_date = null)
    
    This maintains complete audit trail of user profile changes.
    """)
    
    scd_table_path = get_base_path("user_profiles_scd")
    cleanup_path(spark, scd_table_path)
    scd_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("preferred_device", StringType(), True),
        StructField("is_current", IntegerType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
    ])
    
    scd_data = [
        (1001, "alice@academy.example.com", "Alice Smith", "Desktop", 1, "2023-01-01", None),
        (1002, "bob@academy.example.com", "Bob Johnson", "Mobile", 1, "2023-01-01", None),
        (1003, "carol@academy.example.com", "Carol Williams", "Tablet", 1, "2023-06-01", None),
    ]
    
    df_scd = spark.createDataFrame(scd_data, scd_schema)
    df_scd.write.format("delta").mode("overwrite").save(scd_table_path)
    
    print("\nSCD table (initial - all user profiles current):")
    spark.read.format("delta").load(scd_table_path).orderBy("user_id").show()
    
    updates_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("preferred_device", StringType(), True),
    ])
    
    updates = [
        (1001, "alice.new@academy.example.com", "Alice Smith", "Desktop"),  # Email changed
        (1002, "bob@academy.example.com", "Bob Johnson", "Desktop"),  # Device changed
        (1004, "david@academy.example.com", "David Brown", "Mobile"),  # New user
    ]
    
    df_updates = spark.createDataFrame(updates, updates_schema)
    
    print("\nIncoming profile updates:")
    df_updates.show()
    
    delta_table = DeltaTable.forPath(spark, scd_table_path)
    
    staged_updates = df_updates.alias("updates").join(
        spark.read.format("delta").load(scd_table_path).filter("is_current = 1").alias("target"),
        "user_id"
    ).where(
        "updates.email != target.email OR updates.preferred_device != target.preferred_device"
    ).select(
        col("updates.user_id"),
        col("updates.email"),
        col("updates.name"),
        col("updates.preferred_device")
    )
    
    print("\nRecords that have changes (will create new versions):")
    staged_updates.show()
    
    print("\nExecuting SCD Type 2 MERGE...")
    
    delta_table.alias("target").merge(
        staged_updates.alias("staged"),
        "target.user_id = staged.user_id AND target.is_current = 1"
    ).whenMatchedUpdate(set={
        "is_current": lit(0),
        "end_date": lit("2024-01-15")
    }).execute()
    
    new_records = staged_updates.withColumn("is_current", lit(1)) \
        .withColumn("start_date", lit("2024-01-15")) \
        .withColumn("end_date", lit(None).cast("string"))
    
    new_records.write.format("delta").mode("append").save(scd_table_path)
    
    new_users = df_updates.alias("u").join(
        spark.read.format("delta").load(scd_table_path).alias("t"),
        col("u.user_id") == col("t.user_id"),
        "left_anti"
    ).withColumn("is_current", lit(1)) \
     .withColumn("start_date", lit("2024-01-15")) \
     .withColumn("end_date", lit(None).cast("string"))
    
    if new_users.count() > 0:
        new_users.write.format("delta").mode("append").save(scd_table_path)
    
    print("\n✓ SCD Type 2 MERGE completed!")
    print("\nSCD table with history (note users 1001, 1002 have 2 records each):")
    spark.read.format("delta").load(scd_table_path) \
        .orderBy("user_id", "start_date").show()
    
    print("\nCurrent profiles only:")
    spark.read.format("delta").load(scd_table_path) \
        .filter("is_current = 1").orderBy("user_id").show()
    
    # =========================================================================
    # DEDUPLICATION WITH MERGE
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 5: Deduplication with MERGE")
    print("-" * 70)
    
    print("""
    Use MERGE to deduplicate incoming web events before inserting.
    Only insert events that don't already exist in the table.
    """)
    
    dedup_table_path = get_base_path("web_events_dedup")
    cleanup_path(spark, dedup_table_path)
    events = [
        ("E001", "page_view", "/courses", "US", "NA", 1001, "Chrome", "Desktop"),
        ("E002", "click", "/lessons", "UK", "EU", 1002, "Firefox", "Desktop"),
        ("E003", "video_play", "/lessons/spark", "CA", "NA", 1001, "Safari", "Mobile"),
    ]
    
    df_events = spark.createDataFrame(events, columns)
    df_events.write.format("delta").mode("overwrite").save(dedup_table_path)
    
    print("\nExisting web events:")
    spark.read.format("delta").load(dedup_table_path).show()
    
    new_events = [
        ("E002", "click", "/lessons", "UK", "EU", 1002, "Firefox", "Desktop"),  # Duplicate!
        ("E003", "video_play", "/lessons/spark", "CA", "NA", 1001, "Safari", "Mobile"),  # Duplicate!
        ("E004", "quiz_start", "/quiz", "DE", "EU", 1003, "Chrome", "Desktop"),  # New
        ("E005", "download", "/certificates", "JP", "APAC", 1001, "Safari", "Desktop"),  # New
    ]
    
    df_new_events = spark.createDataFrame(new_events, columns)
    
    print("\nNew batch (contains duplicates E002, E003):")
    df_new_events.show()
    
    delta_table = DeltaTable.forPath(spark, dedup_table_path)
    
    print("\nExecuting MERGE for deduplication (insert only new)...")
    delta_table.alias("target").merge(
        df_new_events.alias("source"),
        "target.event_id = source.event_id"
    ).whenNotMatchedInsertAll().execute()
    
    print("\n✓ Deduplication MERGE completed!")
    print("\nEvents table (only E004, E005 were inserted):")
    spark.read.format("delta").load(dedup_table_path).orderBy("event_id").show()
    
    # =========================================================================
    # MERGE SQL SYNTAX
    # =========================================================================
    print("\n" + "-" * 70)
    print("EXAMPLE 6: MERGE using SQL Syntax")
    print("-" * 70)
    
    print("""
    You can also use SQL syntax for MERGE operations:
    """)
    
    spark.read.format("delta").load(table_path).createOrReplaceTempView("web_events")
    
    sql_source = [
        ("E001", "scroll", "/courses/python-101", "US", "NA", 1001, "Chrome", "Mobile"),
        ("E008", "search", "/courses", "BR", "LATAM", 1006, "Edge", "Desktop"),
    ]
    df_sql_source = spark.createDataFrame(sql_source, columns)
    df_sql_source.createOrReplaceTempView("updates")
    
    print("\nSource for SQL MERGE:")
    spark.sql("SELECT * FROM updates").show()
    
    print("\nExecuting SQL MERGE statement...")
    spark.sql(f"""
        MERGE INTO delta.`{table_path}` AS target
        USING updates AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
            UPDATE SET
                target.event_type = source.event_type,
                target.url_path = source.url_path,
                target.device_type = source.device_type
        WHEN NOT MATCHED THEN
            INSERT (event_id, event_type, url_path, country, region, user_id, browser, device_type)
            VALUES (source.event_id, source.event_type, source.url_path, source.country, source.region, source.user_id, source.browser, source.device_type)
    """)
    
    print("\n✓ SQL MERGE completed!")
    print("\nWeb events table after SQL MERGE:")
    spark.read.format("delta").load(table_path).orderBy("event_id").show()
    
    # =========================================================================
    # MERGE PERFORMANCE TIPS
    # =========================================================================
    print("\n" + "-" * 70)
    print("MERGE Performance Best Practices")
    print("-" * 70)
    
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                    MERGE PERFORMANCE TIPS                            ║
    ╠══════════════════════════════════════════════════════════════════════╣
    ║                                                                      ║
    ║  1. MATCH CONDITION OPTIMIZATION                                     ║
    ║     • Use partition columns in match condition when possible         ║
    ║     • Match on indexed/Z-ORDERed columns for faster lookups          ║
    ║     • For web events: match on event_id or (user_id, event_time)     ║
    ║                                                                      ║
    ║  2. SOURCE DATA PREPARATION                                          ║
    ║     • Deduplicate source data BEFORE merge if possible               ║
    ║     • Filter source to only relevant records                         ║
    ║     • Cache source DataFrame if used multiple times                  ║
    ║                                                                      ║
    ║  3. PARTITIONING STRATEGY                                            ║
    ║     • Partition target table on date for time-based web events       ║
    ║     • Use dynamic partition pruning: match on partition columns      ║
    ║                                                                      ║
    ║  4. BATCH SIZE                                                       ║
    ║     • For large merges, consider micro-batching                      ║
    ║     • Monitor Spark UI for shuffle/spill issues                      ║
    ║                                                                      ║
    ║  5. AFTER MERGE                                                      ║
    ║     • Run OPTIMIZE to consolidate files created by MERGE             ║
    ║     • Consider ZORDER on region, device_type for analytics           ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    print("\nMerge operations in history:")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    ).filter(col("operation") == "MERGE").show(truncate=False)
    
    stop_spark_if_local(spark)
    print("\n✓ MERGE demonstration completed!")

if __name__ == "__main__":
    main()
