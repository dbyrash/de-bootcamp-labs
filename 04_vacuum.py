"""
Delta Lake VACUUM
=================

VACUUM removes old data files that are no longer referenced by the Delta table.
This is essential for storage management but affects time travel capabilities.

Key Concepts:
- Delta Lake keeps old files for time travel (default: 7 days)
- VACUUM deletes files older than retention period
- After VACUUM, you CANNOT time travel to versions that used deleted files
- VACUUM is a storage optimization, not a performance optimization

CRITICAL: Always ensure retention period meets your compliance/audit requirements
before running VACUUM!

Data Source: Web events from an online learning platform
"""

from pyspark.sql.functions import col, lit, current_timestamp, expr
from delta import DeltaTable

from web_events_generator import generate_initial_web_events, get_web_events_columns
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local, is_databricks

def count_parquet_files(spark, table_path):
    """Count parquet files in the table directory."""
    if is_databricks():
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        return detail['numFiles'], detail['sizeInBytes'] / (1024 * 1024)
    else:
        import os
        count = 0
        total_size = 0
        for root, dirs, files in os.walk(table_path):
            if '_delta_log' in root:
                continue
            for f in files:
                if f.endswith('.parquet'):
                    count += 1
                    total_size += os.path.getsize(os.path.join(root, f))
        return count, total_size / (1024 * 1024)

def main():
    spark = get_spark()
    
    table_path = get_base_path("web_events_vacuum")
    cleanup_path(spark, table_path)
    print("=" * 70)
    print("VACUUM DEMONSTRATION")
    print("=" * 70)
    
    # =========================================================================
    # CREATE INITIAL TABLE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 1: Creating initial table with multiple versions")
    print("-" * 70)
    
    # Version 0: Initial web events
    data_v0 = [
        ("E001", "page_view", "/courses/python-101", "US", "NA", 1001, "Chrome", "Desktop"),
        ("E002", "click", "/lessons/intro-to-sql", "UK", "EU", 1002, "Firefox", "Desktop"),
        ("E003", "video_play", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),
        ("E004", "quiz_start", "/quiz/python-quiz-1", "DE", "EU", 1001, "Chrome", "Desktop"),
        ("E005", "page_view", "/dashboard", "US", "NA", 1004, "Edge", "Tablet"),
    ]
    columns = get_web_events_columns()
    
    df = spark.createDataFrame(data_v0, columns)
    df.write.format("delta").mode("overwrite").save(table_path)
    print("✓ Version 0: Initial 5 web events created")
    
    # Version 1: Update event types
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.update(
        condition=col("url_path").contains("/quiz/"),
        set={"event_type": lit("quiz_view")}
    )
    print("✓ Version 1: Reclassified quiz page events")
    
    # Version 2: Add more events
    new_events = [
        ("E006", "lesson_complete", "/lessons/delta-lake", "FR", "EU", 1002, "Firefox", "Desktop"),
        ("E007", "course_enroll", "/courses/data-engineering", "AU", "APAC", 1005, "Chrome", "Mobile"),
    ]
    df_new = spark.createDataFrame(new_events, columns)
    df_new.write.format("delta").mode("append").save(table_path)
    print("✓ Version 2: Added 2 new events")
    
    # Version 3: Delete an event
    delta_table.delete(condition=col("event_id") == "E003")
    print("✓ Version 3: Deleted event E003")
    
    # Version 4: Update region
    delta_table.update(
        condition=col("country") == "UK",
        set={"country": lit("GB")}
    )
    print("✓ Version 4: Normalized UK to GB")
    
    # =========================================================================
    # EXAMINE FILES BEFORE VACUUM
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 2: Examining table files BEFORE VACUUM")
    print("-" * 70)
    
    file_count, total_size = count_parquet_files(spark, table_path)
    
    print(f"""
    File Statistics:
    ├── Parquet files: {file_count}
    └── Total size: {total_size:.4f} MB
    
    Why are there {file_count} files for just 6 records?
    
    Delta Lake preserves old data files for:
    ✓ Time travel queries
    ✓ ACID transaction rollback
    ✓ Concurrent read consistency
    
    The current table only needs 1-2 files, but we have {file_count}
    because each UPDATE/DELETE creates new files while preserving old ones.
    """)
    
    print("\nTable History:")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)
    
    # =========================================================================
    # DEMONSTRATE TIME TRAVEL BEFORE VACUUM
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 3: Time Travel BEFORE VACUUM (all versions accessible)")
    print("-" * 70)
    
    print("\nVersion 0 (original 5 events):")
    spark.read.format("delta").option("versionAsOf", 0).load(table_path).show()
    
    print("\nVersion 3 (after delete):")
    spark.read.format("delta").option("versionAsOf", 3).load(table_path).show()
    
    print("\nCurrent version (Version 4):")
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # DRY RUN VACUUM
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 4: VACUUM Dry Run")
    print("-" * 70)
    
    print("""
    VACUUM has a safety feature: default retention of 7 days.
    Files younger than 7 days won't be deleted by default.
    
    For this demo, we'll use a 0-hour retention (DANGEROUS in production!).
    
    First, let's do a DRY RUN to see what would be deleted:
    """)
    
    print("\nDry run VACUUM with 0 hours retention:")
    print("(In production, NEVER use 0 hours - use at least 168 hours/7 days)")
    
    dry_run_result = spark.sql(f"""
        VACUUM delta.`{table_path}` RETAIN 0 HOURS DRY RUN
    """)
    
    print("\nFiles that WOULD be deleted:")
    dry_run_result.show(truncate=False)
    files_to_delete = dry_run_result.count()
    print(f"\nTotal files to be deleted: {files_to_delete}")
    
    # =========================================================================
    # RUN ACTUAL VACUUM
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 5: Running VACUUM")
    print("-" * 70)
    
    print("""
    ⚠️  WARNING: Running VACUUM will permanently delete old files!
    
    After VACUUM:
    - Storage is reclaimed
    - Old versions become inaccessible
    - Time travel to deleted versions will FAIL
    """)
    
    print("\nExecuting VACUUM RETAIN 0 HOURS...")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")
    
    file_count_after, total_size_after = count_parquet_files(spark, table_path)
    
    print(f"""
    ✓ VACUUM completed!
    
    File Statistics AFTER VACUUM:
    ├── Parquet files: {file_count_after} (was {file_count})
    ├── Total size: {total_size_after:.4f} MB (was {total_size:.4f} MB)
    └── Files removed: {file_count - file_count_after}
    """)
    
    # =========================================================================
    # DEMONSTRATE TIME TRAVEL FAILURE AFTER VACUUM
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 6: Time Travel AFTER VACUUM (old versions inaccessible)")
    print("-" * 70)
    
    print("\nCurrent version still works:")
    spark.read.format("delta").load(table_path).show()
    
    print("\nAttempting to read Version 0 (should fail):")
    try:
        spark.read.format("delta").option("versionAsOf", 0).load(table_path).show()
    except Exception as e:
        print(f"""
    ✗ ERROR: Cannot time travel to Version 0!
    
    Error message (truncated):
    {str(e)[:200]}...
    
    This is expected! VACUUM removed the files needed for Version 0.
    """)
    
    # =========================================================================
    # VACUUM BEST PRACTICES
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 7: VACUUM Best Practices")
    print("-" * 70)
    
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                      VACUUM BEST PRACTICES                           ║
    ╠══════════════════════════════════════════════════════════════════════╣
    ║                                                                      ║
    ║  1. RETENTION PERIOD                                                 ║
    ║     • Default: 7 days (168 hours) - DO NOT reduce in production!     ║
    ║     • Recommendation: 7-30 days based on compliance needs            ║
    ║     • Never use 0 hours except for testing                           ║
    ║                                                                      ║
    ║  2. SCHEDULING                                                       ║
    ║     • Run during off-peak hours                                      ║
    ║     • Weekly for most tables                                         ║
    ║     • Daily for high-churn tables (like web events)                  ║
    ║                                                                      ║
    ║  3. BEFORE RUNNING VACUUM                                            ║
    ║     • Ensure no long-running queries are active                      ║
    ║     • Consider audit/compliance requirements for web analytics       ║
    ║     • Always do a DRY RUN first                                      ║
    ║                                                                      ║
    ║  4. CONCURRENT OPERATIONS                                            ║
    ║     • VACUUM can run while reads are happening                       ║
    ║     • VACUUM may conflict with concurrent writes                     ║
    ║     • Use appropriate isolation level                                ║
    ║                                                                      ║
    ║  5. STORAGE CONSIDERATIONS                                           ║
    ║     • Monitor storage growth over time                               ║
    ║     • Balance storage cost vs. time travel needs                     ║
    ║     • Consider cloud lifecycle policies as complement                ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    # =========================================================================
    # RETENTION CONFIGURATION
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 8: Configuring Retention Settings")
    print("-" * 70)
    
    print("""
    You can configure retention at table level:
    
    -- Set minimum retention period for a table
    ALTER TABLE delta.`/path/to/table` 
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');
    
    -- Set log retention (affects history, not data files)
    ALTER TABLE delta.`/path/to/table`
    SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 60 days');
    """)
    
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (
            'delta.deletedFileRetentionDuration' = 'interval 7 days',
            'delta.logRetentionDuration' = 'interval 30 days'
        )
    """)
    
    print("\n✓ Configured retention settings on table")
    print("\nTable properties:")
    spark.sql(f"SHOW TBLPROPERTIES delta.`{table_path}`").show(truncate=False)
    
    # =========================================================================
    # VACUUM + OPTIMIZE WORKFLOW
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 9: Recommended Maintenance Workflow")
    print("-" * 70)
    
    print("""
    Typical maintenance workflow for Delta tables:
    
    ┌─────────────────────────────────────────────────────────────────┐
    │                     MAINTENANCE WORKFLOW                        │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  1. ANALYZE TABLE (optional)                                    │
    │     → Compute statistics for query optimization                 │
    │     ANALYZE TABLE delta.`/path` COMPUTE STATISTICS              │
    │                                                                 │
    │  2. OPTIMIZE                                                    │
    │     → Compact small files                                       │
    │     → Creates new compacted files                               │
    │     OPTIMIZE delta.`/path`                                      │
    │                                                                 │
    │  3. VACUUM (after OPTIMIZE)                                     │
    │     → Remove old, unreferenced files                            │
    │     → Includes pre-compaction files                             │
    │     VACUUM delta.`/path` RETAIN 168 HOURS                       │
    │                                                                 │
    │  Schedule: Daily OPTIMIZE, Weekly VACUUM                        │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
    """)
    
    stop_spark_if_local(spark)
    print("\n✓ VACUUM demonstration completed!")

if __name__ == "__main__":
    main()
