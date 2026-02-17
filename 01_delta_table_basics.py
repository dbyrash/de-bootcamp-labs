"""
Delta Lake Basics - Introduction and Setup
==========================================

This script demonstrates the fundamentals of Delta Lake with PySpark.
Delta Lake is an open-source storage layer that brings ACID transactions
to Apache Spark and big data workloads.

Key Benefits:
- ACID Transactions: Ensures data integrity
- Scalable Metadata: Handles petabyte-scale tables
- Time Travel: Access historical versions of data
- Schema Enforcement: Prevents bad data from being written
- Unified Batch and Streaming: Same table for both workloads

Data Source: Web events from an online learning platform

Compatible with: Local PySpark, Databricks, and other Spark environments
"""

from pyspark.sql.functions import col
from delta import DeltaTable

from databricks_utils import get_spark, get_base_path, stop_spark_if_local
from web_events_generator import generate_web_events

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    base_path = get_base_path()
    
    print("=" * 60)
    print("Creating Web Events Data")
    print("=" * 60)
    
    print("""
    Generating realistic web events data from an online learning platform.
    Each event represents user interactions like page views, video plays,
    quiz submissions, and course enrollments.
    """)
    
    df = generate_web_events(spark, num_events=1000, days_back=7)
    
    print("\nGenerated Web Events DataFrame:")
    df.select("event_id", "event_time", "event_type", "url_path", "user_id", "device_type", "region").show(10, truncate=False)
    print(f"\nSchema:\n{df.schema.simpleString()}")
    
    table_path = f"{base_path}/web_events"
    print(f"\nWriting Delta table to: {table_path}")
    
    df.write.format("delta") \
        .mode("overwrite") \
        .save(table_path)
    
    print("\n✓ Delta table created successfully!")
    
    print("\n" + "=" * 60)
    print("Reading Delta Table")
    print("=" * 60)
    
    events_df = spark.read.format("delta").load(table_path)
    print(f"\nTotal records: {events_df.count()}")
    events_df.select("event_id", "event_time", "event_type", "url_path", "user_id").show(10, truncate=False)
    
    print("\n" + "=" * 60)
    print("Delta Transaction Log")
    print("=" * 60)
    print("""
    The _delta_log directory contains JSON files that track all
    changes to the table. Each commit creates a new JSON file
    with sequential numbering (00000000000000000000.json, etc.)
    
    This log enables:
    - ACID transactions
    - Time travel
    - Audit history
    """)
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\nTable History (first operation):")
    delta_table.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)
    
    print("\n" + "=" * 60)
    print("Web Events Analytics")
    print("=" * 60)
    
    print("\nEvents by Type:")
    events_df.groupBy("event_type").count() \
        .orderBy(col("count").desc()) \
        .show()
    
    print("\nEvents by Device Type:")
    events_df.groupBy("device_type").count() \
        .orderBy(col("count").desc()) \
        .show()
    
    print("\nEvents by Region:")
    events_df.groupBy("region").count() \
        .orderBy(col("count").desc()) \
        .show()
    
    print("\nTop 10 Most Visited Pages:")
    events_df.filter(col("event_type") == "page_view") \
        .groupBy("url_path").count() \
        .orderBy(col("count").desc()) \
        .limit(10) \
        .show(truncate=False)
    
    stop_spark_if_local(spark)
    print("\n✓ Script completed successfully!")

if __name__ == "__main__":
    main()
