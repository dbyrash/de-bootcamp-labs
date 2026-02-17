"""
Delta Lake Compaction (OPTIMIZE)
================================

Compaction is crucial for maintaining query performance in Delta tables.
Over time, as data is inserted, updated, and deleted, tables accumulate
many small files (the "small file problem").

The OPTIMIZE command:
- Combines small files into larger ones
- Improves query performance by reducing file I/O
- Can optionally co-locate data using Z-Ordering

When to run OPTIMIZE:
- After many small batch writes
- Before running expensive queries
- As part of regular maintenance (daily/hourly)

Best Practices:
- Target file size: 1GB (default) for most workloads
- Run during off-peak hours for large tables
- Use Z-ORDER for commonly filtered columns

Data Source: Web events from an online learning platform
"""

from pyspark.sql.functions import col, lit, rand, expr, floor
from delta import DeltaTable

from web_events_generator import generate_web_events
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local, is_databricks

def get_table_file_stats(spark, table_path):
    """Get statistics about the files in a Delta table."""
    if is_databricks():
        # In Databricks, use SQL to get file stats
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        return {
            'num_files': detail['numFiles'],
            'total_size_mb': detail['sizeInBytes'] / (1024 * 1024),
            'avg_file_size_mb': (detail['sizeInBytes'] / detail['numFiles']) / (1024 * 1024) if detail['numFiles'] > 0 else 0,
            'files': []
        }
    else:
        import os
        delta_table = DeltaTable.forPath(spark, table_path)
        
        parquet_files = []
        for root, dirs, files in os.walk(table_path):
            for f in files:
                if f.endswith('.parquet'):
                    file_path = os.path.join(root, f)
                    parquet_files.append({
                        'name': f,
                        'size_mb': os.path.getsize(file_path) / (1024 * 1024)
                    })
        
        return {
            'num_files': len(parquet_files),
            'total_size_mb': sum(f['size_mb'] for f in parquet_files),
            'avg_file_size_mb': sum(f['size_mb'] for f in parquet_files) / len(parquet_files) if parquet_files else 0,
            'files': parquet_files
        }

def main():
    spark = get_spark()
    
    table_path = get_base_path("web_events_compaction")
    cleanup_path(spark, table_path)
    
    print("=" * 70)
    print("COMPACTION (OPTIMIZE) DEMONSTRATION")
    print("=" * 70)
    
    # =========================================================================
    # CREATE TABLE WITH MANY SMALL FILES
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 1: Creating a table with many small files")
    print("-" * 70)
    
    print("""
    Simulating the "small file problem" by writing many small batches.
    This commonly happens with:
    - Streaming ingestion of web events
    - Frequent small batch updates from real-time tracking
    - High-frequency append operations from multiple data sources
    """)
    
    num_batches = 20
    records_per_batch = 100
    
    print(f"\nWriting {num_batches} small batches of {records_per_batch} web events each...")
    
    for i in range(num_batches):
        df = generate_web_events(spark, num_events=records_per_batch, days_back=1)
        
        mode = "overwrite" if i == 0 else "append"
        df.write.format("delta").mode(mode).save(table_path)
        
        if (i + 1) % 5 == 0:
            print(f"  Written {i + 1}/{num_batches} batches...")
    
    print("\n✓ All batches written!")
    
    print("\n" + "-" * 70)
    print("STEP 2: Analyzing table BEFORE optimization")
    print("-" * 70)
    
    stats_before = get_table_file_stats(spark, table_path)
    print(f"""
    File Statistics (BEFORE OPTIMIZE):
    ├── Number of files: {stats_before['num_files']}
    ├── Total size: {stats_before['total_size_mb']:.2f} MB
    └── Average file size: {stats_before['avg_file_size_mb']:.4f} MB
    
    Problem: {stats_before['num_files']} small files!
    - Each query must open all files
    - File overhead dominates actual data
    - Poor parallelization efficiency
    """)
    
    total_rows = spark.read.format("delta").load(table_path).count()
    print(f"    Total web events in table: {total_rows:,}")
    
    # =========================================================================
    # RUN BASIC OPTIMIZE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 3: Running OPTIMIZE (Basic Compaction)")
    print("-" * 70)
    
    print("""
    OPTIMIZE consolidates small files into larger, more efficient files.
    
    Syntax:
        OPTIMIZE delta.`/path/to/table`
        
    Or using Python API:
        delta_table.optimize().executeCompaction()
    """)
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\nRunning optimization...")
    start_time = time.time()
    
    optimization_result = delta_table.optimize().executeCompaction()
    
    elapsed = time.time() - start_time
    print(f"✓ Optimization completed in {elapsed:.2f} seconds")
    
    print("\nOptimization Metrics:")
    optimization_result.show(truncate=False)
    
    # =========================================================================
    # ANALYZE RESULTS
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 4: Analyzing table AFTER optimization")
    print("-" * 70)
    
    stats_after = get_table_file_stats(spark, table_path)
    
    print(f"""
    File Statistics (AFTER OPTIMIZE):
    ├── Number of files: {stats_after['num_files']}
    ├── Total size: {stats_after['total_size_mb']:.2f} MB
    └── Average file size: {stats_after['avg_file_size_mb']:.4f} MB
    
    Improvement:
    ├── Files reduced: {stats_before['num_files']} → {stats_after['num_files']} ({((stats_before['num_files'] - stats_after['num_files']) / stats_before['num_files'] * 100):.1f}% reduction)
    └── Avg file size increased: {stats_before['avg_file_size_mb']:.4f} MB → {stats_after['avg_file_size_mb']:.4f} MB
    """)
    
    total_rows_after = spark.read.format("delta").load(table_path).count()
    print(f"    Total web events after optimization: {total_rows_after:,}")
    print(f"    Data integrity: {'✓ PASSED' if total_rows == total_rows_after else '✗ FAILED'}")
    
    # =========================================================================
    # Z-ORDERING FOR BETTER DATA LAYOUT
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 5: Z-ORDER Optimization")
    print("-" * 70)
    
    print("""
    Z-Ordering co-locates related data in the same files.
    This dramatically improves query performance for filtered queries.
    
    How Z-Ordering helps for web events:
    ┌─────────────────────────────────────────────────────────────────┐
    │ WITHOUT Z-ORDER:                                                │
    │   Query: WHERE region = 'NA' AND device_type = 'Mobile'         │
    │   Result: Must scan ALL files (data randomly distributed)       │
    │                                                                 │
    │ WITH Z-ORDER on (region, device_type):                          │
    │   Query: WHERE region = 'NA' AND device_type = 'Mobile'         │
    │   Result: Only scan files containing NA+Mobile data             │
    │           (data skipping via file statistics)                   │
    └─────────────────────────────────────────────────────────────────┘
    
    Best columns for Z-ORDER in web events:
    - region, device_type: For geographic and device analytics
    - event_type: For filtering specific user actions
    - user_id: For user-centric queries
    """)
    
    print("\nRunning OPTIMIZE with Z-ORDER on (region, device_type)...")
    start_time = time.time()
    
    zorder_result = delta_table.optimize().executeZOrderBy("region", "device_type")
    
    elapsed = time.time() - start_time
    print(f"✓ Z-ORDER optimization completed in {elapsed:.2f} seconds")
    
    print("\nZ-ORDER Metrics:")
    zorder_result.show(truncate=False)
    
    # =========================================================================
    # QUERY PERFORMANCE COMPARISON
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 6: Query Performance (Conceptual)")
    print("-" * 70)
    
    print("""
    After Z-Ordering, queries that filter on region and device_type
    will benefit from data skipping.
    
    Example query performance improvement:
    
    Query: SELECT COUNT(*) FROM web_events WHERE region = 'NA' AND device_type = 'Mobile'
    
    Before Z-ORDER:
    - Files scanned: ALL files
    - Data read: ~100% of table
    
    After Z-ORDER:
    - Files scanned: Only files with NA+Mobile data
    - Data read: ~6-25% of table (depending on selectivity)
    
    Note: For this demo's small dataset, the improvement may not be visible,
    but for tables with millions/billions of web events, the improvement is dramatic.
    """)
    
    print("\nSample query with filter:")
    filtered_df = spark.read.format("delta").load(table_path) \
        .filter((col("region") == "NA") & (col("device_type") == "Mobile"))
    
    print(f"Web events matching region='NA' AND device_type='Mobile': {filtered_df.count()}")
    
    # =========================================================================
    # AUTO OPTIMIZE SETTINGS
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 7: Auto Optimization Settings")
    print("-" * 70)
    
    print("""
    Delta Lake supports automatic optimization to prevent small files:
    
    1. Auto Compaction (autoOptimize):
       - Automatically compacts files after writes
       - Set: delta.autoOptimize.autoCompact = true
    
    2. Optimized Writes:
       - Coalesces small partitions during writes
       - Set: delta.autoOptimize.optimizeWrite = true
    
    Enable via table properties:
    
    ALTER TABLE delta.`/path/to/table` SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    
    Or via SparkSession config:
    
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    """)
    
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    
    print("✓ Enabled optimizeWrite for the table")
    
    print("\nCurrent table properties:")
    spark.sql(f"SHOW TBLPROPERTIES delta.`{table_path}`").show(truncate=False)
    
    # =========================================================================
    # CHECK OPTIMIZATION HISTORY
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 8: Viewing Optimization History")
    print("-" * 70)
    
    print("\nTable history showing optimization operations:")
    delta_table.history().select(
        "version", "timestamp", "operation", "operationMetrics"
    ).filter(col("operation").isin("OPTIMIZE", "WRITE")).show(truncate=False)
    
    stop_spark_if_local(spark)
    print("\n✓ Compaction demonstration completed!")

if __name__ == "__main__":
    main()
