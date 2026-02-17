"""
Databricks Compatibility Utilities
===================================

This module provides utilities for running Delta Lake examples
in both local PySpark and Databricks environments.

Usage:
    from databricks_utils import get_spark, get_base_path, is_databricks
"""

import os

def is_databricks():
    """Check if running in a Databricks environment."""
    # Check for Databricks-specific environment variables
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ or
        "DB_HOME" in os.environ or
        os.path.exists("/databricks")
    )

def get_spark():
    """
    Get or create a SparkSession configured for Delta Lake.
    
    In Databricks: Returns the pre-configured spark session
    Locally: Creates a new SparkSession with Delta Lake support
    
    Returns:
        SparkSession configured for Delta Lake
    """
    if is_databricks():
        # In Databricks, spark is globally available
        # Import from the global namespace
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    else:
        # Local environment - create SparkSession with Delta
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        
        # Only set master for local mode
        builder = builder.master("local[*]")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()

def get_base_path(subdir=""):
    """
    Get base path for Delta tables.
    
    In Databricks: Uses /tmp/delta_lake_demo (DBFS)
    Locally: Uses ./delta_tables
    
    Args:
        subdir: Optional subdirectory name
        
    Returns:
        Path string for storing Delta tables
    """
    if is_databricks():
        base = "/tmp/delta_lake_demo"
    else:
        base = "./delta_tables"
        os.makedirs(base, exist_ok=True)
    
    if subdir:
        path = f"{base}/{subdir}"
        if not is_databricks():
            os.makedirs(path, exist_ok=True)
        return path
    return base

def cleanup_path(spark, path):
    """
    Remove a Delta table path if it exists.
    
    Works in both local and Databricks environments.
    
    Args:
        spark: SparkSession
        path: Path to remove
    """
    if is_databricks():
        # Use dbutils in Databricks
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            dbutils.fs.rm(path, recurse=True)
        except:
            # If dbutils not available, try SQL
            try:
                spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
            except:
                pass
    else:
        import shutil
        if os.path.exists(path):
            shutil.rmtree(path)

def stop_spark_if_local(spark):
    """
    Stop SparkSession only if running locally.
    In Databricks, we don't want to stop the shared session.
    
    Args:
        spark: SparkSession to potentially stop
    """
    if not is_databricks():
        spark.stop()
