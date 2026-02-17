"""
Delta Lake Schema Evolution
===========================

Schema evolution allows you to change your table schema over time without
breaking existing data or requiring rewrites.

Supported Schema Changes:
- Add new columns
- Change column types (with compatible conversions)
- Rename columns
- Reorder columns
- Make columns nullable

Delta Lake's schema enforcement prevents accidental schema changes while
schema evolution enables intentional changes.

Data Source: User activity data from an online learning platform
"""

from pyspark.sql.functions import col, lit, current_timestamp, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, MapType
from delta import DeltaTable

from web_events_generator import generate_user_activity_data
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local

def print_schema_comparison(schema_before, schema_after, title="Schema Comparison"):
    """Print a side-by-side schema comparison."""
    print(f"\n{title}")
    print("-" * 60)
    print(f"{'Before':<30} | {'After':<30}")
    print("-" * 60)
    
    before_fields = {f.name: f for f in schema_before.fields}
    after_fields = {f.name: f for f in schema_after.fields}
    
    all_fields = set(before_fields.keys()) | set(after_fields.keys())
    
    for field in sorted(all_fields):
        before = f"{field}: {before_fields[field].dataType.simpleString()}" if field in before_fields else "(not present)"
        after = f"{field}: {after_fields[field].dataType.simpleString()}" if field in after_fields else "(removed)"
        marker = "  " if before == after.replace("(not present)", "") else "* "
        print(f"{marker}{before:<28} | {after:<28}")

def main():
    spark = get_spark()
    
    table_path = get_base_path("user_activity_schema")
    cleanup_path(spark, table_path)
    
    print("=" * 70)
    print("SCHEMA EVOLUTION DEMONSTRATION")
    print("=" * 70)
    
    # =========================================================================
    # INITIAL SCHEMA
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 1: Create Table with Initial Schema")
    print("-" * 70)
    
    initial_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("total_events", IntegerType(), True),
    ])
    
    initial_data = [
        (1001, "alice@academy.example.com", "Alice Smith", 150),
        (1002, "bob@academy.example.com", "Bob Johnson", 230),
        (1003, "carol@academy.example.com", "Carol Williams", 89),
    ]
    
    df = spark.createDataFrame(initial_data, initial_schema)
    df.write.format("delta").mode("overwrite").save(table_path)
    
    print("\n✓ Initial table created")
    print("\nInitial Schema:")
    spark.read.format("delta").load(table_path).printSchema()
    spark.read.format("delta").load(table_path).show()
    
    original_schema = spark.read.format("delta").load(table_path).schema
    
    # =========================================================================
    # SCHEMA ENFORCEMENT (DEFAULT)
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 2: Schema Enforcement (Preventing Accidental Changes)")
    print("-" * 70)
    
    print("""
    By default, Delta Lake ENFORCES schema on write.
    Attempting to write data with a different schema will FAIL.
    """)
    
    new_data_extra_col = [
        (1004, "david@academy.example.com", "David Brown", 175, "US"),
    ]
    
    print("\nAttempting to write data with extra column 'country'...")
    try:
        df_extra = spark.createDataFrame(
            new_data_extra_col, 
            ["user_id", "email", "name", "total_events", "country"]
        )
        df_extra.write.format("delta").mode("append").save(table_path)
    except Exception as e:
        print(f"""
    ✗ Write FAILED (expected behavior!)
    
    Error: Schema mismatch detected.
    The incoming data has column 'country' which doesn't exist in the table.
    
    This protects against:
    - Accidental schema changes in ETL pipelines
    - Data quality issues from upstream changes
    - Corrupt data entering the analytics tables
    """)
    
    # =========================================================================
    # ADD COLUMN WITH mergeSchema
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 3: Adding New Columns (mergeSchema)")
    print("-" * 70)
    
    print("""
    To intentionally add new columns, use:
    - option("mergeSchema", "true") for single write
    - Spark config: spark.databricks.delta.schema.autoMerge.enabled = true
    """)
    
    new_data_with_extra = [
        (1004, "david@academy.example.com", "David Brown", 175, "US", "Desktop"),
        (1005, "eve@academy.example.com", "Eve Davis", 95, "CA", "Mobile"),
    ]
    
    df_new = spark.createDataFrame(
        new_data_with_extra,
        ["user_id", "email", "name", "total_events", "country", "preferred_device"]
    )
    
    print("\nWriting data with new columns using mergeSchema=true...")
    df_new.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .save(table_path)
    
    print("\n✓ New columns added successfully!")
    
    new_schema = spark.read.format("delta").load(table_path).schema
    print_schema_comparison(original_schema, new_schema, "Schema After Adding Columns")
    
    print("\nTable data (note NULL values for existing records):")
    spark.read.format("delta").load(table_path).orderBy("user_id").show()
    
    # =========================================================================
    # OVERWRITE SCHEMA
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 4: Complete Schema Replacement (overwriteSchema)")
    print("-" * 70)
    
    print("""
    For major schema changes, use overwriteSchema=true.
    This REPLACES the entire table schema (data is also replaced).
    
    Use cases:
    - Complete redesign of user activity tracking
    - Fixing incorrect schema definitions
    - Dev/test environments
    """)
    
    new_complete_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("total_sessions", IntegerType(), True),
        StructField("courses_enrolled", IntegerType(), True),
        StructField("is_premium", BooleanType(), True),
        StructField("created_at", StringType(), True),
    ])
    
    new_complete_data = [
        (1001, "alice@academy.example.com", "Alice S.", 45, 3, True, "2024-01-01"),
        (1002, "bob@academy.example.com", "Bob J.", 28, 2, False, "2024-01-02"),
    ]
    
    print("\nOverwriting with completely new schema...")
    df_new_schema = spark.createDataFrame(new_complete_data, new_complete_schema)
    df_new_schema.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .save(table_path)
    
    print("\n✓ Schema completely replaced!")
    print("\nNew Schema:")
    spark.read.format("delta").load(table_path).printSchema()
    spark.read.format("delta").load(table_path).show()
    
    # =========================================================================
    # ADD NESTED STRUCTURES
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 5: Adding Complex Types (Nested Structures)")
    print("-" * 70)
    
    print("""
    Delta Lake supports schema evolution with complex types:
    - Structs (nested objects) - for user preferences
    - Arrays - for tags/interests
    - Maps - for custom attributes
    """)
    
    schema_with_nested = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("total_sessions", IntegerType(), True),
        StructField("courses_enrolled", IntegerType(), True),
        StructField("is_premium", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("preferences", StructType([
            StructField("theme", StringType(), True),
            StructField("notifications", BooleanType(), True),
            StructField("language", StringType(), True),
        ]), True),
        StructField("interests", ArrayType(StringType()), True),
    ])
    
    nested_data = [
        (1003, "carol@academy.example.com", "Carol W.", 62, 5, True, "2024-01-03",
         {"theme": "dark", "notifications": True, "language": "en"},
         ["python", "data-engineering", "spark"]),
    ]
    
    df_nested = spark.createDataFrame(nested_data, schema_with_nested)
    
    print("\nAdding nested 'preferences' struct and 'interests' array...")
    df_nested.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .save(table_path)
    
    print("\n✓ Complex types added!")
    print("\nUpdated Schema:")
    spark.read.format("delta").load(table_path).printSchema()
    
    print("\nTable with nested data:")
    spark.read.format("delta").load(table_path).show(truncate=False)
    
    # =========================================================================
    # COLUMN TYPE CHANGES WITH ALTER TABLE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 6: Column Operations with ALTER TABLE")
    print("-" * 70)
    
    print("""
    Delta Lake supports column operations via ALTER TABLE:
    
    -- Add column
    ALTER TABLE delta.`/path` ADD COLUMN new_col STRING
    
    -- Change column type (compatible changes only)
    ALTER TABLE delta.`/path` ALTER COLUMN col_name TYPE new_type
    
    -- Rename column
    ALTER TABLE delta.`/path` RENAME COLUMN old_name TO new_name
    
    -- Add column comment
    ALTER TABLE delta.`/path` ALTER COLUMN col_name COMMENT 'description'
    
    -- Change nullability
    ALTER TABLE delta.`/path` ALTER COLUMN col_name DROP NOT NULL
    """)
    
    print("\nAdding 'engagement_score' column via ALTER TABLE...")
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` 
        ADD COLUMN engagement_score DOUBLE COMMENT 'User engagement score 0-100'
    """)
    
    print("\n✓ Column added!")
    print("\nSchema after ALTER TABLE:")
    spark.read.format("delta").load(table_path).printSchema()
    
    # =========================================================================
    # COLUMN MAPPING MODE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 7: Column Mapping Mode (Advanced)")
    print("-" * 70)
    
    print("""
    Column Mapping enables advanced schema evolution:
    
    - Rename columns without rewriting data
    - Drop columns without rewriting data
    - Reorder columns
    
    Enable with:
    ALTER TABLE delta.`/path` SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
    
    Modes:
    - 'none': Default, uses column position
    - 'name': Uses column names (enables rename/drop)
    - 'id': Uses internal column IDs (most flexible)
    """)
    
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
    """)
    
    print("\n✓ Column mapping mode enabled!")
    
    print("\nRenaming 'display_name' to 'name'...")
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}` 
        RENAME COLUMN display_name TO name
    """)
    
    print("\n✓ Column renamed!")
    print("\nSchema after rename:")
    spark.read.format("delta").load(table_path).printSchema()
    
    # =========================================================================
    # SCHEMA EVOLUTION IN MERGE
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 8: Schema Evolution in MERGE Operations")
    print("-" * 70)
    
    print("""
    MERGE operations can also evolve schema when combined with
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    This is useful for:
    - CDC (Change Data Capture) pipelines
    - User profile updates with new fields
    - ETL processes with evolving sources
    """)
    
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    source_data = [
        (1001, "alice@academy.example.com", "Alice Smith", 50, 4, True, 
         "2024-01-01", None, None, 85.5, "Gold"),
        (1006, "frank@academy.example.com", "Frank Miller", 15, 1, False,
         "2024-01-15", None, None, 42.0, "Bronze"),
    ]
    
    source_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("total_sessions", IntegerType(), True),
        StructField("courses_enrolled", IntegerType(), True),
        StructField("is_premium", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("preferences", StructType([
            StructField("theme", StringType(), True),
            StructField("notifications", BooleanType(), True),
            StructField("language", StringType(), True),
        ]), True),
        StructField("interests", ArrayType(StringType()), True),
        StructField("engagement_score", DoubleType(), True),
        StructField("membership_tier", StringType(), True),
    ])
    
    df_source = spark.createDataFrame(source_data, source_schema)
    
    print("\nPerforming MERGE with schema evolution (new 'membership_tier' column)...")
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    delta_table.alias("target").merge(
        df_source.alias("source"),
        "target.user_id = source.user_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("\n✓ MERGE completed with schema evolution!")
    print("\nFinal Schema:")
    spark.read.format("delta").load(table_path).printSchema()
    
    print("\nFinal Table Data:")
    spark.read.format("delta").load(table_path).select(
        "user_id", "name", "email", "courses_enrolled", "engagement_score", "membership_tier"
    ).orderBy("user_id").show()
    
    # =========================================================================
    # SCHEMA HISTORY
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 9: Viewing Schema History")
    print("-" * 70)
    
    print("\nTable history showing schema changes:")
    delta_table.history().select(
        "version", "timestamp", "operation"
    ).orderBy("version").show(truncate=False)
    
    stop_spark_if_local(spark)
    print("\n✓ Schema Evolution demonstration completed!")

if __name__ == "__main__":
    main()
