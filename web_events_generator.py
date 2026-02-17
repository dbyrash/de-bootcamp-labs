"""
Web Events Data Generator
==========================

Shared module for generating realistic web events data used across all Delta Lake examples.
This replaces the fake sales/inventory/product data with realistic web analytics events.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, floor, current_timestamp,
    concat, concat_ws, when, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType
)


# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

WEB_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("url", StringType(), True),
    StructField("url_path", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("academy_id", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
])

# Sample data configurations
URLS = [
    "/courses", "/courses/python-101", "/courses/data-engineering",
    "/courses/spark-fundamentals", "/courses/delta-lake-deep-dive",
    "/lessons/intro-to-sql", "/lessons/spark-basics", "/lessons/delta-lake",
    "/lessons/data-modeling", "/lessons/etl-patterns",
    "/quiz/python-quiz-1", "/quiz/sql-quiz-1", "/quiz/spark-quiz",
    "/dashboard", "/profile", "/settings", "/certificates",
    "/login", "/logout", "/signup",
]

EVENT_TYPES = [
    "page_view", "click", "scroll", "video_play", "video_pause", 
    "video_complete", "quiz_start", "quiz_submit", "lesson_complete",
    "course_enroll", "download", "search"
]

BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
OS_LIST = ["Windows", "macOS", "Linux", "iOS", "Android"]
DEVICES = ["Desktop", "Mobile", "Tablet"]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "AU", "JP", "BR", "IN", "MX"]
REGIONS = ["NA", "EU", "APAC", "LATAM"]

REFERRERS = [
    "https://google.com", "https://linkedin.com", "https://twitter.com",
    "https://facebook.com", "https://youtube.com", "direct", None
]


def generate_web_events(spark, num_events=1000, days_back=7):
    """
    Generate realistic web events data for Delta Lake examples.
    
    Args:
        spark: SparkSession
        num_events: Number of events to generate
        days_back: How many days back to spread events (for time-based demos)
    
    Returns:
        DataFrame with web events
    """
    urls_sql = "array(" + ", ".join(f"'{u}'" for u in URLS) + ")"
    event_types_sql = "array(" + ", ".join(f"'{e}'" for e in EVENT_TYPES) + ")"
    browsers_sql = "array(" + ", ".join(f"'{b}'" for b in BROWSERS) + ")"
    os_sql = "array(" + ", ".join(f"'{o}'" for o in OS_LIST) + ")"
    devices_sql = "array(" + ", ".join(f"'{d}'" for d in DEVICES) + ")"
    countries_sql = "array(" + ", ".join(f"'{c}'" for c in COUNTRIES) + ")"
    regions_sql = "array(" + ", ".join(f"'{r}'" for r in REGIONS) + ")"
    
    df = spark.range(num_events) \
        .withColumn("event_id", expr("uuid()")) \
        .withColumn("event_time", expr(f"from_unixtime(unix_timestamp() - cast(rand() * {days_back} * 24 * 60 * 60 as bigint))")) \
        .withColumn("event_type", expr(f"element_at({event_types_sql}, cast(rand() * {len(EVENT_TYPES)} as int) + 1)")) \
        .withColumn("url", expr(f"element_at({urls_sql}, cast(rand() * {len(URLS)} as int) + 1)")) \
        .withColumn("url_path", col("url")) \
        .withColumn("referrer", expr("""
            CASE 
                WHEN rand() < 0.25 THEN 'https://google.com'
                WHEN rand() < 0.40 THEN 'https://linkedin.com'
                WHEN rand() < 0.50 THEN 'https://twitter.com'
                WHEN rand() < 0.70 THEN NULL
                ELSE 'direct'
            END
        """)) \
        .withColumn("user_id", expr("CASE WHEN rand() < 0.1 THEN NULL ELSE cast(rand() * 10000 as int) END")) \
        .withColumn("session_id", expr("uuid()")) \
        .withColumn("academy_id", expr("cast(rand() * 50 as int) + 1")) \
        .withColumn("device_type", expr(f"element_at({devices_sql}, cast(rand() * {len(DEVICES)} as int) + 1)")) \
        .withColumn("browser", expr(f"element_at({browsers_sql}, cast(rand() * {len(BROWSERS)} as int) + 1)")) \
        .withColumn("os", expr(f"element_at({os_sql}, cast(rand() * {len(OS_LIST)} as int) + 1)")) \
        .withColumn("country", expr(f"element_at({countries_sql}, cast(rand() * {len(COUNTRIES)} as int) + 1)")) \
        .withColumn("region", expr(f"element_at({regions_sql}, cast(rand() * {len(REGIONS)} as int) + 1)")) \
        .drop("id")
    
    return df


def generate_web_events_batch(spark, batch_size=100, batch_num=0):
    """
    Generate a small batch of web events (for demonstrating small file problem).
    
    Args:
        spark: SparkSession
        batch_size: Number of events per batch
        batch_num: Batch number (for reproducibility)
    
    Returns:
        DataFrame with web events
    """
    return generate_web_events(spark, num_events=batch_size, days_back=1)


def generate_initial_web_events(spark):
    """
    Generate a small set of web events for basic demos (time travel, vacuum, etc.).
    Returns a list of tuples that can be used with createDataFrame.
    """
    return [
        ("E001", "page_view", "/courses/python-101", "US", "NA", 1001, "Chrome", "Desktop"),
        ("E002", "click", "/lessons/intro-to-sql", "UK", "EU", 1002, "Firefox", "Desktop"),
        ("E003", "video_play", "/lessons/spark-basics", "CA", "NA", 1003, "Safari", "Mobile"),
        ("E004", "quiz_start", "/quiz/python-quiz-1", "DE", "EU", 1001, "Chrome", "Desktop"),
        ("E005", "page_view", "/dashboard", "US", "NA", 1004, "Edge", "Tablet"),
        ("E006", "lesson_complete", "/lessons/delta-lake", "FR", "EU", 1002, "Firefox", "Desktop"),
        ("E007", "course_enroll", "/courses/data-engineering", "AU", "APAC", 1005, "Chrome", "Mobile"),
        ("E008", "download", "/certificates", "JP", "APAC", 1003, "Safari", "Desktop"),
    ]


def get_web_events_columns():
    """Return column names for basic web events data."""
    return ["event_id", "event_type", "url_path", "country", "region", "user_id", "browser", "device_type"]


def generate_user_activity_data(spark, num_users=100):
    """
    Generate user activity summary data for schema evolution and merge demos.
    """
    df = spark.range(num_users) \
        .withColumn("user_id", (col("id") + 1000).cast("int")) \
        .withColumn("email", concat(lit("user"), col("id"), lit("@academy.example.com"))) \
        .withColumn("name", concat(lit("User "), col("id"))) \
        .withColumn("total_events", floor(rand() * 500 + 10).cast("int")) \
        .withColumn("total_sessions", floor(rand() * 50 + 1).cast("int")) \
        .withColumn("courses_enrolled", floor(rand() * 10).cast("int")) \
        .withColumn("lessons_completed", floor(rand() * 30).cast("int")) \
        .withColumn("quiz_score_avg", rand() * 40 + 60) \
        .withColumn("preferred_device", expr("CASE WHEN rand() < 0.6 THEN 'Desktop' WHEN rand() < 0.85 THEN 'Mobile' ELSE 'Tablet' END")) \
        .withColumn("country", expr("element_at(array('US', 'UK', 'CA', 'DE', 'FR', 'AU'), cast(rand() * 6 as int) + 1)")) \
        .withColumn("signup_date", expr("date_sub(current_date(), cast(rand() * 365 as int))")) \
        .withColumn("is_premium", expr("rand() < 0.3")) \
        .drop("id")
    
    return df


def generate_daily_metrics(spark, num_days=30):
    """
    Generate daily aggregated metrics for Gold layer demos.
    """
    df = spark.range(num_days) \
        .withColumn("metric_date", expr("date_sub(current_date(), cast(id as int))")) \
        .withColumn("total_page_views", floor(rand() * 50000 + 10000).cast("int")) \
        .withColumn("unique_visitors", floor(rand() * 5000 + 1000).cast("int")) \
        .withColumn("new_signups", floor(rand() * 200 + 20).cast("int")) \
        .withColumn("course_enrollments", floor(rand() * 100 + 10).cast("int")) \
        .withColumn("lessons_completed", floor(rand() * 500 + 100).cast("int")) \
        .withColumn("quiz_submissions", floor(rand() * 300 + 50).cast("int")) \
        .withColumn("avg_session_duration_min", rand() * 20 + 5) \
        .withColumn("bounce_rate", rand() * 0.3 + 0.2) \
        .drop("id")
    
    return df
