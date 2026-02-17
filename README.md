# Delta Lake Optimizations with PySpark

A comprehensive collection of Python scripts demonstrating Delta Lake features and optimizations.

## Local Setup

### Requirements

- Python 3.8+
- Java 8 or 11 (required for PySpark)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd delta-table-day-1
```

2. (Optional) Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Verify Java Installation

PySpark requires Java. Verify it's installed:
```bash
java -version
```

If not installed:
- **macOS**: `brew install openjdk@11`
- **Ubuntu**: `sudo apt install openjdk-11-jdk`
- **Windows**: Download from [Adoptium](https://adoptium.net/)


## Scripts Overview

| Script | Topic | Key Concepts |
|--------|-------|--------------|
| `01_delta_table_basics.py` | Introduction | Table creation, ACID transactions, transaction log |
| `02_time_travel.py` | Time Travel | Version queries, restore, audit history |
| `03_compaction_optimize.py` | Compaction | OPTIMIZE, Z-ORDER, auto-optimization |
| `04_vacuum.py` | Vacuuming | Storage cleanup, retention policies |
| `05_schema_evolution.py` | Schema Evolution | mergeSchema, column mapping, ALTER TABLE |
| `06_merge_upsert.py` | MERGE Operations | Upserts, SCD Type 2, deduplication |
| `07_maintenance_best_practices.py` | Best Practices | Health checks, maintenance workflows |

## Running the Scripts

```bash
python 01_delta_table_basics.py
python 02_time_travel.py
python 03_compaction_optimize.py
python 04_vacuum.py
python 05_schema_evolution.py
python 06_merge_upsert.py
python 07_maintenance_best_practices.py
```

## Quick Reference

### Time Travel
```python
# Query by version
df = spark.read.format("delta").option("versionAsOf", 0).load(path)

# Query by timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load(path)

# Restore
delta_table.restoreToVersion(2)
```

### Compaction (OPTIMIZE)
```python
# Basic compaction
delta_table.optimize().executeCompaction()

# Z-ORDER for query optimization
delta_table.optimize().executeZOrderBy("region", "date")
```

### VACUUM
```sql
-- Dry run first
VACUUM delta.`/path` RETAIN 168 HOURS DRY RUN

-- Execute
VACUUM delta.`/path` RETAIN 168 HOURS
```

### Schema Evolution
```python
# Add columns
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)

# Replace schema
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
```

### MERGE (Upsert)
```python
delta_table.alias("target").merge(
    source.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

## Maintenance Schedule

| Frequency | Operation |
|-----------|-----------|
| Daily | OPTIMIZE high-churn tables |
| Weekly | OPTIMIZE all tables, VACUUM |
| Monthly | Review partitioning, analyze patterns |

## Key Configuration

```python
# Auto-optimization
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

## Output Directory

All scripts create tables in `./delta_tables/` directory.
