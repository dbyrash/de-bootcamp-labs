"""
Delta Lake Maintenance & Best Practices
=======================================

This script combines all maintenance operations into a comprehensive
workflow that you can adapt for production environments.

Topics covered:
- Complete maintenance workflow
- Monitoring table health
- Automated maintenance scheduling
- Performance tuning configurations
- Common issues and solutions

Data Source: Web events from an online learning platform
"""

from pyspark.sql.functions import col, lit, current_timestamp, expr, rand, floor
from delta import DeltaTable
from datetime import datetime
import time

from web_events_generator import generate_web_events
from databricks_utils import get_spark, get_base_path, cleanup_path, stop_spark_if_local, is_databricks

class DeltaTableHealthCheck:
    """Utility class to analyze Delta table health and recommend maintenance."""
    
    def __init__(self, spark, table_path):
        self.spark = spark
        self.table_path = table_path
        self.delta_table = DeltaTable.forPath(spark, table_path)
    
    def get_file_statistics(self):
        """Get statistics about data files."""
        if is_databricks():
            detail = self.spark.sql(f"DESCRIBE DETAIL delta.`{self.table_path}`").collect()[0]
            num_files = detail['numFiles']
            total_size = detail['sizeInBytes']
            if num_files == 0:
                return {'num_files': 0, 'total_size_mb': 0, 'avg_size_mb': 0, 'min_size_mb': 0, 'max_size_mb': 0}
            return {
                'num_files': num_files,
                'total_size_mb': total_size / (1024 * 1024),
                'avg_size_mb': (total_size / num_files) / (1024 * 1024),
                'min_size_mb': 0,  # Not available via DESCRIBE DETAIL
                'max_size_mb': 0,
            }
        else:
            import os
            files = []
            for root, dirs, filenames in os.walk(self.table_path):
                if '_delta_log' in root:
                    continue
                for f in filenames:
                    if f.endswith('.parquet'):
                        path = os.path.join(root, f)
                        files.append({
                            'name': f,
                            'size_bytes': os.path.getsize(path)
                        })
            
            if not files:
                return {'num_files': 0, 'total_size_mb': 0, 'avg_size_mb': 0, 'min_size_mb': 0, 'max_size_mb': 0}
            
            sizes = [f['size_bytes'] for f in files]
            return {
                'num_files': len(files),
                'total_size_mb': sum(sizes) / (1024 * 1024),
                'avg_size_mb': (sum(sizes) / len(sizes)) / (1024 * 1024),
                'min_size_mb': min(sizes) / (1024 * 1024),
                'max_size_mb': max(sizes) / (1024 * 1024),
            }
    
    def get_version_info(self):
        """Get version and history information."""
        history = self.delta_table.history()
        latest = history.orderBy(col("version").desc()).first()
        oldest = history.orderBy(col("version").asc()).first()
        
        return {
            'current_version': latest['version'],
            'oldest_version': oldest['version'],
            'total_versions': history.count(),
            'latest_operation': latest['operation'],
            'latest_timestamp': latest['timestamp'],
        }
    
    def analyze_and_recommend(self):
        """Analyze table and provide maintenance recommendations."""
        file_stats = self.get_file_statistics()
        version_info = self.get_version_info()
        
        recommendations = []
        
        if file_stats['num_files'] > 10 and file_stats['avg_size_mb'] < 64:
            recommendations.append({
                'issue': 'Small File Problem',
                'severity': 'HIGH',
                'action': 'Run OPTIMIZE to compact small files',
                'details': f"{file_stats['num_files']} files with avg size {file_stats['avg_size_mb']:.2f} MB"
            })
        
        if file_stats['max_size_mb'] > 0 and file_stats['min_size_mb'] / file_stats['max_size_mb'] < 0.1:
            recommendations.append({
                'issue': 'Uneven File Sizes',
                'severity': 'MEDIUM',
                'action': 'Run OPTIMIZE to balance file sizes',
                'details': f"Min: {file_stats['min_size_mb']:.2f} MB, Max: {file_stats['max_size_mb']:.2f} MB"
            })
        
        if version_info['total_versions'] > 100:
            recommendations.append({
                'issue': 'High Version Count',
                'severity': 'LOW',
                'action': 'Consider running VACUUM to clean up old versions',
                'details': f"{version_info['total_versions']} versions in history"
            })
        
        return {
            'file_statistics': file_stats,
            'version_info': version_info,
            'recommendations': recommendations,
            'health_score': self._calculate_health_score(file_stats, version_info, recommendations)
        }
    
    def _calculate_health_score(self, file_stats, version_info, recommendations):
        """Calculate a health score from 0-100."""
        score = 100
        
        for rec in recommendations:
            if rec['severity'] == 'HIGH':
                score -= 30
            elif rec['severity'] == 'MEDIUM':
                score -= 15
            elif rec['severity'] == 'LOW':
                score -= 5
        
        return max(0, score)

def run_maintenance_workflow(spark, table_path, options=None):
    """
    Run a complete maintenance workflow on a Delta table.
    
    Args:
        spark: SparkSession
        table_path: Path to Delta table
        options: Dict with optional settings:
            - run_optimize: bool (default: True)
            - run_vacuum: bool (default: True)
            - vacuum_retain_hours: int (default: 168)
            - zorder_columns: list (default: None)
            - analyze_stats: bool (default: True)
    """
    options = options or {}
    run_optimize = options.get('run_optimize', True)
    run_vacuum = options.get('run_vacuum', True)
    vacuum_retain_hours = options.get('vacuum_retain_hours', 168)
    zorder_columns = options.get('zorder_columns', None)
    analyze_stats = options.get('analyze_stats', True)
    
    results = {
        'start_time': datetime.now(),
        'steps': []
    }
    
    delta_table = DeltaTable.forPath(spark, table_path)
    
    print("\n📊 Step 1: Running health check...")
    health_checker = DeltaTableHealthCheck(spark, table_path)
    health_report = health_checker.analyze_and_recommend()
    results['initial_health'] = health_report
    print(f"   Initial health score: {health_report['health_score']}/100")
    
    if run_optimize:
        print("\n🔧 Step 2: Running OPTIMIZE...")
        start = time.time()
        
        if zorder_columns:
            print(f"   Z-ORDER columns: {zorder_columns}")
            optimize_result = delta_table.optimize().executeZOrderBy(*zorder_columns)
        else:
            optimize_result = delta_table.optimize().executeCompaction()
        
        elapsed = time.time() - start
        results['steps'].append({
            'name': 'OPTIMIZE',
            'duration_seconds': elapsed,
            'success': True
        })
        print(f"   ✓ Completed in {elapsed:.2f} seconds")
    
    if run_vacuum:
        print(f"\n🧹 Step 3: Running VACUUM (retain {vacuum_retain_hours} hours)...")
        start = time.time()
        
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN {vacuum_retain_hours} HOURS")
        
        elapsed = time.time() - start
        results['steps'].append({
            'name': 'VACUUM',
            'duration_seconds': elapsed,
            'success': True
        })
        print(f"   ✓ Completed in {elapsed:.2f} seconds")
    
    if analyze_stats:
        print("\n📈 Step 4: Computing statistics...")
        start = time.time()
        
        spark.sql(f"ANALYZE TABLE delta.`{table_path}` COMPUTE STATISTICS")
        
        elapsed = time.time() - start
        results['steps'].append({
            'name': 'ANALYZE',
            'duration_seconds': elapsed,
            'success': True
        })
        print(f"   ✓ Completed in {elapsed:.2f} seconds")
    
    print("\n📊 Step 5: Running final health check...")
    health_report_final = health_checker.analyze_and_recommend()
    results['final_health'] = health_report_final
    print(f"   Final health score: {health_report_final['health_score']}/100")
    
    results['end_time'] = datetime.now()
    results['total_duration'] = (results['end_time'] - results['start_time']).total_seconds()
    
    return results

def main():
    spark = get_spark()
    
    table_path = get_base_path("web_events_maintenance")
    cleanup_path(spark, table_path)
    print("=" * 70)
    print("DELTA LAKE MAINTENANCE & BEST PRACTICES")
    print("=" * 70)
    
    # =========================================================================
    # CREATE A TABLE THAT NEEDS MAINTENANCE
    # =========================================================================
    print("\n" + "-" * 70)
    print("SETUP: Creating a web events table that needs maintenance")
    print("-" * 70)
    
    num_batches = 15
    records_per_batch = 50
    
    print(f"\nSimulating {num_batches} small batch writes of web events...")
    
    for i in range(num_batches):
        df = generate_web_events(spark, num_events=records_per_batch, days_back=7)
        
        mode = "overwrite" if i == 0 else "append"
        df.write.format("delta").mode(mode).save(table_path)
    
    print(f"✓ Created table with {num_batches} small file batches")
    
    delta_table = DeltaTable.forPath(spark, table_path)
    for _ in range(5):
        delta_table.update(
            condition=expr("rand() < 0.1"),
            set={"event_type": lit("updated_event")}
        )
    print("✓ Added 5 update operations")
    
    # =========================================================================
    # TABLE HEALTH CHECK
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 1: Table Health Analysis")
    print("-" * 70)
    
    health_checker = DeltaTableHealthCheck(spark, table_path)
    report = health_checker.analyze_and_recommend()
    
    print(f"""
    📊 TABLE HEALTH REPORT
    ═══════════════════════════════════════════════════════════════
    
    FILE STATISTICS:
    ├── Number of files: {report['file_statistics']['num_files']}
    ├── Total size: {report['file_statistics']['total_size_mb']:.2f} MB
    ├── Average file size: {report['file_statistics']['avg_size_mb']:.4f} MB
    ├── Min file size: {report['file_statistics']['min_size_mb']:.4f} MB
    └── Max file size: {report['file_statistics']['max_size_mb']:.4f} MB
    
    VERSION INFO:
    ├── Current version: {report['version_info']['current_version']}
    ├── Total versions: {report['version_info']['total_versions']}
    └── Latest operation: {report['version_info']['latest_operation']}
    
    HEALTH SCORE: {report['health_score']}/100
    """)
    
    if report['recommendations']:
        print("    RECOMMENDATIONS:")
        for rec in report['recommendations']:
            print(f"    ⚠️  [{rec['severity']}] {rec['issue']}")
            print(f"       Action: {rec['action']}")
            print(f"       Details: {rec['details']}")
    
    # =========================================================================
    # RUN MAINTENANCE WORKFLOW
    # =========================================================================
    print("\n" + "-" * 70)
    print("STEP 2: Running Maintenance Workflow")
    print("-" * 70)
    
    maintenance_options = {
        'run_optimize': True,
        'run_vacuum': True,
        'vacuum_retain_hours': 0,  # For demo only - use 168+ in production!
        'zorder_columns': ['region', 'device_type'],
        'analyze_stats': True
    }
    
    results = run_maintenance_workflow(spark, table_path, maintenance_options)
    
    print(f"""
    
    ═══════════════════════════════════════════════════════════════
    MAINTENANCE WORKFLOW SUMMARY
    ═══════════════════════════════════════════════════════════════
    
    Total Duration: {results['total_duration']:.2f} seconds
    
    Steps Completed:
    """)
    
    for step in results['steps']:
        print(f"    ✓ {step['name']}: {step['duration_seconds']:.2f}s")
    
    print(f"""
    Health Score Improvement:
    ├── Before: {results['initial_health']['health_score']}/100
    └── After: {results['final_health']['health_score']}/100
    
    File Count:
    ├── Before: {results['initial_health']['file_statistics']['num_files']}
    └── After: {results['final_health']['file_statistics']['num_files']}
    """)
    
    # =========================================================================
    # BEST PRACTICES REFERENCE
    # =========================================================================
    print("\n" + "-" * 70)
    print("REFERENCE: Delta Lake Best Practices for Web Events")
    print("-" * 70)
    
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║              PRODUCTION BEST PRACTICES FOR WEB EVENTS                ║
    ╠══════════════════════════════════════════════════════════════════════╣
    ║                                                                      ║
    ║  📁 FILE MANAGEMENT                                                  ║
    ║  ─────────────────                                                   ║
    ║  • Target file size: 1GB for batch, 128MB-256MB for streaming        ║
    ║  • Enable auto-optimize for streaming web events:                    ║
    ║    spark.databricks.delta.autoCompact.enabled = true                 ║
    ║    spark.databricks.delta.optimizeWrite.enabled = true               ║
    ║  • Run OPTIMIZE weekly or when file count > 10x ideal                ║
    ║                                                                      ║
    ║  🧹 VACUUM STRATEGY                                                  ║
    ║  ─────────────────                                                   ║
    ║  • Default retention: 7 days minimum                                 ║
    ║  • Increase for compliance: 30-90 days for analytics data            ║
    ║  • Schedule: Weekly for most tables                                  ║
    ║  • Always run OPTIMIZE before VACUUM                                 ║
    ║                                                                      ║
    ║  📊 Z-ORDERING FOR WEB EVENTS                                        ║
    ║  ─────────────────────────────                                       ║
    ║  • Choose 1-4 high-cardinality columns                               ║
    ║  • Recommended: region, device_type, event_type                      ║
    ║  • Re-run Z-ORDER after significant data changes                     ║
    ║  • Don't Z-ORDER on already-partitioned columns                      ║
    ║                                                                      ║
    ║  🔀 PARTITIONING FOR WEB EVENTS                                      ║
    ║  ───────────────────────────────                                     ║
    ║  • Partition by event_date for time-series queries                   ║
    ║  • Consider academy_id for multi-tenant isolation                    ║
    ║  • Target: 1GB+ per partition                                        ║
    ║  • Avoid over-partitioning (thousands of partitions)                 ║
    ║                                                                      ║
    ║  ⏰ MAINTENANCE SCHEDULE                                             ║
    ║  ─────────────────────                                               ║
    ║  • Hourly: Auto-compact for streaming tables                         ║
    ║  • Daily: OPTIMIZE high-churn web event tables                       ║
    ║  • Weekly: OPTIMIZE all tables, VACUUM                               ║
    ║  • Monthly: Review partition strategy, analyze query patterns        ║
    ║                                                                      ║
    ║  📈 MONITORING                                                       ║
    ║  ──────────                                                          ║
    ║  • Track file count per table                                        ║
    ║  • Monitor average file size                                         ║
    ║  • Alert on version count > threshold                                ║
    ║  • Log maintenance operation durations                               ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    # =========================================================================
    # CONFIGURATION REFERENCE
    # =========================================================================
    print("\n" + "-" * 70)
    print("REFERENCE: Key Configuration Options")
    print("-" * 70)
    
    print("""
    ┌────────────────────────────────────────────────────────────────────┐
    │ SPARK SESSION CONFIGURATIONS                                       │
    ├────────────────────────────────────────────────────────────────────┤
    │                                                                    │
    │ # Auto-optimization (great for streaming web events)               │
    │ spark.databricks.delta.autoCompact.enabled = true                  │
    │ spark.databricks.delta.optimizeWrite.enabled = true                │
    │                                                                    │
    │ # Target file sizes                                                │
    │ spark.databricks.delta.optimize.maxFileSize = 1073741824  # 1GB    │
    │ spark.databricks.delta.autoCompact.minNumFiles = 50                │
    │                                                                    │
    │ # Schema evolution                                                 │
    │ spark.databricks.delta.schema.autoMerge.enabled = true             │
    │                                                                    │
    │ # Retention (disable check for testing only!)                      │
    │ spark.databricks.delta.retentionDurationCheck.enabled = true       │
    │                                                                    │
    └────────────────────────────────────────────────────────────────────┘
    
    ┌────────────────────────────────────────────────────────────────────┐
    │ TABLE PROPERTIES (via ALTER TABLE SET TBLPROPERTIES)               │
    ├────────────────────────────────────────────────────────────────────┤
    │                                                                    │
    │ # Retention settings                                               │
    │ delta.deletedFileRetentionDuration = 'interval 7 days'             │
    │ delta.logRetentionDuration = 'interval 30 days'                    │
    │                                                                    │
    │ # Auto-optimization per table                                      │
    │ delta.autoOptimize.optimizeWrite = true                            │
    │ delta.autoOptimize.autoCompact = true                              │
    │                                                                    │
    │ # Data skipping                                                    │
    │ delta.dataSkippingNumIndexedCols = 32  # default                   │
    │                                                                    │
    │ # Column mapping for rename/drop                                   │
    │ delta.columnMapping.mode = 'name'                                  │
    │                                                                    │
    └────────────────────────────────────────────────────────────────────┘
    """)
    
    print("\n" + "-" * 70)
    print("Final Table State")
    print("-" * 70)
    
    print("\nTable properties:")
    spark.sql(f"SHOW TBLPROPERTIES delta.`{table_path}`").show(truncate=False)
    
    print("\nTable history (last 5 operations):")
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.history(5).select(
        "version", "timestamp", "operation"
    ).show(truncate=False)
    
    print("\nSample web events data:")
    spark.read.format("delta").load(table_path).select(
        "event_id", "event_type", "url_path", "region", "device_type"
    ).show(5)
    
    stop_spark_if_local(spark)
    print("\n✓ Maintenance demonstration completed!")

if __name__ == "__main__":
    main()
