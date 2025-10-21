#!/usr/bin/env python3
"""
Long-Running Spark Job for UI Validation

This script runs for approximately 10 minutes and demonstrates various
Spark operations to showcase different UI features:
- Jobs and Stages
- SQL/DataFrame operations
- Executor metrics
- Storage/caching
- Shuffle operations

Perfect for testing Spark UI access and exploring its features.
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, expr, max, min, rand, sum


def main():
    print("=" * 80)
    print("LONG-RUNNING SPARK JOB FOR UI VALIDATION")
    print("=" * 80)
    print()
    print("This job will run for approximately 10 minutes.")
    print("Use this time to explore the Spark UI features:")
    print("  • Jobs tab - See job progression")
    print("  • Stages tab - Monitor stage execution")
    print("  • Storage tab - View cached DataFrames")
    print("  • Executors tab - Check executor metrics")
    print("  • SQL tab - Inspect query plans")
    print()

    # Create Spark session
    spark = SparkSession.builder.appName("Long-Running UI Validation Job").getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    print("✓ Spark session created")
    print()

    # ========================================================================
    # STAGE 1: Generate Large Dataset (2 minutes)
    # ========================================================================
    print("-" * 80)
    print("STAGE 1: Generating large dataset (100 million rows)")
    print("Expected duration: ~2 minutes")
    print("-" * 80)
    print()

    # Generate a large dataset with 100 million rows
    df_large = (
        spark.range(0, 100_000_000)
        .withColumn("category", (col("id") % 10).cast("string"))
        .withColumn("value", (rand() * 1000).cast("integer"))
        .withColumn("region", expr("array('North', 'South', 'East', 'West')[cast(id % 4 as int)]"))
    )

    print("  Dataset schema:")
    df_large.printSchema()

    # Trigger evaluation with a count
    total_rows = df_large.count()
    print(f"  ✓ Generated {total_rows:,} rows")
    print()

    time.sleep(5)  # Pause to observe UI

    # ========================================================================
    # STAGE 2: Cache and Aggregations (2 minutes)
    # ========================================================================
    print("-" * 80)
    print("STAGE 2: Caching dataset and performing aggregations")
    print("Expected duration: ~2 minutes")
    print("-" * 80)
    print()

    # Cache the dataset to observe Storage tab
    df_large.cache()
    print("  Caching dataset in memory...")

    # Force caching with an action
    cached_count = df_large.count()
    print(f"  ✓ Cached {cached_count:,} rows")
    print()

    print("  Check Spark UI → Storage tab to see cached DataFrame!")
    print()

    time.sleep(10)  # Pause to check Storage tab

    # Perform aggregations by category
    print("  Aggregating by category...")
    agg_by_category = (
        df_large.groupBy("category")
        .agg(
            count("id").alias("count"),
            sum("value").alias("total_value"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value"),
        )
        .orderBy("category")
    )

    print()
    print("  Category Aggregations:")
    agg_by_category.show()
    print()

    time.sleep(5)

    # ========================================================================
    # STAGE 3: Shuffle-Heavy Operations (3 minutes)
    # ========================================================================
    print("-" * 80)
    print("STAGE 3: Shuffle-heavy operations (joins and repartitioning)")
    print("Expected duration: ~3 minutes")
    print("-" * 80)
    print()

    # Create a dimension table for joins
    print("  Creating dimension table...")
    dim_categories = spark.createDataFrame(
        [
            ("0", "Electronics", "Tech"),
            ("1", "Books", "Media"),
            ("2", "Clothing", "Fashion"),
            ("3", "Food", "Grocery"),
            ("4", "Toys", "Entertainment"),
            ("5", "Sports", "Recreation"),
            ("6", "Tools", "Hardware"),
            ("7", "Garden", "Outdoor"),
            ("8", "Beauty", "Personal Care"),
            ("9", "Auto", "Automotive"),
        ],
        ["category_id", "category_name", "department"],
    )

    print("  ✓ Dimension table created")
    print()

    # Perform join (will cause shuffle)
    print("  Performing join operation (watch for shuffle in UI)...")
    df_joined = df_large.join(
        dim_categories, df_large.category == dim_categories.category_id, "inner"
    )

    # Show joined results
    print()
    print("  Joined Data Sample:")
    df_joined.select("id", "category", "category_name", "department", "value", "region").show(10)
    print()

    time.sleep(10)  # Pause to observe shuffle

    # Repartition to create more shuffle
    print("  Repartitioning data (32 partitions)...")
    df_repartitioned = df_joined.repartition(32, "department")

    # Count to trigger repartitioning
    repartitioned_count = df_repartitioned.count()
    print(f"  ✓ Repartitioned {repartitioned_count:,} rows across 32 partitions")
    print()

    time.sleep(5)

    # ========================================================================
    # STAGE 4: Multi-dimensional Analysis (2 minutes)
    # ========================================================================
    print("-" * 80)
    print("STAGE 4: Multi-dimensional analysis")
    print("Expected duration: ~2 minutes")
    print("-" * 80)
    print()

    # Aggregate by department and region
    print("  Aggregating by department and region...")
    dept_region_agg = (
        df_repartitioned.groupBy("department", "region")
        .agg(
            count("id").alias("transactions"),
            sum("value").alias("total_sales"),
            avg("value").alias("avg_transaction"),
        )
        .orderBy("department", "region")
    )

    print()
    print("  Department × Region Sales Analysis:")
    dept_region_agg.show(20)
    print()

    time.sleep(10)  # Pause to view results

    # ========================================================================
    # STAGE 5: Window Functions and Complex Queries (1 minute)
    # ========================================================================
    print("-" * 80)
    print("STAGE 5: Window functions and complex SQL")
    print("Expected duration: ~1 minute")
    print("-" * 80)
    print()

    from pyspark.sql.functions import dense_rank, row_number
    from pyspark.sql.window import Window

    # Create a window for ranking
    window_spec = Window.partitionBy("department").orderBy(col("total_sales").desc())

    # Add rankings
    print("  Computing regional rankings within departments...")
    ranked_regions = dept_region_agg.withColumn("rank", row_number().over(window_spec)).withColumn(
        "dense_rank", dense_rank().over(window_spec)
    )

    # Show top regions per department
    print()
    print("  Top Performing Regions by Department:")
    ranked_regions.filter(col("rank") <= 2).orderBy("department", "rank").show()
    print()

    time.sleep(5)

    # ========================================================================
    # FINAL STAGE: Summary Statistics (1 minute)
    # ========================================================================
    print("-" * 80)
    print("FINAL STAGE: Computing summary statistics")
    print("-" * 80)
    print()

    # Overall statistics
    print("  Computing overall statistics...")
    overall_stats = df_repartitioned.agg(
        count("id").alias("total_transactions"),
        sum("value").alias("total_revenue"),
        avg("value").alias("avg_transaction"),
        min("value").alias("min_transaction"),
        max("value").alias("max_transaction"),
    )

    print()
    print("  OVERALL STATISTICS:")
    print("  " + "=" * 76)
    overall_stats.show(truncate=False)
    print()

    # Department summary
    print("  Department Summary:")
    dept_summary = (
        df_repartitioned.groupBy("department")
        .agg(count("id").alias("transactions"), sum("value").alias("revenue"))
        .orderBy(col("revenue").desc())
    )

    dept_summary.show()
    print()

    # Region summary
    print("  Region Summary:")
    region_summary = (
        df_repartitioned.groupBy("region")
        .agg(count("id").alias("transactions"), sum("value").alias("revenue"))
        .orderBy(col("revenue").desc())
    )

    region_summary.show()
    print()

    # Cleanup
    print("-" * 80)
    print("Cleaning up...")
    df_large.unpersist()
    print("  ✓ Unpersisted cached DataFrame")
    print()

    # Final summary
    print("=" * 80)
    print("JOB COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print()
    print("Total execution time: ~10 minutes")
    print()
    print("What you should have observed in Spark UI:")
    print("  ✓ Jobs tab - Multiple jobs corresponding to each stage")
    print("  ✓ Stages tab - Detailed stage execution with tasks")
    print("  ✓ Storage tab - Cached DataFrame (Stage 2)")
    print("  ✓ Executors tab - Executor metrics and resource usage")
    print("  ✓ SQL tab - Query plans for DataFrame operations")
    print()
    print("Spark UI features to explore:")
    print("  • Click on job names to see stages")
    print("  • Click on stages to see task details")
    print("  • Check 'Event Timeline' for task scheduling")
    print("  • View 'DAG Visualization' for execution plan")
    print("  • Monitor executor GC time and memory usage")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
