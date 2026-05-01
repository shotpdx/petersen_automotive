"""
Silver layer for Petersen Automotive Museum pipeline.

Reads from bronze tables and applies data quality transformations:
- Type casting (date/timestamp strings to proper types)
- Data validation (health scores, costs, temperatures, humidity)
- Filtering of invalid records
- Adding processing metadata columns
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_vehicles",
    cluster_by=["vehicle_id"],
    comment="Cleaned and validated vehicle master data"
)
def silver_vehicles():
    """
    Clean vehicle master data from bronze layer.
    
    Transformations:
    - Cast date strings to DATE type
    - Add processing timestamp
    """
    return (
        spark.read.table("bronze_vehicles")
        # Cast date columns
        .withColumn(
            "acquisition_date",
            F.to_date(F.col("acquisition_date"), "yyyy-MM-dd")
        )
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
    )


@dp.table(
    name="silver_maintenance_events",
    cluster_by=["vehicle_id"],
    comment="Cleaned and validated maintenance event records"
)
def silver_maintenance_events():
    """
    Clean maintenance event records from bronze layer.
    
    Transformations:
    - Cast date strings to proper types
    - Validate cost is positive
    - Validate health_score_after is in 0-100 range
    - Filter out invalid records
    - Add processing timestamp
    """
    return (
        spark.read.table("bronze_maintenance_events")
        # Cast date columns
        .withColumn(
            "event_date",
            F.to_date(F.col("event_date"), "yyyy-MM-dd")
        )
        # Validate cost is positive
        .filter(
            (F.col("cost").isNotNull()) &
            (F.col("cost") > 0)
        )
        # Validate health_score_after is in valid range (0-100)
        .filter(
            (F.col("health_score_after").isNotNull()) &
            (F.col("health_score_after") >= 0) &
            (F.col("health_score_after") <= 100)
        )
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
    )


@dp.table(
    name="silver_sensor_readings",
    cluster_by=["vehicle_id"],
    comment="Cleaned and validated sensor reading records"
)
def silver_sensor_readings():
    """
    Clean sensor reading data from bronze layer.
    
    Transformations:
    - Cast timestamp strings to TIMESTAMP type
    - Validate temperature_f is in realistic range (32-120°F)
    - Validate humidity_pct is in 0-100 range
    - Filter out invalid records
    - Add processing timestamp
    """
    return (
        spark.read.table("bronze_sensor_readings")
        # Cast timestamp column
        .withColumn(
            "reading_timestamp",
            F.to_timestamp(F.col("reading_timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        # Validate temperature is in realistic range (32-120°F)
        .filter(
            (F.col("temperature_f").isNotNull()) &
            (F.col("temperature_f") >= 32) &
            (F.col("temperature_f") <= 120)
        )
        # Validate humidity is in valid range (0-100%)
        .filter(
            (F.col("humidity_pct").isNotNull()) &
            (F.col("humidity_pct") >= 0) &
            (F.col("humidity_pct") <= 100)
        )
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
    )


@dp.table(
    name="silver_condition_assessments",
    cluster_by=["vehicle_id"],
    comment="Cleaned and validated condition assessment records"
)
def silver_condition_assessments():
    """
    Clean condition assessment records from bronze layer.
    
    Transformations:
    - Cast date strings to proper types
    - Validate health_score is in 0-100 range
    - Filter out invalid records
    - Add processing timestamp
    """
    return (
        spark.read.table("bronze_condition_assessments")
        # Cast date columns
        .withColumn(
            "assessment_date",
            F.to_date(F.col("assessment_date"), "yyyy-MM-dd")
        )
        # Validate health_score is in valid range (0-100)
        .filter(
            (F.col("health_score").isNotNull()) &
            (F.col("health_score") >= 0) &
            (F.col("health_score") <= 100)
        )
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
    )
