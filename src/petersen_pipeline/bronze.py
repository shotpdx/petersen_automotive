"""
Bronze layer for Petersen Automotive Museum pipeline.

Ingests raw parquet files from UC Volume and creates streaming tables
with minimal transformation (just adding metadata columns).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="bronze_vehicles",
    cluster_by=["vehicle_id"],
    comment="Raw vehicle master data from source system"
)
def bronze_vehicles():
    """
    Ingest vehicle master data from parquet files.
    
    Adds ingestion timestamp for lineage tracking.
    """
    return (
        spark.read.format("parquet")
        .load("/Volumes/sandbox_us_west_2/lakefoundry/artifacts/petersen_raw/vehicles/")
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_maintenance_events",
    cluster_by=["vehicle_id"],
    comment="Raw maintenance events from source system"
)
def bronze_maintenance_events():
    """
    Ingest maintenance event records from parquet files.
    
    Clusters by vehicle_id for efficient querying of maintenance history per vehicle.
    Adds ingestion timestamp for lineage tracking.
    """
    return (
        spark.read.format("parquet")
        .load("/Volumes/sandbox_us_west_2/lakefoundry/artifacts/petersen_raw/maintenance_events/")
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_sensor_readings",
    cluster_by=["vehicle_id"],
    comment="Raw sensor readings from source system"
)
def bronze_sensor_readings():
    """
    Ingest sensor reading data from parquet files.
    
    Clusters by vehicle_id for efficient querying of sensor data per vehicle.
    Adds ingestion timestamp for lineage tracking.
    """
    return (
        spark.read.format("parquet")
        .load("/Volumes/sandbox_us_west_2/lakefoundry/artifacts/petersen_raw/sensor_readings/")
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_condition_assessments",
    cluster_by=["vehicle_id"],
    comment="Raw condition assessment records from source system"
)
def bronze_condition_assessments():
    """
    Ingest condition assessment records from parquet files.
    
    Clusters by vehicle_id for efficient querying of assessment history per vehicle.
    Adds ingestion timestamp for lineage tracking.
    """
    return (
        spark.read.format("parquet")
        .load("/Volumes/sandbox_us_west_2/lakefoundry/artifacts/petersen_raw/condition_assessments/")
        .withColumn("_ingested_at", F.current_timestamp())
    )
