"""Generate synthetic data for Petersen Automotive Museum demo."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import os
import sys

# Use Databricks Connect for local execution
try:
    from databricks.connect import DatabricksSession
    # Read credentials from environment or config
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
    
    # Connect with serverless compute
    spark = DatabricksSession.builder.remote(
        host=host,
        token=token,
        serverless=True
    ).getOrCreate()
except Exception as e:
    print(f"Error: Could not create Databricks session: {e}")
    print("This script must run on a Databricks cluster or with Databricks Connect configured.")
    sys.exit(1)

# =============================================================================
# CONFIGURATION - Edit these values
# =============================================================================
CATALOG = "sandbox_us_west_2"
SCHEMA = "lakefoundry"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/artifacts/petersen_raw"

# Data sizes
N_VEHICLES = 400
N_MAINTENANCE_EVENTS = 2000
N_SENSOR_READINGS = 50000
N_CONDITION_ASSESSMENTS = 800

# Date range - last 6 months from today
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

# Manufacturers
MANUFACTURERS = [
    "Porsche", "Ferrari", "Mercedes-Benz", "BMW", "Jaguar",
    "Ford", "Chevrolet", "Lamborghini", "Aston Martin", "Cord", "Other"
]

# Maintenance event types
MAINTENANCE_TYPES = [
    "Engine Tune-Up", "Condition Assessment", "Clutch Replacement",
    "Brake System Overhaul", "Oil Change", "Tire Rotation", "Battery Replacement",
    "Transmission Service", "Suspension Inspection", "Electrical System Check",
    "Paint Touch-Up", "Interior Restoration", "Fuel System Cleaning",
    "Cooling System Flush", "Spark Plug Replacement"
]

# Reproducibility
SEED = 42

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
print(f"Creating catalog/schema/volume if needed...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.artifacts")

print(f"Generating: {N_VEHICLES:,} vehicles, {N_MAINTENANCE_EVENTS:,} maintenance events, {N_SENSOR_READINGS:,} sensor readings, {N_CONDITION_ASSESSMENTS:,} condition assessments")

# =============================================================================
# 1. VEHICLES (Master Table)
# =============================================================================
print("Generating vehicles...")

def generate_vin():
    """Generate a realistic 17-character VIN."""
    # Format: 3 chars (manufacturer), 6 chars (model/variant), 8 chars (serial)
    manufacturer_code = ''.join(np.random.choice(list('ABCDEFGHJKLMNPRSTUVWXYZ'), 3))
    model_code = ''.join(np.random.choice(list('0123456789ABCDEFGHJKLMNPRSTUVWXYZ'), 6))
    serial = ''.join(np.random.choice(list('0123456789ABCDEFGHJKLMNPRSTUVWXYZ'), 8))
    return manufacturer_code + model_code + serial

vehicles_data = []
for i in range(N_VEHICLES):
    manufacturer = np.random.choice(MANUFACTURERS, p=[0.12, 0.10, 0.12, 0.12, 0.08, 0.10, 0.12, 0.08, 0.08, 0.02, 0.06])
    year_probs = np.linspace(0.001, 0.05, 64)
    year_probs = year_probs / year_probs.sum()  # Normalize to sum to 1
    year = np.random.choice(range(1960, 2024), p=year_probs)
    
    vehicles_data.append({
        "vehicle_id": f"VEH-{i:05d}",
        "vin": generate_vin(),
        "manufacturer": manufacturer,
        "model": fake.word().title(),
        "year": int(year),
        "acquisition_date": (END_DATE - timedelta(days=np.random.randint(365, 7300))).strftime("%Y-%m-%d"),
        "display_location": np.random.choice(["Main Gallery", "Restoration Bay", "Storage", "Special Exhibition", "Outdoor Display"]),
        "image_url": f"https://petersen.org/vehicles/veh_{i:05d}.jpg"
    })

vehicles_pdf = pd.DataFrame(vehicles_data)

# Create lookups for foreign keys
vehicle_ids = vehicles_pdf["vehicle_id"].tolist()
vehicle_manufacturer_map = dict(zip(vehicles_pdf["vehicle_id"], vehicles_pdf["manufacturer"]))
vehicle_year_map = dict(zip(vehicles_pdf["vehicle_id"], vehicles_pdf["year"]))

print(f"  Created {len(vehicles_pdf):,} vehicles")

# =============================================================================
# 2. MAINTENANCE EVENTS (References Vehicles)
# =============================================================================
print("Generating maintenance events...")

# Maintenance cost distribution by manufacturer (luxury brands cost more)
manufacturer_cost_factors = {
    "Porsche": 3.5,
    "Ferrari": 4.0,
    "Lamborghini": 3.8,
    "Mercedes-Benz": 2.5,
    "BMW": 2.3,
    "Jaguar": 2.2,
    "Aston Martin": 3.0,
    "Cord": 1.8,
    "Ford": 1.0,
    "Chevrolet": 0.9,
    "Other": 1.2
}

maintenance_data = []
for i in range(N_MAINTENANCE_EVENTS):
    vehicle_id = np.random.choice(vehicle_ids)
    manufacturer = vehicle_manufacturer_map[vehicle_id]
    year = vehicle_year_map[vehicle_id]
    
    # Older vehicles need more maintenance
    age = 2024 - year
    maintenance_frequency_factor = 1.0 + (age / 100.0)
    
    event_type = np.random.choice(MAINTENANCE_TYPES)
    base_cost = np.random.lognormal(4.5, 0.8)  # Log-normal distribution for realistic costs
    cost = round(base_cost * manufacturer_cost_factors[manufacturer] * maintenance_frequency_factor, 2)
    
    # Health score after maintenance (typically improves)
    health_score_after = np.random.randint(60, 100)
    
    maintenance_data.append({
        "maintenance_id": f"MAINT-{i:06d}",
        "vehicle_id": vehicle_id,
        "event_date": (END_DATE - timedelta(days=np.random.randint(0, 180))).strftime("%Y-%m-%d"),
        "event_type": event_type,
        "description": fake.sentence(nb_words=8),
        "cost": cost,
        "health_score_after": health_score_after
    })

maintenance_pdf = pd.DataFrame(maintenance_data)
print(f"  Created {len(maintenance_pdf):,} maintenance events")

# =============================================================================
# 3. SENSOR READINGS (References Vehicles)
# =============================================================================
print("Generating sensor readings...")

sensor_data = []
for i in range(N_SENSOR_READINGS):
    vehicle_id = np.random.choice(vehicle_ids)
    
    # Distribute readings across the 6-month period
    reading_timestamp = END_DATE - timedelta(
        days=np.random.randint(0, 180),
        hours=np.random.randint(0, 24),
        minutes=np.random.randint(0, 60)
    )
    
    # Temperature and humidity with realistic ranges
    temperature_f = np.random.normal(loc=72, scale=15)  # Mean 72°F, std dev 15
    temperature_f = np.clip(temperature_f, 32, 120)  # Realistic range
    
    humidity_pct = np.random.normal(loc=50, scale=20)  # Mean 50%, std dev 20
    humidity_pct = np.clip(humidity_pct, 10, 95)
    
    sensor_data.append({
        "sensor_id": f"SENS-{i:07d}",
        "vehicle_id": vehicle_id,
        "reading_timestamp": reading_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_f": round(temperature_f, 1),
        "humidity_pct": round(humidity_pct, 1),
        "location": np.random.choice(["Main Gallery", "Restoration Bay", "Storage", "Special Exhibition", "Outdoor Display"])
    })

sensor_pdf = pd.DataFrame(sensor_data)
print(f"  Created {len(sensor_pdf):,} sensor readings")

# =============================================================================
# 4. CONDITION ASSESSMENTS (References Vehicles)
# =============================================================================
print("Generating condition assessments...")

assessment_data = []
for i in range(N_CONDITION_ASSESSMENTS):
    vehicle_id = np.random.choice(vehicle_ids)
    year = vehicle_year_map[vehicle_id]
    
    # Older vehicles tend to have lower health scores
    age = 2024 - year
    age_factor = max(0.3, 1.0 - (age / 200.0))
    
    # Health score correlates with age and maintenance
    base_health = np.random.normal(loc=75, scale=15) * age_factor
    health_score = int(np.clip(base_health, 0, 100))
    
    assessment_data.append({
        "assessment_id": f"ASSESS-{i:06d}",
        "vehicle_id": vehicle_id,
        "assessment_date": (END_DATE - timedelta(days=np.random.randint(0, 180))).strftime("%Y-%m-%d"),
        "health_score": health_score,
        "notes": fake.sentence(nb_words=12)
    })

assessment_pdf = pd.DataFrame(assessment_data)
print(f"  Created {len(assessment_pdf):,} condition assessments")

# =============================================================================
# 5. SAVE TO VOLUME
# =============================================================================
print(f"\nSaving to {VOLUME_PATH}...")

spark.createDataFrame(vehicles_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/vehicles")
spark.createDataFrame(maintenance_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/maintenance_events")
spark.createDataFrame(sensor_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/sensor_readings")
spark.createDataFrame(assessment_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/condition_assessments")

print("Done!")

# =============================================================================
# 6. VALIDATION
# =============================================================================
print("\n=== VALIDATION ===")
print(f"Manufacturer distribution:\n{vehicles_pdf['manufacturer'].value_counts().to_string()}")
print(f"\nYear range: {vehicles_pdf['year'].min()} - {vehicles_pdf['year'].max()}")
print(f"Average maintenance cost by manufacturer:")
maintenance_with_mfg = maintenance_pdf.merge(vehicles_pdf[['vehicle_id', 'manufacturer']], on='vehicle_id')
print(maintenance_with_mfg.groupby('manufacturer')['cost'].mean().sort_values(ascending=False).to_string())
print(f"\nAverage health score by year:")
assessment_with_year = assessment_pdf.merge(vehicles_pdf[['vehicle_id', 'year']], on='vehicle_id')
print(assessment_with_year.groupby('year')['health_score'].mean().sort_values(ascending=False).head(10).to_string())
print(f"\nSensor readings date range: {sensor_pdf['reading_timestamp'].min()} - {sensor_pdf['reading_timestamp'].max()}")
print(f"Maintenance events date range: {maintenance_pdf['event_date'].min()} - {maintenance_pdf['event_date'].max()}")
