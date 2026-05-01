# Petersen Automotive Demo Implementation Plan

**Goal:** Build an analytics pipeline and interactive dashboard for the Petersen Automotive Museum's vehicle collection health monitoring system.

**Architecture:** Synthetic data generation creates raw vehicle, maintenance, and sensor data in a UC Volume. A Spark Declarative Pipeline (SDP) processes this through bronze/silver/gold medallion layers. An APX React application provides an interactive dashboard matching the mockup, querying gold-layer tables via FastAPI backend.

---

### Task 1: Generate Synthetic Data

**Depends On:** none

**Files:**
- Create: `scripts/generate_petersen_data.py`

**Requirements:**
- Generate 6 months of data ending at current date
- Create 4 entity types:
  - **vehicles**: ~400 vehicles with VIN, manufacturer, model, year, acquisition_date, display_location, image_url
  - **maintenance_events**: ~2000 events with vehicle_id, event_date, event_type (Engine Tune-Up, Condition Assessment, Clutch Replacement, Brake System Overhaul, Oil Change, etc.), description, cost, health_score_after
  - **sensor_readings**: ~50,000 readings with vehicle_id, reading_timestamp, temperature_f, humidity_pct, location
  - **condition_assessments**: ~800 assessments with vehicle_id, assessment_date, health_score (0-100), notes
- Manufacturers: Porsche, Ferrari, Mercedes-Benz, BMW, Jaguar, Ford, Chevrolet, Lamborghini, Aston Martin, Cord, plus "Other"
- Use realistic distributions (Porsche has highest maintenance costs, health scores vary by age/maintenance)
- Save as parquet files to `/Volumes/sandbox_us_west_2/lakefoundry/artifacts/petersen_raw/`

**Acceptance Criteria:**
- [ ] Script executes without errors on Databricks
- [ ] All 4 entity parquet folders created in volume
- [ ] Data spans last 6 months with realistic distributions

**Skills:** `synthetic-data-generation`

---

### Task 2: Create Bronze Layer Pipeline

**Depends On:** none

**Files:**
- Create: `src/petersen_pipeline/bronze.py`

**Requirements:**
- Use `pyspark.pipelines` (dp) API with `@dp.table()` decorators
- Ingest from Volume parquet files using `read_files()` or Auto Loader
- Create 4 streaming tables: `bronze_vehicles`, `bronze_maintenance_events`, `bronze_sensor_readings`, `bronze_condition_assessments`
- Add `_ingested_at` timestamp metadata column
- Use `cluster_by` for efficient querying (vehicle_id for events/readings)

**Acceptance Criteria:**
- [ ] All 4 bronze tables defined with proper schemas
- [ ] Ingestion metadata columns added
- [ ] Clustering keys specified

**Skills:** `spark-declarative-pipelines`

---

### Task 3: Create Silver Layer Pipeline

**Depends On:** 2

**Files:**
- Create: `src/petersen_pipeline/silver.py`

**Requirements:**
- Clean and validate bronze data
- Create streaming tables: `silver_vehicles`, `silver_maintenance_events`, `silver_sensor_readings`, `silver_condition_assessments`
- Add data quality expectations (valid health scores 0-100, valid dates, non-null required fields)
- Standardize manufacturer names (handle case variations)
- Cast types appropriately (dates, decimals for costs)

**Acceptance Criteria:**
- [ ] All 4 silver tables with validated data
- [ ] Data quality expectations defined
- [ ] Type casting and standardization applied

**Skills:** `spark-declarative-pipelines`

---

### Task 4: Create Gold Layer Pipeline

**Depends On:** 3

**Files:**
- Create: `src/petersen_pipeline/gold.py`

**Requirements:**
- Create materialized views for dashboard consumption:
  - `gold_vehicle_health_current`: Latest health score per vehicle with manufacturer, model, year
  - `gold_collection_health_summary`: Overall collection health score, vehicles at risk count, upcoming/overdue maintenance counts
  - `gold_health_trend_by_manufacturer`: Monthly average health score by manufacturer
  - `gold_maintenance_cost_by_manufacturer`: Average maintenance cost by manufacturer
  - `gold_vehicle_maintenance_timeline`: Maintenance events per vehicle for timeline view
  - `gold_sensor_data_daily`: Daily aggregated sensor readings per vehicle
- Use appropriate clustering for query patterns

**Acceptance Criteria:**
- [ ] All 6 gold materialized views created
- [ ] Aggregations match dashboard requirements
- [ ] Query-optimized with clustering

**Skills:** `spark-declarative-pipelines`

---

### Task 5: Deploy Pipeline with DAB

**Depends On:** 2, 3, 4

**Files:**
- Create: `databricks.yml`
- Create: `resources/petersen_pipeline.yml`

**Requirements:**
- Create DAB bundle configuration
- Configure pipeline with catalog `sandbox_us_west_2`, schema `lakefoundry`
- Include all pipeline source files (bronze.py, silver.py, gold.py)
- Set serverless compute
- Validate and deploy the bundle
- Run the pipeline to populate tables

**Acceptance Criteria:**
- [ ] `databricks bundle validate` succeeds
- [ ] `databricks bundle deploy` succeeds
- [ ] `databricks bundle run petersen_pipeline` completes successfully
- [ ] All bronze/silver/gold tables populated with data

**Skills:** `asset-bundles`, `spark-declarative-pipelines`

---

### Task 6: Create APX Application Backend

**Depends On:** 4

**Files:**
- Create: `src/petersen_app/backend/models.py`
- Create: `src/petersen_app/backend/router.py`

**Requirements:**
- Initialize APX project with `apx init`
- Define Pydantic models for all dashboard data types:
  - `CollectionHealthSummary`: overall_score, vehicles_at_risk, upcoming_maintenance, overdue_maintenance, total_vehicles
  - `VehicleHealth`: vehicle_id, vin, manufacturer, model, year, health_score, trend, image_url
  - `HealthTrendPoint`: date, score, manufacturer
  - `MaintenanceCostByManufacturer`: manufacturer, avg_cost
  - `MaintenanceEvent`: event_id, vehicle_id, event_date, event_type, description, cost, health_score_after
  - `SensorReading`: timestamp, temperature_f, humidity_pct
- Create API routes with proper `operation_id` naming:
  - `GET /api/collection-health` → `getCollectionHealth`
  - `GET /api/vehicles` → `listVehicles`
  - `GET /api/vehicles/{vehicle_id}` → `getVehicle`
  - `GET /api/vehicles/{vehicle_id}/maintenance` → `getVehicleMaintenanceTimeline`
  - `GET /api/vehicles/{vehicle_id}/sensors` → `getVehicleSensorData`
  - `GET /api/health-trend` → `getHealthTrend` (with manufacturer filter)
  - `GET /api/maintenance-costs` → `getMaintenanceCostsByManufacturer`
  - `GET /api/manufacturers` → `listManufacturers`
- Query gold tables using Databricks SQL connector

**Acceptance Criteria:**
- [ ] APX project initialized
- [ ] All models defined with proper types
- [ ] All routes return data from gold tables
- [ ] `apx dev check` passes

**Skills:** `databricks-app-apx`

---

### Task 7: Create APX Application Frontend

**Depends On:** 6

**Files:**
- Create: `src/petersen_app/ui/routes/_sidebar/overview.tsx`
- Create: `src/petersen_app/ui/routes/_sidebar/vehicles.tsx`
- Create: `src/petersen_app/ui/routes/_sidebar/vehicles.$vehicleId.tsx`
- Modify: `src/petersen_app/ui/routes/_sidebar/route.tsx`

**Requirements:**
- Match the UI mockup design:
  - Dark theme with red accent (#E53935 for Petersen branding)
  - Header with "PETERSEN AUTOMOTIVE MUSEUM" logo, "COLLECTION HEALTH DASHBOARD", weather widget, last updated timestamp
  - Sidebar navigation: Overview, Vehicles, Maintenance, Analytics, Alerts, Reports, Settings
- Overview page components:
  - KPI cards row: Overall Collection Health Score (gauge), Vehicles at Risk, Upcoming Maintenance, Maintenance Overdue, Total Vehicles
  - Manufacturer filter: Horizontal scrollable list with manufacturer icons/names, selection state
  - Vehicle Health Trend: Line chart (recharts) with time range selector
  - Maintenance Event Timeline: Horizontal timeline for selected vehicle
  - Sensor Data Overlay: Dual-axis chart for temperature/humidity
  - Average Maintenance Cost by Manufacturer: Bar chart
  - Vehicle Spotlight: Featured vehicle card with image, VIN, health score, trend
- Use Suspense boundaries with skeleton fallbacks
- Install required shadcn components (card, badge, select, skeleton, tabs)
- Install recharts for charts

**Acceptance Criteria:**
- [ ] Dashboard matches mockup layout and styling
- [ ] All charts render with real data
- [ ] Manufacturer filter updates all relevant components
- [ ] Loading skeletons display during data fetch
- [ ] `apx dev check` passes

**Skills:** `databricks-app-apx`, `web-design-guidelines`, `vercel-react-best-practices`

---

### Task 8: Deploy APX Application

**Depends On:** 7

**Files:**
- Create: `resources/petersen_app.yml`
- Create: `src/petersen_app/app.yaml`

**Requirements:**
- Add APX app resource to DAB bundle
- Configure environment variables in app.yaml:
  - `DATABRICKS_WAREHOUSE_ID`
  - `DATABRICKS_CATALOG`: sandbox_us_west_2
  - `DATABRICKS_SCHEMA`: lakefoundry
- Deploy with `databricks bundle deploy`
- Start app with `databricks bundle run petersen_app`
- Verify app is accessible and displays data

**Acceptance Criteria:**
- [ ] App resource added to bundle
- [ ] `databricks bundle deploy` succeeds
- [ ] `databricks bundle run petersen_app` starts the app
- [ ] App URL accessible and displays dashboard with live data

**Skills:** `asset-bundles`, `databricks-app-apx`
