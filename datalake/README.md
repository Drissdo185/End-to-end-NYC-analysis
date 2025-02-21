# NYC Taxi Data Lake Setup and Usage Guide

## Infrastructure Components

Our data lake setup consists of the following components:

### 1. Trino (Port 8080)
- Distributed SQL query engine
- Version: 410
- Used for querying data across different sources
- Container name: datalake-trino

### 2. Hive Metastore (Port 9083)
- Metadata repository service
- Version: 3.1.2
- Container name: datalake-hive-metastore
- Configured with PostgreSQL backend

### 3. PostgreSQL Metastore DB (Port 5433)
- Backend database for Hive metastore
- Version: 11
- Container name: datalake-metastore-db
- Credentials:
  - Database: metastore
  - Username: hive
  - Password: hive

### 4. MinIO Object Storage (Ports 9000, 9001)
- S3-compatible object storage
- Container name: datalake-minio
- Stores Parquet files
- Credentials:
  - Access key: minio_access_key
  - Secret key: minio_secret_key

## Setup Instructions

### 1. Accessing Trino CLI

```bash
# Connect to Trino container
docker exec -ti datalake-trino bash

# Start Trino CLI
trino
```

### 2. Creating Schemas and Tables

First, create the bronze and silver schemas:

```sql
-- Create schemas for bronze and silver layers
CREATE SCHEMA IF NOT EXISTS datalake.bronze
WITH (location = 's3://bronze/');

CREATE SCHEMA IF NOT EXISTS datalake.silver
WITH (location = 's3://silver/');
```

Then, create the taxi trips tables in both layers:

```sql
-- Create table in bronze layer (raw data)
CREATE TABLE datalake.bronze.taxi_trips (
  dolocationid INT,
  dropoff_datetime TIMESTAMP,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  pickup_datetime TIMESTAMP,
  pulocationid INT,
  ratecodeid DOUBLE,
  store_and_fwd_flag VARCHAR(30),
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  trip_distance DOUBLE,
  vendorid INT
) WITH (
  external_location = 's3://bronze/raw',
  format = 'PARQUET'
);

-- Create table in silver layer (processed data)
CREATE TABLE datalake.silver.taxi_trips (
  dolocationid INT,
  dropoff_datetime TIMESTAMP,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  pickup_datetime TIMESTAMP,
  pulocationid INT,
  ratecodeid DOUBLE,
  store_and_fwd_flag VARCHAR(30),
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  trip_distance DOUBLE,
  vendorid INT,
  -- Additional derived columns for silver layer
  trip_duration_minutes DOUBLE,
  cost_per_mile DOUBLE,
  day_of_week VARCHAR(10),
  hour_of_day INT
) WITH (
  external_location = 's3://silver/processed',
  format = 'PARQUET'
);
```

## Data Lake Architecture

Our data lake follows a multi-layer architecture:

### Bronze Layer
- Raw data storage
- Minimal transformations
- Complete history preservation
- Location: s3://bronze/raw

### Silver Layer
- Cleaned and processed data
- Additional derived columns
- Business logic applications
- Location: s3://silver/processed

## Example Queries

### 1. Basic Statistics

Get overall statistics for trips and fares:

```sql
SELECT 
    COUNT(*) as total_trips,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare,
    AVG(tip_amount) as avg_tip
FROM datalake.silver.taxi_trips;
```

### 2. Hourly Trip Analysis

Analyze trip patterns by hour:

```sql
SELECT 
    EXTRACT(HOUR FROM pickup_datetime) as hour_of_day,
    COUNT(*) as num_trips,
    AVG(total_amount) as avg_fare
FROM datalake.silver.taxi_trips
GROUP BY EXTRACT(HOUR FROM pickup_datetime)
ORDER BY hour_of_day;
```

## Data Dictionary

Table: `taxi_trips`

| Column Name | Data Type | Description |
|------------|-----------|-------------|
| dolocationid | INT | Drop-off location ID |
| dropoff_datetime | TIMESTAMP | Drop-off date and time |
| fare_amount | DOUBLE | Base fare amount |
| improvement_surcharge | DOUBLE | Improvement surcharge fee |
| mta_tax | DOUBLE | MTA tax amount |
| passenger_count | INT | Number of passengers |
| payment_type | INT | Payment method code |
| pickup_datetime | TIMESTAMP | Pick-up date and time |
| pulocationid | INT | Pick-up location ID |
| ratecodeid | DOUBLE | Rate code identifier |
| store_and_fwd_flag | VARCHAR(30) | Store and forward flag |
| tip_amount | DOUBLE | Tip amount |
| tolls_amount | DOUBLE | Tolls amount |
| total_amount | DOUBLE | Total fare amount |
| trip_distance | DOUBLE | Trip distance in miles |
| vendorid | INT | Vendor identifier |

## Maintenance and Troubleshooting

### Checking Service Status
```bash
# Check all containers status
docker ps

# Check specific container logs
docker logs datalake-trino
docker logs datalake-hive-metastore
docker logs datalake-metastore-db
docker logs datalake-minio
```

### Common Issues and Solutions

1. If Trino cannot connect to Hive metastore:
   - Check if hive-metastore container is running
   - Verify metastore_db is accessible
   - Check network connectivity between containers

2. If data is not visible in queries:
   - Verify MinIO credentials
   - Check if Parquet files are properly loaded
   - Verify schema and table definitions

3. If queries are slow:
   - Check Trino worker configuration
   - Verify resource allocation
   - Review query optimization settings