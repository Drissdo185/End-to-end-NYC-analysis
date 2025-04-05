# NYC Taxi Data Lake Documentation

## Overview

This document outlines the setup and configuration of a data lake for NYC taxi trip data using a modern data stack including Trino, MinIO, and Hive Metastore. The system enables efficient querying and analysis of taxi trip data through a distributed SQL query engine.

## Architecture

### Components

- **MinIO**: Object storage service compatible with Amazon S3 API, used for storing raw data
- **Hive Metastore**: Metadata repository for Hadoop/Hive tables
- **Trino**: Distributed SQL query engine for big data
- **Apache Spark**: Data processing engine used for ETL operations

### Data Flow

1. Raw data files (Parquet format) are uploaded to MinIO bronze bucket
2. Spark processes data and stores it in silver bucket
3. Trino provides SQL interface to query data across all buckets

## Infrastructure Setup

### Docker Compose Configuration

The infrastructure is containerized using Docker Compose with the following services:

- **Trino**: Distributed query engine
- **Hive Metastore**: Metadata repository
- **MinIO**: Object storage
- **Postgres**: Database for Hive Metastore

```yaml
version: "3.9"
services:
  trino:
    ports:
      - "8080:8080"
    container_name: datalake-trino
    image: "trinodb/trino:410"
    hostname: trino
    volumes:
      - ./trino/etc:/usr/lib/trino/etc:ro
      - ./trino/catalog:/etc/trino/catalog
    depends_on:
      - hive-metastore

  metastore_db:
    container_name: datalake-metastore-db
    image: postgres:11
    hostname: metastore_db
    ports:
      - '5433:5432'
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore

  hive-metastore:
    container_name: datalake-hive-metastore
    image: 'starburstdata/hive:3.1.2-e.18'
    hostname: hive-metastore
    ports:
      - '9083:9083'
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: jdbc:postgresql://metastore_db:5432/metastore
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      S3_ENDPOINT: http://minio:9000
      S3_ACCESS_KEY: minio_access_key
      S3_SECRET_KEY: minio_secret_key
      S3_PATH_STYLE_ACCESS: "true"
    depends_on:
      - metastore_db
      
  minio:
    image: minio/minio
    container_name: datalake-minio
    hostname: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_storage:/data
    environment:
      MINIO_ACCESS_KEY: minio_access_key
      MINIO_SECRET_KEY: minio_secret_key
    command: server --console-address ":9001" /data
```

## Trino Configuration

### Catalog Configuration

Create a catalog configuration file at `trino/catalog/datalake.properties`:

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083

# Object storage credentials (MinIO)
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minio_access_key
hive.s3.aws-secret-key=minio_secret_key
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.allow-drop-table=true
```

### File Permissions

Ensure proper file permissions:

```bash
chmod 644 trino/catalog/datalake.properties
```

## Data Schema Setup

### Creating Schema

Connect to Trino CLI and create a schema:

```bash
docker exec -ti datalake-trino bash
trino
```

```sql
CREATE SCHEMA IF NOT EXISTS datalake.nyc_taxi 
WITH (location = 's3://bronze/');
```

### Creating Table

Create an external table for NYC taxi data:

```sql
CREATE TABLE IF NOT EXISTS datalake.nyc_taxi.yellow_tripdata (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
)
WITH (
    format = 'PARQUET',
    external_location = 's3://bronze/'
);
```

## Data Processing

### Bronze to Silver

The data processing pipeline uses Spark to transform data from the bronze bucket to the silver bucket:

```python
def clean_data_with_spark(spark_df):
    cleaned_df = spark_df \
        .fillna(1, subset=["passenger_count"]) \
        .fillna(1, subset=["RatecodeID"]) \
        .fillna("N", subset=["store_and_fwd_flag"]) \
        .fillna(0.0, subset=["congestion_surcharge"]) \
        .fillna(0.0, subset=["Airport_fee"])
    
    return cleaned_df
```

## Querying Data

After setting up the schema and tables, you can query the data using standard SQL:

```sql
SELECT 
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    payment_type
FROM 
    datalake.nyc_taxi.yellow_tripdata
GROUP BY 
    payment_type
ORDER BY 
    avg_fare DESC;
```

## Troubleshooting

### Catalog Not Found

If you receive the error `Catalog 'datalake' does not exist`:

1. Verify the catalog configuration file exists and has correct permissions
   ```bash
   ls -la trino/catalog/
   chmod 644 trino/catalog/datalake.properties
   ```

2. Restart the Trino container
   ```bash
   docker restart datalake-trino
   ```

3. Check Trino logs for errors
   ```bash
   docker logs datalake-trino
   ```

4. Verify Hive Metastore connection
   ```bash
   docker ps | grep metastore
   ```

### MinIO Connection Issues

If Trino can't connect to MinIO:

1. Verify MinIO is running
   ```bash
   docker ps | grep minio
   ```

2. Check MinIO logs
   ```bash
   docker logs datalake-minio
   ```

3. Verify bucket existence in MinIO console at `http://localhost:9001`

## Conclusion

This documentation provides a comprehensive guide to setting up and using a data lake for NYC taxi trip data. By following these steps, you can create a scalable and efficient system for analyzing large volumes of taxi trip data using SQL.