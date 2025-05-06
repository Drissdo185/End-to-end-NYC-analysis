# src/scripts/init_airflow.sh
#!/bin/bash

mkdir -p logs/airflow
mkdir -p src/airflow/dags

export AIRFLOW_HOME=$(pwd)/src/airflow

airflow db init


airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

chmod +x src/scripts/init_airflow.sh