# Bamboo Airflow & dbt Project

## Overview
This project orchestrates data loading and transformation using Apache Airflow and dbt with DuckDB as the backend. It includes:
- Automated CSV ingestion into DuckDB
- Dynamic dbt model execution for each SQL file
- Airflow DAGs for scheduling and dependency management

## Structure
```
bamboo-repo/
  airflow_local/
    dags/
      dbt_dag.py         # Main Airflow DAG
    docker-compose.yaml  # Airflow deployment config
  bamboo_dbt_project/
    models/              # dbt model SQL files
    dbt_project.yml      # dbt project config
    profiles.yaml        # dbt profile config
  scripts/
    csv_load.py          # Script to load CSVs into DuckDB
    csv_load_config.yaml # Config for CSV loading
  logs/                  # Log files
  inbound_data/          # Source CSV files
```

## DBT Models:
- **Architecture**
    - Staging: 
        - Data loaded from raw incrementally
        - Null strings converted to NULL object
        - Timestamps formatted
        - Rows removed where quote_ref is not populated to remove invalid rows
    - Structured:
        - Data split up from staging schema to defined data model
    - Presentation:
        - Data processed to fulfill stakeholder requirements
- **Tests**
    - Data quality tests added in `schema.yml` file for relevant tests, e.g. uniqueness and null checks

## Airflow DAG: `dbt_dag.py`
- **dbt_setup**: Prepares Python virtual environment and installs dbt-duckdb.
- **run_csv_load_script**: Loads CSVs into DuckDB using `csv_load.py`.
- **Dynamic dbt model tasks**: Runs each dbt model SQL file sequentially.
- **Scheduling**: DAG runs daily at 6am.
- **Error Handling**: Retries and failure callbacks for robust execution.

## Usage
1. **Start Airflow**
    - cd into folder with `docker-compose.yaml`
    - run:
   ```
   cd bamboo-repo/airflow_local
   docker compose up -d
   ```
2. **Configure CSV Loading**
   - Edit `scripts/csv_load_config.yaml` to specify source file path and database path in the airflow container and the schema and name of the table to load source data into.
3. **Add dbt Models**
   - Place your SQL models in `bamboo_dbt_project/models/<folder>/`.
4. **Run the DAG**
   - Trigger manually or wait for scheduled run (6am daily).

## Customization
- **Add new models**: Place `.sql` files in the appropriate `models` subfolder.
- **Change schedule**: Edit `schedule_interval` in `dbt_dag.py`.
- **Error notifications**: Enable email in the failure callback in `dbt_dag.py`.

## Requirements
- Docker
- Apache Airflow
- dbt-duckdb
- Python 3.8+

## Troubleshooting
- Check Airflow logs in `airflow_local/logs/` for errors.
- Ensure all paths in configs match your deployment.
- Ensure all filepaths are mounted in the docker container.
- For dependency issues, rebuild the Docker containers.

## Future Improvements
- **Airflow**:
    - Currently dbt tasks run sequentially as there is an issue writing to duckdb databases with multiple jobs at once, look into how to run tasks concurrently, potentially by writing tables to different duckdb files and then combining, or using a different database.
    - Set up notifications for dag failures.
- **DBT**:
    - Rewrite `models/presentation/application_aggs.sql` to properly show the first loan in the series.
- **CSV Load**:
    - Include process to take into account changing schema for raw data.
    - Add a function where files of other file types could also be processed.
- **Tasks to make process production ready**
    - Create multiple environments (prd & dev)
    - Make process to automatically run unit tests before deployment to prd
    - Add metadata cataloging for tracing lineage
    - Look into potential dashboard solutions for stakeholders