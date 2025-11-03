
"""
csv_load.py
-------------
Script to load CSV files into DuckDB tables with configuration from a YAML file.
Handles schema creation, table creation, and appending data.
Includes error handling and logging.
"""

import duckdb
import yaml
import logging


# Set up logger for error handling and info messages
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def open_file(file_path):
    """
    Opens and reads the contents of a file.
    Args:
        file_path (str): Path to the file to open.
    Returns:
        Contents of the file.
    Raises:
        Exception: If the file cannot be opened.
    """
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        logger.error(f"Error opening file {file_path}: {e}")
        raise

def load_config(config_filepath):
    """
    Loads YAML configuration from the given file path.
    Args:
        config_filepath (str): Path to YAML config file.
    Returns:
        dict: Parsed configuration.
    Raises:
        Exception: If loading or parsing fails.
    """
    try:
        file = open_file(config_filepath)
        return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise


def load_csv_to_duckdb(db_path, schema, csv_path, table_name):
    """
    Loads a CSV file into a DuckDB table, creating the schema and table if needed.
    Appends data if the table already exists.
    Args:
        db_path (str): Path to DuckDB database file.
        schema (str): Schema name.
        csv_path (str): Path to CSV file.
        table_name (str): Table name.
    Raises:
        Exception: If any DuckDB operation fails.
    """
    try:
        con = duckdb.connect(f'{db_path}')
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        table_exists = con.execute(f'''
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        ''').fetchone()[0]
        logger.info(f"Table exists: {table_exists}")
        if not table_exists:
            logger.info(f"Creating table '{schema}.{table_name}'")
            con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT *, current_timestamp as load_timestamp FROM read_csv_auto('{csv_path}')")
        else:
            logger.info(f"Appending data into '{schema}.{table_name}'")
            con.execute(f"INSERT INTO {schema}.{table_name} SELECT *, current_timestamp as load_timestamp FROM read_csv_auto('{csv_path}')")
    except Exception as e:
        logger.error(f"Error loading CSV to DuckDB for table {table_name}: {e}")
        raise


def process_config(config):
    """
    Processes the config dictionary and loads each CSV into DuckDB.
    Args:
        config (dict): Configuration dictionary.
    Raises:
        Exception: If any step fails.
    """
    try:
        keys = config.keys()
        for file in keys:
            db_path = config[file]['DATABASE_PATH']
            schema = config[file]['SCHEMA']
            csv_path = config[file]['FILE_PATH']
            table_name = config[file]['TABLE_NAME']
            load_csv_to_duckdb(db_path, schema, csv_path, table_name)
    except Exception as e:
        logger.error(f"Error in main CSV load loop: {e}")
        raise


# Main script execution: load config and process CSVs
if __name__ == "__main__":
    config_filepath = '/opt/airflow/scripts/csv_load_config.yaml'
    config = load_config(config_filepath)
    process_config(config)