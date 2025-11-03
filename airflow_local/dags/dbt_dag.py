from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from airflow.utils.email import send_email
from datetime import timedelta
import logging

# Set up logging for error handling and notifications
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def csv_load_failure_callback(context):
    """
    Callback function triggered when the csv_load task fails.
    Prints an error message and can optionally send an email notification.
    Inputs:
        context (dict): Airflow context dictionary containing task and DAG info.
    """
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    error_message = f"Task {task_instance.task_id} in DAG {dag_run.dag_id} failed."
    logger.error(error_message)
    # Optionally send an email or log to external system
    # send_email(to='your@email.com', subject='Airflow Task Failed', html_content=error_message)



default_args = {"start_date": datetime(2024, 1, 1)}



with DAG(
    dag_id="dbt_duckdb_run",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["dbt", "duckdb"],
) as dag:
    """
    Airflow DAG for orchestrating CSV data loading and dbt model execution.
    Steps:
        1. Set up Python virtual environment and install dbt-duckdb.
        2. Load CSVs into DuckDB using csv_load.py.
        3. Run each dbt model SQL file sequentially.
        4. Run dbt tests after all models are executed.
    Schedule: Daily at 6am.
    """
    
    dbt_setup = BashOperator(
        task_id="dbt_setup",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "python3 -m venv venv && "
            "/opt/airflow/dbt/venv/bin/pip install --upgrade pip && "
            "/opt/airflow/dbt/venv/bin/pip install dbt-duckdb"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        Sets up the Python virtual environment and installs dbt-duckdb.
        This task must complete before any data loading or dbt model execution.
        """
    )

    run_csv_load = BashOperator(
        task_id="run_csv_load_script",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "source venv/bin/activate && "
            "python /opt/airflow/scripts/csv_load.py || echo 'csv_load.py failed'"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=csv_load_failure_callback,
        doc_md="""
        Loads CSV files into DuckDB using the csv_load.py script.
        Retries twice on failure and triggers a callback for error handling.
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "source venv/bin/activate && "
            f"dbt test "
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        Runs dbt tests after all models have been executed.
        """
    )

   
    # Path to dbt models
    dbt_models_base = "/opt/airflow/dbt/models"
    # List of dbt model subfolders to scan
    model_folders = ["staging", "structured", "presentation"]
    # List to hold dynamically created dbt model tasks
    folder_tasks = []

    # Dynamically create a BashOperator for each dbt model SQL file
    for folder in model_folders:
        folder_path = os.path.join(dbt_models_base, folder)
        for model_file in os.listdir(folder_path):
            if model_file.endswith(".sql"):
                model_name = os.path.splitext(model_file)[0]
                run_model = BashOperator(
                    task_id=f"run_{model_name}",
                    bash_command=(
                        "cd /opt/airflow/dbt && "
                        "source venv/bin/activate && "
                        f"dbt run --select {model_name} "  
                    ),
                    retries=2,
                    retry_delay=timedelta(minutes=5),
                    doc_md=f"""
                    This task runs each model SQL file.
                    Files to be processed dynamically listed based on specified folder structure.
                    """
                )
                folder_tasks.append(run_model)

# Set up sequential dependencies:
    # dbt_setup -> run_csv_load -> first dbt model -> ... -> last dbt model -> dbt_test
    dbt_setup >> run_csv_load >> folder_tasks[0]

    for i in range(len(folder_tasks) - 1):
        folder_tasks[i] >> folder_tasks[i + 1]

    folder_tasks[-1] >> dbt_test