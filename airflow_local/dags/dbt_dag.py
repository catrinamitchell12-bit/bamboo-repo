from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
from airflow.utils.email import send_email
from datetime import timedelta

def csv_load_failure_callback(context):
        task_instance = context.get('task_instance')
        dag_run = context.get('dag_run')
        error_message = f"Task {task_instance.task_id} in DAG {dag_run.dag_id} failed."
        print(error_message)
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
    
    dbt_setup = BashOperator(
        task_id="dbt_setup",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "python3 -m venv venv && "
            "/opt/airflow/dbt/venv/bin/pip install --upgrade pip && "
            "/opt/airflow/dbt/venv/bin/pip install dbt-duckdb"
        ),
    )
    # Task to run csv_load.py script with error handling
    run_csv_load = BashOperator(
        task_id="run_csv_load_script",
        bash_command="python /opt/airflow/scripts/csv_load.py || echo 'csv_load.py failed'",
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=csv_load_failure_callback,
    )
   
    # Path to dbt models
    dbt_models_base = "/opt/airflow/dbt/models"
    # Folders to scan
    model_folders = ["presentation", "staging", "structured"]

    for folder in model_folders:
        group_id = f"{folder}_models"
        folder_path = os.path.join(dbt_models_base, folder)
        with TaskGroup(group_id=group_id, tooltip=f"DBT models in {folder}") as tg:
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
                    )
                    run_csv_load >> dbt_setup >> run_model
