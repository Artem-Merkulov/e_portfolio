from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
import os

profile_config = ProfileConfig(
    profile_name="AutoDV",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_local",
        profile_args={"schema": "a_merkulov_stg"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/AutoDV",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2004, 8, 30),
    catchup=True,
    dag_id="autodv-dag",
    default_args={"retries": 1},
    operator_args={
        "vars": f'{{load_dt: "{{{{ ds }}}}", load_src: "1C"}}'
    },
    max_active_runs=1,
    render_config=RenderConfig(emit_datasets=False),
    tags=["autodv", "postgres", "run"]
)