from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt.operators.dbt_operator import (
    DbtSnapshotOperator,
)

default_args = {
  'dir': '/opt/airflow/dbt',
  'profiles_dir': '/opt/airflow/dbt',
  'start_date': days_ago(3)
}

with DAG(dag_id='erp_odin_etl_monthly',
         default_args=default_args,
         schedule_interval='0 0 1 * *',
    ) as dag:

    dbt_snapshot = DbtSnapshotOperator(
        task_id='dbt_snapshot',
    )

    dbt_snapshot


if __name__ == "__main__":
    dag.cli()
