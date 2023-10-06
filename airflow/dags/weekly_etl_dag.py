import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtDepsOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator
)

default_args = {
  'dir': '/opt/airflow/dbt',
  'profiles_dir': '/opt/airflow/dbt',
  'start_date': datetime.datetime(2023,1,29)
}

with DAG(dag_id='erp_odin_etl_weekly',
         default_args=default_args,
         schedule_interval='0 0 * * 0',
    ) as dag:

    load_talent_data = AirbyteTriggerSyncOperator(
        task_id='load_talent_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='4277886e-7b26-4020-ab0e-11ab8d7f0f59',
    )

    load_lms_data = AirbyteTriggerSyncOperator(
            task_id='load_lms_data',
            airbyte_conn_id='odin-airbyte-dev',
            connection_id='3f122626-d911-4efe-8c0c-d1f59fb057c1',
        )

    load_talent_data

    load_lms_data

if __name__ == "__main__":
    dag.cli()
