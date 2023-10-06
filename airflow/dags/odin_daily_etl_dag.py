from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtDepsOperator,
    DbtRunOperator,
    DbtTestOperator
)
from airflow.operators.python import PythonOperator
from send_email.send_mail import *

default_args = {
  'dir': '/opt/airflow/dbt',
  'profiles_dir': '/opt/airflow/dbt',
  'start_date': days_ago(3)
}



with DAG(dag_id='erp_odin_etl_daily',
         default_args=default_args,
         schedule_interval='0 20 * * *',
    ) as dag:

    load_hrm_data = AirbyteTriggerSyncOperator(
        task_id='load_hrm_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='337842d5-2ca8-4f59-8080-7c2c731a9f5e',
    )

    load_komu_data = AirbyteTriggerSyncOperator(
        task_id='load_komu_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='34a792f7-ff32-4bda-87b4-ff7fc4a6f0ad',
    )

    load_timesheet_data = AirbyteTriggerSyncOperator(
        task_id='load_timesheet_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='ec235441-3d03-4e33-9ad9-d3cf92a850cb',
    )

    load_project_data = AirbyteTriggerSyncOperator(
        task_id='load_project_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='2209f603-f84a-4061-8b9c-93e4609a7382',
    )

    load_crm_data = AirbyteTriggerSyncOperator(
        task_id='load_crm_data',
        airbyte_conn_id='odin-airbyte-dev',
        connection_id='f87e1a8c-17d5-428f-8773-fcabdca4f8dd',
    )

    validate_raw_data = DbtTestOperator(
        task_id='validate_raw_data',
        select='source:*',
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )
    
    dbt_deps = DbtDepsOperator(
        task_id='dbt_deps',
    )

    dbt_seed = DbtSeedOperator(
        task_id='dbt_seed',
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        exclude="models.komu.*"
    )

    dbt_test = DbtTestOperator(
        task_id='dbt_test',
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    send_email = PythonOperator(
        task_id = 'send_email', 
        python_callable = send_email_after_dbt_test,
        op_kwargs= {
            "sender_email" : "@ncc.asia'",
            "password" : "password",
            "receiver_email" : "linh.nguyen@ncc.asia"
        },    
    )

    load_komu_data >> validate_raw_data

    load_hrm_data >> validate_raw_data

    load_timesheet_data >> validate_raw_data

    load_project_data >> validate_raw_data

    load_crm_data >> validate_raw_data

    validate_raw_data >> dbt_run  >> dbt_test >> send_email

    dbt_deps
        
if __name__ == "__main__":
    dag.cli()
