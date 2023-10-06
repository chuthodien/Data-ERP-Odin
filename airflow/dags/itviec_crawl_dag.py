from datetime import datetime
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from crawl_post_cv.crawl_itviec import *

schema = 'public'
table_name = 'job_itviec'
POSTGRES_CONN_ID = "postgres_default"
parent_dir = "/opt/airflow/dags/crawl_post_cv"

def copy_data(copy_sql):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    logging.info("Exporting query to file")
    pg_hook.copy_expert(copy_sql, filename="/opt/airflow/dags/crawl_post_cv/job_itviec.csv")

with DAG(
    dag_id='itviec_crawl_daily_elt_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=2, day=1),
    catchup=False
) as dag:
    
    crawl_job = BashOperator(
        task_id="crawl_job",
        bash_command= f"xvfb-run --auto-servernum --server-num=1 --server-args='-screen 0, 1920x1080x24' python {parent_dir}/crawl_itviec.py",
    )
    
    insert_job = PythonOperator(
        task_id="insert_data",
        python_callable=copy_data,
        op_kwargs={
            "copy_sql": f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    "id" SERIAL PRIMARY KEY,
                    "title" TEXT NOT NULL,
                    "skill" TEXT ,
                    "address" TEXT,
                    "salary" TEXT,
                    "type_work" TEXT,
                    "date_post" TEXT,
                    "company_name" TEXT,
                    "company_text" TEXT,
                    "type_company" TEXT,
                    "member_company" TEXT,
                    "day_work" TEXT,
                    "country_company" TEXT,
                    "OT" TEXT,
                    "reason_choice" TEXT,
                    "job_description" TEXT,
                    "skills_and_experience" TEXT,
                    "detail_benefit" TEXT,
                    "time" TIMESTAMP
                );
                COPY {schema}.{table_name}(
                    "title",
                    "skill",
                    "address",
                    "salary",
                    "type_work",
                    "date_post",
                    "company_name",
                    "company_text",
                    "type_company",
                    "member_company",
                    "day_work",
                    "country_company",
                    "OT",
                    "reason_choice",
                    "job_description",
                    "skills_and_experience",
                    "detail_benefit") FROM STDIN WITH CSV HEADER"""
        }
    )
    crawl_job >> insert_job
    
if __name__ == "__main__":
    dag.cli()
