from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from crawl_post_cv import cvpost

date = days_ago(1)
parent_dir = "/opt/airflow/dags/crawl_post_cv"

with DAG(
        dag_id="crawl_cv_daily_elt_dag",
        start_date=days_ago(1),
        schedule_interval='0 20 * * *',
        catchup=False,
    ) as dag:

    crawl_cv = BashOperator(
        task_id = "crawl_cv",
        bash_command= f"xvfb-run --auto-servernum --server-num=1 --server-args='-screen 0, 1920x1080x24' python {parent_dir}/cvcrawl.py"
    )
    
    post_cv = PythonOperator(
        task_id = "post_cv",
        python_callable= cvpost.readfilecsv,
        op_kwargs= {
            "file" : f"{parent_dir}/cv.csv"
        },
    )
    
    crawl_cv >> post_cv
    
if __name__ == "__main__":
    dag.cli()