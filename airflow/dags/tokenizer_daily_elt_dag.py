import logging
import csv
import os
import shutil
from time import sleep
import py_vncorenlp
import phonlp
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

POSTGRES_CONN_ID = "postgres_default"
date = days_ago(1)
top_len = 5
schema = 'public'
table_name = 'top_komu_token'
postaglist = ['N','V','A']

def check_raw_data(data:str):
    if data == '':
        return False
    elif data.startswith("*"):
        return False
    else:
        return True
    
def copy_data(copy_sql):
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    logging.info("Exporting query to file")
    pg_hook.copy_expert(copy_sql, filename="/opt/airflow/temp.csv")
    
def tokenizer_transform_data():
    print("Load vncorenlp")
    rdrsegmenter = py_vncorenlp.VnCoreNLP(annotators=["wseg"], save_dir='/opt/airflow/dags/models/vncorenlp')
    print("Load phonlp")
    model = phonlp.load(save_dir='/opt/airflow/dags/models/phonlp')
    print("Start transform")
    tokens = {}
    
    stopwords = []
    with open("/opt/airflow/dags/models/vietnamese-stopwords.txt","r", encoding="utf8") as file:
        stopwords = [line.replace('\n','').replace(' ', '_') for line in file.readlines()]
    if(len(stopwords)==0):
        print("Could not load vietnamese-stopwords !!")
        
    with open("/opt/airflow/temp.csv", 'r', encoding="utf8") as file:
        csvreader = csv.reader(file)
        for row in csvreader:
            print(row)
            try:
                if (check_raw_data(row[0])):
                    annotator = model.annotate(text = ' '.join(rdrsegmenter.word_segment(row[0])))
                    for i in range(0, len(annotator[0])):
                        wordIFO = ''
                        posTagIFO = ''
                        for j in range(0,len(annotator[0][i])):
                            wordForm = annotator[0][i][j]
                            posTag = ' '.join(annotator[1][i][j])[0]
                            if (posTag == posTagIFO):
                                try:
                                    int(tokens[posTagIFO].get(wordIFO))
                                    tokens[posTagIFO][wordIFO] -= 1
                                    if(tokens[posTagIFO].get(wordIFO) == 0):
                                        tokens[posTagIFO].pop(wordIFO)
                                except Exception:
                                    pass
                                wordForm = wordIFO + '_' + wordForm
                                
                            if(len(wordForm)>2 and (posTag in postaglist)):
                                if(not tokens.get(posTag)):
                                    tokens[posTag] = {}
                                try:
                                    int(tokens[posTag].get(wordForm))
                                    tokens[posTag][wordForm] += 1
                                except Exception:
                                    tokens[posTag][wordForm] = 1
                            wordIFO = wordForm
                            posTagIFO = posTag
            except Exception as e:
                print(e)
                pass
    print(f"Tokens: {tokens}")
    # Token data example
    # tokens = {
    #     "N": {
    #         "Daily": 20,
    #         "Container": 10,
    #         "Docker": 5,
    #         "Example": 2,
    #         "Hihi": 1
    #     }
    # }
    with open("/opt/airflow/temp.csv", "w") as file:
        header = ['token','number','postag','time']
        writer = csv.writer(file)
        writer.writerow(header)
        for posTag in tokens:
            tokens[posTag] = dict(sorted(tokens[posTag].items(), key=lambda item: item[1], reverse=True ))
            i = 0
            for token in tokens[posTag]:
                if(token.lower() not in stopwords):
                    writer.writerow([token, tokens[posTag][token], posTag, date])
                    print(f"Writed {token}, {tokens[posTag][token]}, {posTag}, {date}")
                    i+=1
                    if(i==top_len):
                        break

with DAG(
        dag_id="top_komu_message_daily_elt_dag",
        start_date=days_ago(1),
        schedule_interval='0 21 * * *',
        catchup=False,
    ) as dag:

    get_data = PythonOperator(
        task_id="get_raw_data",
        python_callable=copy_data,
        op_kwargs={
            "copy_sql": f"""
                COPY (
                    SELECT DISTINCT content
                    FROM base.base_komu_msgs msg
                    INNER JOIN base.base_komu_users u ON msg.authorid = u.komu_user_id
                    WHERE is_bot = false AND date(msg.created_time) = '{date}'
                ) TO STDOUT WITH CSV HEADER""" 
        }
    )
    
    transform = PythonOperator(
        task_id='tokenizer_tranform',
        python_callable=tokenizer_transform_data
    )
    
    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=copy_data,
        op_kwargs={
            "copy_sql": f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    "token_id" SERIAL PRIMARY KEY,
                    "token" TEXT NOT NULL,
                    "number" INTEGER NOT NULL,
                    "postag" TEXT NOT NULL,
                    "time" TIMESTAMP
                );
                COPY {schema}.{table_name}(token, number, postag, time) FROM STDIN WITH CSV HEADER"""
        }
    )
    
    get_data >> transform >> insert_data
    
if __name__ == "__main__":
    dag.cli()