import requests
import json
import csv
import os
# defining the api-endpoint 
API_ENDPOINT = "http://stg-api-talent.nccsoft.vn/api/services/app/Public/CreateExternalCV"
# your API key here
API_KEY = "12345678"
# headers API
headers =  {
    "X-Secret-Key":API_KEY
    }
extercv = {
        'Name':'',
        'Email':'',
        'Phone':'',
        'IsFemale':'',
        'UserTypeName':'',
        'PositionName':'',
        'CVSourceName':'TopCV',
        'BranchName':'',
        'NCCEmail':'',
        'Birthday':'',
        'ReferencName':'',
        'Note':'',
        'CV':'',
        'Avatar':'',
        'Metadata':'',
        'ExternalId':''
        }
# Parent_dir
parent_dir = '/opt/airflow/dags/crawl_post_cv/'
# data to be sent to api
def postdata(data:dict,flag:int):
    if flag == 1:
        with open(parent_dir + 'store/'+ data['ExternalId']+'.pdf','rb') as f:
            file = {'CV':f}
            r = requests.post(url = API_ENDPOINT, data=data,files=file,headers=headers)
            pastebin_url = r.text
            print("The pastebin URL is:%s"%pastebin_url)
    else:
        r = requests.post(url = API_ENDPOINT, data=data,headers=headers)
        pastebin_url = r.text
        print("The pastebin URL is:%s"%pastebin_url)

# READ FILE CSV TO POST CV
def readfilecsv(file:str):
    try:
        with open(file,"r",encoding='utf-8') as f:
            der = csv.reader(f)
            rows = []
            for row in der:
                if row:
                    rows.append(row)
            for row in rows:
                ob = extercv
                flag = 0
                ob['ExternalId']= row[0]
                ob['Name']= row[1]
                ob['PositionName']= row[2]
                ob['Metadata']= json.dumps(row[4], indent = 4) 
                if len(row[7])>100:
                    flag=1
                else:
                    flag=0
                postdata(ob,flag)
    except Exception:
        pass

    

