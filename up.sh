docker-compose -f docker-compose-dwh.yml -f docker-compose-airbyte.yml -f docker-compose-airflow.yml -f docker-compose-superset.yml --env-file .env up -d

# docker-compose -f docker-compose-dwh.yml -f docker-compose-airbyte.yml --env-file .env up -d

# gitlab+deploy-token-638805:yEG7z-xJpDpCfPBXx_5Z