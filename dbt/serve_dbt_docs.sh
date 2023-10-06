export DWH_POSTGRES_HOST=172.16.100.157
export DWH_POSTGRES_ADMIN=admin
export DWH_POSTGRES_PASSWORD=admin
export DWH_POSTGRES_DB=testdb
export DWH_POSTGRES_PRS_SCHEMA=public

# Serve docs on http://localhost:4444
dbt docs serve --port 4444 --project-dir .  --profiles-dir .
