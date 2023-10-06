export DWH_POSTGRES_HOST='172.16.100.157'
export DWH_POSTGRES_ADMIN=admin
export DWH_POSTGRES_PASSWORD=admin
export DWH_POSTGRES_DB=testdb
export DWH_POSTGRES_PRS_SCHEMA=public

# Create tables and views
dbt run --project-dir ./ --profiles-dir ./ --full-refresh