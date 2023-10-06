export DWH_POSTGRES_HOST=172.16.100.157
export DWH_POSTGRES_ADMIN=admin
export DWH_POSTGRES_PASSWORD=admin
export DWH_POSTGRES_DB=testdb
export DWH_POSTGRES_PRS_SCHEMA=stg

# Create tables and views
dbt compile --project-dir ./ --profiles-dir ./