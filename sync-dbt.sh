cd orgi-dbt
git pull 
cd ../
docker-compose -f docker-compose-metriql.yml down -v
docker-compose -f docker-compose-metriql.yml up -d