pip3 install -r requirements.txt

psql -d template1 -h localhost -p 5433 -U gpuser -f ./dwh/init_dwh.sql

./update_dags.sh