#Install Python Reqs
pip3 install -r requirements.txt

#Create DWH structure
psql -d postgres -h localhost -p 5433 -U gpuser -f ./dwh/init_dwh.sql

#copy JDBC driver
mkdir ~/jdbc
cp ./jdbc/postgresql-42.2.23.jar ~/jdbc/postgresql-42.2.23.jar

#Install Airflow DAGs
mkdir ~/airflow/dags
./update_dags.sh
