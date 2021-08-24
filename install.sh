#Install Python Reqs
pip3 install -r requirements.txt

#Create DWH structure
psql -d postgres -h localhost -p 5433 -U gpuser -f ./dwh/init_dwh.sql

#copy JDBC driver
mkdir ~/jdbc
cp ./jdbc/postgresql-42.2.23.jar /home/user/jdbc/postgresql-42.2.23.jar

#setup config file
cp ./rd_etl_config.yml /home/user/rd_etl_config.yml
export RD_ETL_CONFIG=/home/user/rd_etl_config.yml

#create airflow connections
airflow connections add \
	--conn-host '127.0.0.1' \
	--conn-login 'gpuser' \
	--conn-password 'secret' \
	--conn-port 5433 \
	--conn-schema 'rd_dwh' \
	--conn-type 'postgres' \
    'dwh_connection'
	
airflow connections add \
	--conn-login 'user' \
	--conn-type 'hdfs' \
	--conn-host '127.0.0.1' \
	--conn-port 50070 \
	'hadoop_connection'            
	
airflow connections add \
	--conn-type 'http' \
	--conn-login 'rd_dreams' \
	--conn-password 'djT6LasE' \
	--conn-host 'https://robot-dreams-de-api.herokuapp.com' \
	'out_of_stock_connection'
	
airflow connections add \
	--conn-host '127.0.0.1' \
	--conn-login 'pguser' \
	--conn-password 'secret' \
	--conn-schema 'dshop_bu' \
	--conn-type 'postgres' \
	--conn-port 5432 \
	'postgres_dshop_bu_connection' 

#Install Airflow DAGs
mkdir ~/airflow/dags
./update_dags.sh
