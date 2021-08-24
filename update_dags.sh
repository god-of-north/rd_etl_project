rm /home/user/rd_etl_config.yml
cp ./rd_etl_config.yml /home/user/rd_etl_config.yml

rm dshop_bu_dag.zip
rm out_of_stock_dag.zip
rm append_date_dag.zip

zip -r dshop_bu_dag.zip dshop_bu_dag.py operators dwh tools
zip -r out_of_stock_dag.zip out_of_stock_dag.py data_loader operators dwh tools
zip -r append_date_dag.zip append_date_dag.py dwh tools

rm ~/airflow/dags/dshop_bu_dag.zip
rm ~/airflow/dags/out_of_stock_dag.zip
rm ~/airflow/dags/append_date_dag.zip

cp dshop_bu_dag.zip  ~/airflow/dags/
cp out_of_stock_dag.zip  ~/airflow/dags/
cp append_date_dag.zip ~/airflow/dags/append_date_dag.zip

airflow dags list

