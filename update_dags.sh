rm dshop_bu_dag.zip
rm out_of_stock_dag.zip

zip -r dshop_bu_dag.zip dshop_bu_dag.py operators dwh
zip -r out_of_stock_dag.zip out_of_stock_dag.py data_loader operators dwh

rm ~/airflow/dags/dshop_bu_dag.zip
rm ~/airflow/dags/out_of_stock_dag.zip

cp dshop_bu_dag.zip  ~/airflow/dags/
cp out_of_stock_dag.zip  ~/airflow/dags/

airflow dags list

