rm dshop_dag.zip
rm dshop_bu_dag.zip
rm out_of_stock_dag.zip

zip dshop_dag.zip dshop_dag.py postgres_dump_to_hdfs_operator.py bronze_to_silver_operator.py
zip dshop_bu_dag.zip dshop_bu_dag.py postgres_dump_to_hdfs_operator.py bronze_to_silver_operator.py
zip -r out_of_stock_dag.zip data_loader out_of_stock_dag.py bronze_to_silver_operator.py bronze_to_silver_append_operator.py

rm ~/airflow/dags/dshop_dag.zip
rm ~/airflow/dags/dshop_bu_dag.zip
rm ~/airflow/dags/out_of_stock_dag.zip

cp dshop_dag.zip  ~/airflow/dags/
cp dshop_bu_dag.zip  ~/airflow/dags/
cp out_of_stock_dag.zip  ~/airflow/dags/

airflow dags list

