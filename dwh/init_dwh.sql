DROP DATABASE rd_dwh;

CREATE DATABASE rd_dwh ENCODING 'UTF8';

ALTER DATABASE rd_dwh OWNER TO gpuser;

\connect rd_dwh


CREATE TABLE dim_date(
	action_date date PRIMARY KEY,
	action_week int not NULL,
	action_month int not NULL,
	action_year int not NULL,
	action_weekday varchar(3) not NULL
);

CREATE TABLE dim_products(
	product_id SERIAL PRIMARY KEY,
	product_name int,
	department_name varchar(127),
	aisle varchar(127)
);

CREATE TABLE dim_area(
	area_id SERIAL PRIMARY KEY,
	area varchar(64)
);

CREATE TABLE dim_clients(
	client_id SERIAL PRIMARY KEY,
	fullname varchar(64),
	area_id int
);

CREATE TABLE dim_stores(
	store_id SERIAL PRIMARY KEY,
	area_id int,
	type varchar(64)
);

CREATE TABLE fact_orders(
	product_id int,
	client_id int,
	store_id int,
	order_date date,
	quantity int,
	CONSTRAINT pkey_orders PRIMARY KEY(product_id, client_id, store_id, order_date)
);

CREATE TABLE fact_out_of_stock(
	product_id int,
	oos_date date,
	CONSTRAINT pkey_oos PRIMARY KEY(product_id, oos_date)
);
