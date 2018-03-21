CREATE DATABASE project;
USE project;

CREATE EXTERNAL TABLE daily_revenue(
order_date string,
daily_revenue double)
STORED AS parquet
LOCATION '/project/output/daily_revenue';