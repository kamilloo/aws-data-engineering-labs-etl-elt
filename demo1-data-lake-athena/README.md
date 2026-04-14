# Demo 1 — Query Raw Data from Data Lake

## Steps

1. Upload data to S3 (data/raw/)
2. Open Athena
3. Create table:

CREATE EXTERNAL TABLE orders_raw (
  order_id INT,
  customer STRING,
  price DOUBLE,
  quantity DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://<your-bucket>/data/raw/';

4. Query raw data:

SELECT * FROM orders_raw;

5. Transform on the fly:

SELECT 
  customer,
  price * COALESCE(quantity, 1) AS total_price
FROM orders_raw;
