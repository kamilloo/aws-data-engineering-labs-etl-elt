# Demo 1 — Query Raw Data from Data Lake

## Steps

1. Upload data to S3 (Separate your data in Amazon S3)

```
/data/raw/orders/orders.csv
/data/raw/customers/customers.csv
```
2. Open Athena and Set query result location (S3 bucket)
3. Create tables:

```sql
CREATE EXTERNAL TABLE orders_raw (
  order_id INT,
  customer_id INT,
  product STRING,
  quantity INT,
  price DOUBLE,
  date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://etl-elt-lab-student1/data/raw/orders/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

```sql
CREATE EXTERNAL TABLE customers_raw (
customer_id INT,
name STRING,
country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://etl-elt-lab-student1/data/raw/customers/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

4. Query raw data:
```SQL
SELECT * FROM orders_raw;
SELECT * FROM customers_raw;
```

5. Transform on the fly:
```SQL
SELECT 
  c.name,
  c.country,
  o.product,
  COALESCE(o.quantity, 1) AS quantity,
  o.price,
  o.price * COALESCE(o.quantity, 1) AS total,
  date_parse(replace(date, '/', '-'), '%Y-%m-%d') AS clean_date
FROM orders_raw o
LEFT JOIN customers_raw c
ON o.customer_id = c.customer_id;
```


