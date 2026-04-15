# Demo 2 — ETL (Glue Pipeline)

## Steps

1. Upload data to S3 (Separate your data in Amazon S3)

```
/data/raw/orders/orders.csv
/data/raw/customers/customers.csv
```

2. Go to AWS Glue → ETL Jobs
3. Add Glue Job
   - Use Script etl_script.py, 
   - Job type: Spark
   - set name
   - set IAM Role
   - save
4. Run & wait
5. Open Athena and Set query result location (S3 bucket) if needed
6. Create tables:

```sql
CREATE EXTERNAL TABLE final_orders (
  order_id INT,
  customer_id INT,
  product STRING,
  quantity INT,
  price DOUBLE,
  date DATE,
  total_price DOUBLE,
  name STRING,
  country STRING
)
STORED AS PARQUET
LOCATION 's3://etl-elt-lab-student1/data/processed/final/';
```

7. Query final results:
```SQL
SELECT * FROM final_orders LIMIT 10;
```
