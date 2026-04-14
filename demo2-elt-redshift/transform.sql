-- Create raw table
CREATE TABLE orders_raw (
  order_id INT,
  customer_id INT,
  price FLOAT,
  quantity FLOAT
);

-- Transform inside warehouse
CREATE TABLE orders_transformed AS
SELECT
  customer_id,
  COALESCE(quantity, 1) * price AS total_price
FROM orders_raw;
