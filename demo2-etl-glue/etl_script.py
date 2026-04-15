from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, to_date

from awsglue.context import GlueContext
from pyspark.context import SparkContext

# -----------------------------
# Init Glue/Spark
# -----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------
# Read ORDERS
# -----------------------------
orders = spark.read.csv(
    "s3://etl-elt-lab-student1/data/raw/orders/",
    header=True,
    inferSchema=True
)

# -----------------------------
# Read CUSTOMERS
# -----------------------------
customers = spark.read.csv(
    "s3://etl-elt-lab-student1/data/raw/customers/",
    header=True,
    inferSchema=True
)

# -----------------------------
# CLEAN ORDERS
# -----------------------------

orders_clean = orders.withColumn(
    "quantity",
    when(col("quantity").isNull(), 1).otherwise(col("quantity"))
)

# Force correct types (IMPORTANT FIX)
orders_clean = orders_clean.withColumn(
    "order_id", col("order_id").cast("int")
).withColumn(
    "customer_id", col("customer_id").cast("int")
).withColumn(
    "quantity", col("quantity").cast("int")
).withColumn(
    "price", col("price").cast("double")   # 🔥 FIX: force DOUBLE
)

# Fix date format (2024/01/03 -> 2024-01-03)
orders_clean = orders_clean.withColumn(
    "date",
    to_date(regexp_replace(col("date"), "/", "-"), "yyyy-MM-dd")
)

# Add derived column
orders_clean = orders_clean.withColumn(
    "total_price",
    col("quantity") * col("price")
)

# -----------------------------
# CLEAN CUSTOMERS
# -----------------------------
customers_clean = customers.withColumn(
    "customer_id",
    col("customer_id").cast("int")
).withColumn(
    "country",
    when(col("country").isNull(), "Unknown").otherwise(col("country"))
)

# -----------------------------
# JOIN DATASETS
# -----------------------------
final_df = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left"
)

# -----------------------------
# DEBUG (VERY IMPORTANT)
# -----------------------------
final_df.printSchema()
final_df.show(10, truncate=False)

# -----------------------------
# WRITE OUTPUT (PARQUET)
# -----------------------------
final_df.write.mode("overwrite").parquet(
    "s3://etl-elt-lab-student1/data/processed/final/"
)