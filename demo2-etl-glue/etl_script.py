from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, to_date

from awsglue.context import GlueContext
from pyspark.context import SparkContext

# =========================================================
# INITIALIZATION
# =========================================================
# Starting ETL processing (start pipeline)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =========================================================
# DATA DISCOVERY
# =========================================================
# Load raw datasets from S3 to understand structure and schema
orders = spark.read.csv(
    "s3://etl-elt-lab-student1/data/raw/orders/",
    header=True,
    inferSchema=True
)

customers = spark.read.csv(
    "s3://etl-elt-lab-student1/data/raw/customers/",
    header=True,
    inferSchema=True
)

# =========================================================
# DATA CLEANING
# =========================================================
# Fix missing values and incorrect formats in raw data

orders_clean = orders.withColumn(
    "quantity",
    when(col("quantity").isNull(), 1).otherwise(col("quantity"))
)

# Handle inconsistent data types (force schema consistency)
orders_clean = orders_clean.withColumn(
    "order_id", col("order_id").cast("int")
).withColumn(
    "customer_id", col("customer_id").cast("int")
).withColumn(
    "quantity", col("quantity").cast("int")
).withColumn(
    "price", col("price").cast("double")   # ensure numeric consistency
)


# =========================================================
# DATA WRANGLING
# =========================================================
# Standardize inconsistent formats (e.g., date formatting issues)

orders_clean = orders_clean.withColumn(
    "date",
    to_date(regexp_replace(col("date"), "/", "-"), "yyyy-MM-dd")
)


# =========================================================
# DATA ENRICHING
# =========================================================
# Create new business metrics from existing data

orders_clean = orders_clean.withColumn(
    "total_price",
    col("quantity") * col("price")
)


# =========================================================
# DATA STRUCTURING
# =========================================================
# Normalize customer dataset for joining and analytics

customers_clean = customers.withColumn(
    "customer_id",
    col("customer_id").cast("int")
).withColumn(
    "country",
    when(col("country").isNull(), "Unknown").otherwise(col("country"))
)

# =========================================================
# DATA ENRICHING
# =========================================================
# Combine datasets into a single analytical dataset

final_df = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left"
)


# =========================================================
# DATA VALIDATION
# =========================================================
# Verify final structure before writing output

final_df.printSchema()
final_df.show(10, truncate=False)


# =========================================================
# DATA PUBLISHING
# =========================================================
# Write cleaned + enriched dataset to S3 for analytics use

final_df.write.mode("overwrite").parquet(
    "s3://etl-elt-lab-student1/data/processed/final/"
)