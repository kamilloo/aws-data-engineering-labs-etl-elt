from pyspark.sql.functions import col, when, coalesce, to_date, regexp_replace
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 📥 Read data
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

# 🔧 Transform orders
orders_clean = orders.withColumn(
    "quantity",
    when(col("quantity").isNull(), 1).otherwise(col("quantity"))
)

# Fix date format (replace / with -)
orders_clean = orders_clean.withColumn(
    "date",
    to_date(regexp_replace(col("date"), "/", "-"), "yyyy-MM-dd")
)

# Add total_price
orders_clean = orders_clean.withColumn(
    "total_price",
    col("quantity") * col("price")
)

# 🔧 Transform customers
customers_clean = customers.withColumn(
    "country",
    coalesce(col("country"), col("country")).otherwise("Unknown")
)

# 🤝 Join datasets
final_df = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left"
)

# 📤 Write output
final_df.write.mode("overwrite").parquet(
    "s3://etl-elt-lab-student1/data/processed/final/"
)