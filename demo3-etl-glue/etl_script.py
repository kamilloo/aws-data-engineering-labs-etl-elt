from pyspark.sql.functions import col, when

# Example transformation script

def transform(df):
    df_clean = df.withColumn(
        "quantity",
        when(col("quantity").isNull(), 1).otherwise(col("quantity"))
    )

    df_clean = df_clean.withColumn(
        "total_price",
        col("quantity") * col("price")
    )

    return df_clean
