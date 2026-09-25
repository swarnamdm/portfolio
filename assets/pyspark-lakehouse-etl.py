"""Sample PySpark lakehouse ETL: S3 raw -> curated.

Demo / concept code: the pattern a Spark job would run on EMR/Databricks —
read raw JSON, clean and dedupe, write partitioned Parquet to the curated zone.
Imports are commented so the file is readable without a Spark cluster;
the local simulation in simulate_lakehouse.py runs the equivalent pandas logic.
"""
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, year, month, dayofmonth


def build_spark() -> object:
    # return (
    #     SparkSession.builder
    #     .appName("raw-to-curated")
    #     .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    #     .getOrCreate()
    # )
    raise NotImplementedError("Demo file — needs a Spark cluster; see simulate_lakehouse.py")


def raw_to_curated(spark, raw_path: str, curated_path: str) -> None:
    """spark.read.json(raw) -> clean/dedupe -> partitioned parquet write."""
    # raw = spark.read.json(raw_path)
    #
    # curated = (
    #     raw
    #     .withColumn("order_date", to_date(col("order_ts")))
    #     .withColumn("year", year(col("order_date")))
    #     .withColumn("month", month(col("order_date")))
    #     .withColumn("day", dayofmonth(col("order_date")))
    #     .dropna(subset=["order_id"])
    #     .dropDuplicates(["order_id"])
    #     .select("order_id", "customer_id", "order_date", "region",
    #             "quantity", "unit_price",
    #             (col("quantity") * col("unit_price")).alias("line_total"),
    #             "year", "month", "day")
    # )
    #
    # (curated.write
    #     .mode("overwrite")
    #     .partitionBy("year", "month", "day", "region")
    #     .parquet(curated_path))   # s3://lake/curated/orders/
    #
    # curated.createOrReplaceTempView("curated_orders")
    # spark.sql("CACHE TABLE curated_orders")
    print("Pattern: raw JSON -> clean/dedupe/type -> partitioned Parquet (year=/month=/day=/region=).")


if __name__ == "__main__":
    print("PySpark lakehouse pattern (needs a cluster for real execution).")
    print("Run simulate_lakehouse.py for the local zone-by-zone demo.")
