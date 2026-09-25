"""Sample Databricks (PySpark) transform: bronze -> silver.

Demo / concept code: the pattern a Databricks notebook/job would run —
read raw landing data, clean, dedupe, enrich, write partitioned silver output.
Shown with pandas here so the logic is readable without a Spark cluster;
the real job would use spark.read / DataFrame API equivalently.
"""
import pandas as pd


def transform_orders(bronze: pd.DataFrame) -> pd.DataFrame:
    silver = bronze.copy()

    # 1. normalize column names
    silver.columns = [c.strip().lower().replace(" ", "_") for c in silver.columns]

    # 2. drop rows missing the business key
    before = len(silver)
    silver = silver.dropna(subset=["order_id"])
    dropped_no_key = before - len(silver)

    # 3. dedupe on the business key, keep latest
    silver = silver.sort_values("order_ts").drop_duplicates("order_id", keep="last")
    deduped = before - dropped_no_key - len(silver)

    # 4. enrich: line total + order date partition column
    silver["line_total"] = (silver["quantity"] * silver["unit_price"]).round(2)
    silver["order_date"] = pd.to_datetime(silver["order_ts"]).dt.date

    # silver.write.mode("overwrite").partitionBy("order_date").parquet("s3://lake/silver/orders/")
    return silver, {"dropped_no_key": dropped_no_key, "deduped": deduped}


if __name__ == "__main__":
    print("Databricks transform pattern: bronze -> clean/dedupe/enrich -> silver (partitioned).")
    print("See simulate_dag.py for the end-to-end local demo run.")
