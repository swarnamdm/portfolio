"""Sample Airflow DAG: daily_sales_etl.

Demo / concept code: shows a standard extract -> Databricks transform ->
load -> data-quality gate pattern with retries and an SLA.
Requires Apache Airflow to run for real; see simulate_dag.py for a local run.
"""
from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

# with DAG(
#     dag_id="daily_sales_etl",
#     default_args=DEFAULT_ARGS,
#     schedule="@daily",
#     start_date=datetime(2026, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=["sales", "etl"],
# ) as dag:
#
#     extract_orders = PythonOperator(
#         task_id="extract_orders",
#         python_callable=extract_orders_from_api,   # REST API -> bronze/orders_raw.csv
#     )
#
#     run_databricks = DatabricksRunNowOperator(
#         task_id="run_databricks",
#         job_name="sales_silver_transform",        # Spark: clean/dedupe/enrich -> silver
#         notebook_params={"ds": "{{ ds }}"},
#     )
#
#     load_warehouse = PythonOperator(
#         task_id="load_warehouse",
#         python_callable=merge_into_fact_orders,   # MERGE silver -> warehouse.fact_orders
#     )
#
#     data_quality = PythonOperator(
#         task_id="data_quality",
#         python_callable=run_dq_gates,             # row-count, null-rate, freshness checks
#     )
#
#     extract_orders >> run_databricks >> load_warehouse >> data_quality

print("DAG definition: daily_sales_etl — extract_orders >> run_databricks >> load_warehouse >> data_quality")
print("(Airflow runtime not installed here; run simulate_dag.py for the local demo.)")
