"""Daily sales pipeline: load staged files -> capture new rows -> update 3 summaries -> check."""
from __future__ import annotations

from datetime import datetime
from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN = "snowflake_default"


@dag(
    dag_id="daily_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["daily-sales", "stage", "incremental"],
)
def daily_sales_pipeline():

    load_raw = SQLExecuteQueryOperator(
        task_id="load_raw",
        conn_id=CONN,
        sql="""
            COPY INTO raw_sales
            FROM @sales_stage
            FILE_FORMAT = (FORMAT_NAME = 'sales_csv_format')
            ON_ERROR = 'ABORT_STATEMENT';
        """,
    )

    capture_new_rows = SQLExecuteQueryOperator(
        task_id="capture_new_rows",
        conn_id=CONN,
        sql=[
            "TRUNCATE TABLE IF EXISTS new_sales_batch",
            """
            INSERT INTO new_sales_batch
                (order_id, order_date, product, region, quantity, unit_price)
            SELECT order_id, order_date, product, region, quantity, unit_price
            FROM new_sales_stream
            WHERE METADATA$ACTION = 'INSERT'
            """,
        ],
    )

    update_summary_by_region = SQLExecuteQueryOperator(
        task_id="update_summary_by_region",
        conn_id=CONN,
        sql="""
            MERGE INTO summary_by_region AS t
            USING (
                SELECT region, COUNT(*) AS n_orders,
                       SUM(quantity * unit_price) AS revenue
                FROM new_sales_batch GROUP BY region
            ) AS s
            ON t.region = s.region
            WHEN MATCHED THEN UPDATE SET
                total_orders  = t.total_orders  + s.n_orders,
                total_revenue = t.total_revenue + s.revenue
            WHEN NOT MATCHED THEN
                INSERT (region, total_orders, total_revenue)
                VALUES (s.region, s.n_orders, s.revenue);
        """,
    )

    update_summary_by_product = SQLExecuteQueryOperator(
        task_id="update_summary_by_product",
        conn_id=CONN,
        sql="""
            MERGE INTO summary_by_product AS t
            USING (
                SELECT product, COUNT(*) AS n_orders,
                       SUM(quantity * unit_price) AS revenue
                FROM new_sales_batch GROUP BY product
            ) AS s
            ON t.product = s.product
            WHEN MATCHED THEN UPDATE SET
                total_orders  = t.total_orders  + s.n_orders,
                total_revenue = t.total_revenue + s.revenue
            WHEN NOT MATCHED THEN
                INSERT (product, total_orders, total_revenue)
                VALUES (s.product, s.n_orders, s.revenue);
        """,
    )

    update_summary_by_day_region = SQLExecuteQueryOperator(
        task_id="update_summary_by_day_region",
        conn_id=CONN,
        sql="""
            MERGE INTO summary_by_day_region AS t
            USING (
                SELECT order_date, region, COUNT(*) AS n_orders,
                       SUM(quantity * unit_price) AS revenue
                FROM new_sales_batch GROUP BY order_date, region
            ) AS s
            ON t.order_date = s.order_date AND t.region = s.region
            WHEN MATCHED THEN UPDATE SET
                total_orders  = t.total_orders  + s.n_orders,
                total_revenue = t.total_revenue + s.revenue
            WHEN NOT MATCHED THEN
                INSERT (order_date, region, total_orders, total_revenue)
                VALUES (s.order_date, s.region, s.n_orders, s.revenue);
        """,
    )

    check_final_tables = SQLExecuteQueryOperator(
        task_id="check_final_tables",
        conn_id=CONN,
        sql="""
            SELECT
              (SELECT COUNT(*) FROM summary_by_region)     AS region_rows,
              (SELECT COUNT(*) FROM summary_by_product)    AS product_rows,
              (SELECT COUNT(*) FROM summary_by_day_region) AS day_region_rows;
        """,
    )

    load_raw >> capture_new_rows >> [
        update_summary_by_region,
        update_summary_by_product,
        update_summary_by_day_region,
    ] >> check_final_tables


daily_sales_pipeline()
