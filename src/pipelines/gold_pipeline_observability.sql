CREATE OR REFRESH MATERIALIZED VIEW workspace.gold.pipeline_observability
AS

SELECT

    window(silver_processing_timestamp, '1 minute') AS time_window,

    COUNT(*) AS total_orders,

    AVG(
        unix_timestamp(silver_processing_timestamp) -
        unix_timestamp(bronze_processing_timestamp)
    ) AS avg_bronze_silver_latency_sec,

    MAX(
        unix_timestamp(silver_processing_timestamp) -
        unix_timestamp(bronze_processing_timestamp)
    ) AS max_bronze_silver_latency_sec,

    MIN(
        unix_timestamp(silver_processing_timestamp) -
        unix_timestamp(bronze_processing_timestamp)
    ) AS min_bronze_silver_latency_sec

    

FROM workspace.silver.orders_silver

GROUP BY
    window(silver_processing_timestamp, '1 minute');