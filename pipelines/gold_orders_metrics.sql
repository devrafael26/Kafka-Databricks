CREATE OR REFRESH MATERIALIZED VIEW workspace.gold.order_metrics
AS

SELECT

    window(orderdate, '1 minute') AS time_window,

    COUNT(*) AS total_orders,

    SUM(orderunits) AS total_units,

    ROUND(AVG(orderunits), 2) AS avg_units_per_order,

    MIN(orderunits) AS min_units,

    MAX(orderunits) AS max_units

FROM silver_orders

GROUP BY
    window(orderdate, '1 minute');