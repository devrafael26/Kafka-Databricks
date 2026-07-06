CREATE OR REFRESH MATERIALIZED VIEW workspace.gold.city_ranking
AS

SELECT 
    state,
    city,
    -- Converte o timestamp completo para apenas a data (YYYY-MM-DD)
    TO_DATE(orderdate) AS order_day,
    COUNT(*) AS total_orders,
    SUM(orderunits) AS total_units,
    ROUND(AVG(orderunits), 0) AS avg_units
FROM silver_orders
GROUP BY 
    state,
    city,
    TO_DATE(orderdate)
