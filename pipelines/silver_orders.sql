CREATE OR REFRESH STREAMING TABLE workspace.silver.orders
(
  CONSTRAINT orderunits_valido
    EXPECT (orderunits >= 0)
    ON VIOLATION DROP ROW,

  CONSTRAINT city_valida
    EXPECT (city IS NOT NULL)
    ON VIOLATION DROP ROW
)

COMMENT "Silver Orders: Dados validados com CDC (SCD Type 1) e colunas técnicas";

APPLY CHANGES INTO silver_orders

FROM
(
    SELECT
        -- Colunas de negócio
        orderid,
        orderdate,
        itemid,
        CAST(orderunits AS INT) AS orderunits,
        city,
        state,
        zipcode,
        -- Coluna já existentes da Bronze
        bronze_processing_timestamp,
        -- Coluna técnica da Silver
        current_timestamp() AS silver_processing_timestamp

    FROM STREAM(bronze_orders)

)

KEYS (orderid)

SEQUENCE BY bronze_processing_timestamp

STORED AS SCD TYPE 1;
