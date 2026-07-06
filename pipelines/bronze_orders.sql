CREATE OR REFRESH STREAMING TABLE workspace.bronze.orders
COMMENT "Tabela Silver: Estrutura relacional limpa e tipada"
AS 
SELECT 
    
    CAST(dados.orderid AS LONG) AS orderid,
    to_timestamp(dados.ordertime / 1000) AS orderdate,
    dados.itemid AS itemid,
    CAST(dados.orderunits AS DOUBLE) AS orderunits,
    dados.address.city AS city,
    dados.address.state AS state,
    dados.address.zipcode AS zipcode,
    current_timestamp() AS bronze_processing_timestamp
FROM (
    SELECT 
        from_json(
            regexp_extract(decode(value, 'UTF-8'), '(\\{.*\\})', 1),
            'ordertime LONG, orderid LONG, itemid STRING, orderunits DOUBLE, address STRUCT<city:STRING, state:STRING, zipcode:LONG>'
        ) AS dados
    FROM STREAM(raw_orders)
)
WHERE dados.orderid IS NOT NULL
