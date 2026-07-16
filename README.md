![Arquitetura do StreamLake Platform](docs/architecture.png)

# StreamLake Platform

 Plataforma de Dados Streaming utilizando **Apache Kafka**,
 **Databricks**, **Spark Structured Streaming**, **Delta Lake** e
 **Lakeflow Declarative Pipelines**.

Projeto de Engenharia de Dados end-to-end para processamento de eventos
em tempo real, implementando uma arquitetura Lakehouse com foco em
**Streaming**, **Qualidade de Dados**, **CDC**, **Observabilidade** e
**CI/CD**.

------------------------------------------------------------------------

# 🚀 Visão Geral

O **StreamLake Platform** simula uma plataforma de processamento de
pedidos em tempo real.

Os eventos são publicados em um tópico **Apache Kafka**, consumidos pelo
**Spark Structured Streaming** e persistidos em uma arquitetura
Lakehouse composta pelas camadas:

-   Raw
-   Bronze
-   Silver
-   Gold

O projeto foi desenvolvido para demonstrar uma implementação moderna de
Engenharia de Dados utilizando recursos do ecossistema Databricks.

🏗️ Arquitetura

``` text
                Apache Kafka
                     │
                     ▼
              Raw Delta Table
             (Kafka Events)
                     │
                     ▼
             Bronze Layer
  Parsing + Schema Enforcement
                     │
                     ▼
             Silver Layer
  Data Quality + CDC (SCD Type 1)
                     │
                     ▼
              Gold Layer
Business Analytics + Observability
```

# 🛠️ Tecnologias Utilizadas

### Streaming

-   Apache Kafka
-   Spark Structured Streaming

### Plataforma de Dados

-   Databricks
-   Delta Lake
-   Lakeflow Declarative Pipelines (Delta Live Tables)

### Processamento

-   PySpark
-   Spark SQL

### Versionamento e CI/CD

-   GitHub
-   GitHub Actions
-   Databricks Asset Bundles

### Testes

-   Pytest
-   JSON Schema

# 🔄 Pipeline de Dados

## Ingestão Kafka

Os eventos chegam ao Kafka no formato JSON.

``` json
{
  "ordertime":1499273278470,
  "orderid":6,
  "itemid":"Item_915",
  "orderunits":6.76,
  "address":{
    "city":"City_",
    "state":"State_75",
    "zipcode":46300
  }
}
```

A camada **Raw** preserva o evento original recebido do tópico Kafka,
incluindo seus metadados (`key`, `value`, `topic`, `partition`,
`offset`, `timestamp` e `timestampType`).

# 🥉 Bronze Layer

Responsável por transformar os eventos brutos em uma estrutura tipada.

### Processamentos realizados

-   Decodificação do payload Kafka;
-   Conversão Binary → JSON;
-   Parsing utilizando schema definido;
-   Conversão de timestamps;
-   Tipagem das colunas.

``` sql
from_json(
    decode(value,'UTF-8'),
    schema
)
```

Resultado:

``` text
orderid
orderdate
itemid
orderunits
city
state
zipcode
```

# 🥈 Silver Layer

## CDC

Implementado utilizando:

``` sql
APPLY CHANGES INTO
KEYS(orderid)
```

com comportamento **SCD Type 1**.

## Data Quality

``` sql
CONSTRAINT orderunits_valido
EXPECT(orderunits >= 0)
ON VIOLATION DROP ROW
```

Validações implementadas:

-   Quantidades negativas;
-   Campos obrigatórios;
-   Integridade dos registros.

# 🥇 Gold Layer

Camada responsável pela disponibilização de dados analíticos e
indicadores de observabilidade.

## 📈 Métricas Analíticas

``` sql
COUNT(orderid) AS total_orders,
SUM(orderunits) AS total_units,
AVG(orderunits) AS avg_units_per_order
```

### Métricas disponibilizadas

-   Quantidade total de pedidos;
-   Volume total de unidades processadas;
-   Média, mínimo e máximo de unidades por pedido;
-   Ranking por cidade;
-   Ranking por estado;
-   Eventos processados por janela;
-   Latência média, mínima e máxima entre Bronze e Silver.

## 📊 Tabelas Analíticas

### gold_orders_metrics

``` text
time_window
total_orders
total_units
avg_units_per_order
min_units
max_units
```

### gold_city_ranking

``` text
state
city
order_day
total_orders
total_units
avg_units
```

### gold_state_ranking

``` text
state
total_orders
total_units
avg_units
```

### gold_pipeline_observability

Indicadores monitorados:

-   Eventos processados por janela;
-   Latência média Bronze → Silver;
-   Latência mínima Bronze → Silver;
-   Latência máxima Bronze → Silver;
-   Evolução da latência.

# 🧪 Testes Automatizados

Estrutura:

``` text
tests/
├── fixtures/
│   └── order_event.json
├── schemas/
│   ├── order_event_schema.json
│   ├── bronze_orders_schema.json
│   └── silver_orders_schema.json
├── test_order_event_contract.py
├── test_bronze_schema.py
├── test_silver_schema.py
└── test_data_quality.py
```

### Contrato do Evento Kafka

``` text
Kafka Event
    │
    ▼
JSON Schema Validation
```

### Validações

-   Contrato JSON Schema;
-   Schema Bronze;
-   Schema Silver;
-   `orderunits >= 0`;
-   `city NOT NULL`;
-   `orderid` único.

Execução:

``` bash
pytest tests/
```

Resultado esperado:

``` text
6 passed
```

# 🔁 CI/CD

## Continuous Integration (CI)

``` text
Pull Request
      │
      ▼
GitHub Actions
      ├── Install dependencies
      ├── Run Pytest
      └── Validate Databricks Bundle
```

## Continuous Deployment (CD)

``` text
Merge Pull Request
        │
        ▼
     Push main
        │
        ▼
Databricks Bundle Deploy
```

# 📂 Estrutura do Projeto

``` text
.
├── .github
│   └── workflows
│       ├── databricks-ci.yml
│       └── databricks-cd.yml
├── resources
│   ├── kafka_pipeline.pipeline.yml
│   └── kafka_stream_job.job.yml
├── docs
│   ├── 1.imagem.PNG
│   ├── 2.confluent-kafka.PNG
│   ├── 3.pipeline.PNG
│   ├── 4. job.PNG
│   ├── 5.tasks.PNG
│   ├── 6.analytics.PNG
│   └── 7.observalidade.PNG
├── src
│   ├── notebooks
│   │   └── ingest_kafka.py
│   └── pipelines
│       ├── bronze_orders.sql
│       ├── silver_orders.sql
│       ├── gold_orders_metrics.sql
│       ├── gold_city_ranking.sql
│       ├── gold_state_ranking.sql
│       └── gold_pipeline_observability.sql
├── tests
│   ├── fixtures
│   ├── schemas
│   │   ├── order_event_schema.json
│   │   ├── bronze_orders_schema.json
│   │   └── silver_orders_schema.json
│   ├── test_order_event_contract.py
│   ├── test_bronze_schema.py
│   ├── test_silver_schema.py
│   └── test_data_quality.py
├── databricks.yml
└── README.md
```

# 🚀 Resultados

-   Ingestão de eventos em streaming com Apache Kafka.
-   Processamento distribuído com Spark Structured Streaming.
-   Arquitetura Lakehouse (Raw, Bronze, Silver e Gold).
-   CDC utilizando `APPLY CHANGES INTO` (SCD Type 1).
-   Data Quality com Expectations.
-   Contratos de dados utilizando JSON Schema.
-   Testes automatizados com Pytest.
-   CI/CD utilizando GitHub Actions e Databricks Asset Bundles.
-   Orquestração utilizando Databricks Workflows.
-   Dashboards analíticos e de observabilidade.
