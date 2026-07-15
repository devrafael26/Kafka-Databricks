StreamLake Platform

Plataforma de Dados Streaming utilizando Apache Kafka, Databricks, Spark Structured Streaming, Delta Lake e Lakeflow Declarative Pipelines.

Projeto de Engenharia de Dados end-to-end para processamento de eventos em tempo real, implementando uma arquitetura Lakehouse com foco em Streaming, Qualidade de Dados, CDC, Observabilidade e CI/CD.

🚀 Visão Geral

O StreamLake Platform simula uma plataforma de processamento de pedidos em tempo real.

Os eventos são publicados em um tópico Apache Kafka, consumidos pelo Spark Structured Streaming e persistidos em uma arquitetura Lakehouse composta pelas camadas:

Raw
Bronze
Silver
Gold

O projeto foi desenvolvido para demonstrar uma implementação moderna de Engenharia de Dados utilizando recursos do ecossistema Databricks.

🏗️ Arquitetura

                    Apache Kafka
                         │
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


 🛠️ Tecnologias Utilizadas
Streaming
Apache Kafka
Spark Structured Streaming
Plataforma de Dados
Databricks
Delta Lake
Lakeflow Declarative Pipelines (Delta Live Tables)
Processamento
PySpark
Spark SQL
Versionamento e CI/CD
GitHub
GitHub Actions
Databricks Asset Bundles
Testes
Pytest
JSON Schema
🔄 Pipeline de Dados
Ingestão Kafka

Os eventos chegam ao Kafka no formato JSON.

Exemplo:

{
  "ordertime": 1499273278470,
  "orderid": 6,
  "itemid": "Item_915",
  "orderunits": 6.76,
  "address": {
    "city": "City_",
    "state": "State_75",
    "zipcode": 46300
  }
}

A camada Raw preserva o evento original recebido do tópico Kafka, incluindo seus metadados (key, value, topic, partition, offset, timestamp e timestampType).

🥉 Bronze Layer

Responsável por transformar os eventos brutos em uma estrutura tipada.

Processamentos realizados
Decodificação do payload Kafka;
Conversão Binary → JSON;
Parsing utilizando schema definido;
Conversão de timestamps;
Tipagem das colunas.

Exemplo:

from_json(
    decode(value,'UTF-8'),
    schema
)

Resultado:

orderid
orderdate
itemid
orderunits
city
state
zipcode

🥈 Silver Layer

Camada responsável pela aplicação das regras de negócio.

CDC

Implementado utilizando:
APPLY CHANGES INTO
com
KEYS(orderid)
implementando comportamento SCD Type 1.

Data Quality

Foram adicionadas regras utilizando Expectations.

Exemplo:

CONSTRAINT orderunits_valido
EXPECT(orderunits >= 0)
ON VIOLATION DROP ROW

Validações implementadas:

Quantidades negativas;
Campos obrigatórios;
Integridade dos registros.

🥇 Gold Layer

Camada responsável pela disponibilização de dados analíticos para consumo.

A partir dos dados confiáveis da Silver foram criadas tabelas agregadas contendo métricas de negócio, análises dimensionais e indicadores de observabilidade.

📈 Métricas Analíticas

Exemplo:
COUNT(orderid) AS total_orders,
SUM(orderunits) AS total_units,
AVG(orderunits) AS avg_units_per_order

Métricas disponibilizadas
Quantidade total de pedidos;
Volume total de unidades processadas;
Média de unidades por pedido;
Valores mínimo e máximo de unidades;
Ranking por cidade;
Ranking por estado;
Eventos processados por janela;
Latência média Bronze → Silver;
Latência mínima Bronze → Silver;
Latência máxima Bronze → Silver.

📊 Tabelas Analíticas

gold_orders_metrics

Consolida métricas gerais dos pedidos.
time_window
total_orders
total_units
avg_units_per_order
min_units
max_units

gold_city_ranking

Ranking de pedidos por cidade.
state
city
order_day
total_orders
total_units
avg_units

gold_state_ranking

Ranking consolidado por estado.
state
total_orders
total_units
avg_units

gold_pipeline_observability

Tabela destinada ao monitoramento operacional da pipeline.

Indicadores monitorados:

Eventos processados por janela;
Latência média Bronze → Silver;
Latência mínima Bronze → Silver;
Latência máxima Bronze → Silver;
Evolução da latência ao longo do processamento.

🧪 Testes Automatizados

A pipeline possui testes automatizados utilizando Pytest, executados durante o processo de Continuous Integration (CI).

Estrutura

tests/
├── fixtures/
│   └── order_event.json
│
├── schemas/
│   ├── order_event_schema.json
│   ├── bronze_orders_schema.json
│   └── silver_orders_schema.json
│
├── test_order_event_contract.py
├── test_bronze_schema.py
├── test_silver_schema.py
└── test_data_quality.py


Contrato do Evento Kafka

Validação do payload utilizando JSON Schema.

Kafka Event
     │
     ▼
JSON Schema Validation

Validação de Schema

Verificação das estruturas das camadas:

Bronze;
Silver.

Garantindo:

existência das colunas;
tipos esperados;
conformidade com os schemas definidos.
Data Quality

Validações implementadas:

orderunits ≥ 0;
city NOT NULL;
orderid único.

Execução
pytest tests/

Resultado esperado:
6 passed


🔁 CI/CD

O projeto utiliza GitHub Actions para automação da integração e entrega contínuas.

Continuous Integration (CI)

Executado em Pull Requests para a branch main.

Pull Request
      │
      ▼
GitHub Actions
      │
      ├── Install dependencies
      ├── Run Pytest
      └── Validate Databricks Bundle

Objetivo:

Impedir que alterações com problemas cheguem à branch principal.

Continuous Deployment (CD)

Executado após merge na branch main.
Merge Pull Request
        │
        ▼
     Push main
        │
        ▼
Databricks Bundle Deploy

Responsável por publicar a aplicação no ambiente Databricks.

📂 Estrutura do Projeto

.
├── .github
│   └── workflows
│       ├── databricks-ci.yml
│       └── databricks-cd.yml
│
├── resources
│   ├── kafka_pipeline.pipeline.yml
│   └── kafka_stream_job.job.yml
│
├── src
│   ├── notebooks
│   │   └── ingest_kafka.ipynb
│   │
│   └── pipelines
│       ├── bronze_orders.sql
│       ├── silver_orders.sql
│       ├── gold_orders_metrics.sql
│       ├── gold_city_ranking.sql
│       ├── gold_state_ranking.sql
│       └── gold_pipeline_observability.sql
│
├── tests
│   ├── fixtures
│   ├── schemas
│   ├── test_order_event_contract.py
│   ├── test_bronze_schema.py
│   ├── test_silver_schema.py
│   └── test_data_quality.py
│
├── databricks.yml
└── README.md


🚀 Resultados

O projeto demonstra uma implementação completa de uma plataforma moderna de Engenharia de Dados utilizando o ecossistema Databricks.

Principais capacidades implementadas:

Ingestão de eventos em streaming com Apache Kafka;
Processamento distribuído com Spark Structured Streaming;
Arquitetura Lakehouse (Raw, Bronze, Silver e Gold);
CDC utilizando APPLY CHANGES INTO (SCD Type 1);
Data Quality com Expectations;
Contratos de dados utilizando JSON Schema;
Testes automatizados com Pytest;
CI/CD utilizando GitHub Actions e Databricks Asset Bundles;
Orquestração utilizando Databricks Workflows;
Dashboards analíticos e de observabilidade.
