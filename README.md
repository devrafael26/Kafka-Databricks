# StreamLake Platform

Plataforma de Dados Streaming com Apache Kafka + Databricks + Delta Lake

Projeto de engenharia de dados end-to-end para processamento de eventos em tempo real,
utilizando arquitetura Lakehouse, pipelines declarativos, controle de qualidade de dados
e automação CI/CD.

---

## 📌 Visão Geral

O StreamLake Platform simula uma plataforma de processamento de pedidos em tempo real.

Os eventos de pedidos são publicados em um tópico Kafka, consumidos pelo Databricks e processados através de uma arquitetura em camadas utilizando o Lakeflow Declarative Pipelines (DLT):

- Bronze: ingestão e persistência dos eventos brutos;
- Silver: dados tratados, tipados e enriquecidos;
- Gold: camada analítica (em desenvolvimento).

O objetivo do projeto é demonstrar uma implementação moderna de Data Engineering,
com foco em streaming, governança, qualidade e automação.

---

# 🏗️ Arquitetura

```text
                    Apache Kafka
                         |
                         |
                         v
                  Raw Delta Table
                  (Kafka events)
                         |
                         |
                         v
                    Bronze Layer
             Parsing + Schema Enforcement
                         |
                         |
                         v
                    Silver Layer
          Data Quality + CDC + SCD Type 1
                         |
                         |
                         v
                    Gold Layer
              Business Analytics (future)

🛠️ Tecnologias Utilizadas

Streaming

* Apache Kafka
* Structured Streaming

Data Platform

* Databricks
* Delta Lake
* Lakeflow Declarative Pipelines (Delta Live Tables)

Processing

* PySpark / Spark SQL

Versionamento e CI/CD

* GitHub
* GitHub Actions
* Databricks Asset Bundles

Testes

* Pytest
* JSON Schema

⸻

🔄 Pipeline de Dados

1. Ingestão Kafka

Eventos são recebidos no formato JSON:

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

A camada Raw mantém o evento original recebido do tópico Kafka.

⸻

🥉 Bronze Layer

Responsável por transformar o evento bruto em uma estrutura tipada.

Processamentos realizados:

* Decodificação do payload Kafka;
* Conversão Binary → JSON;
* Parsing utilizando schema definido;
* Conversão de timestamps;
* Tipagem das colunas.

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

Implementações:
CDC

Utilização de:
APPLY CHANGES INTO

com:
KEYS(orderid)

Implementando comportamento:
SCD Type 1

Data Quality

Foram adicionadas regras de qualidade utilizando expectations:

Exemplo:
CONSTRAINT orderunits_valido
EXPECT(orderunits >= 0)
ON VIOLATION DROP ROW

Validações:

* Quantidades negativas;
* Campos obrigatórios;
* Integridade dos registros.

🥇 Gold Layer

Camada responsável pela disponibilização de dados analíticos e indicadores para consumo.

A partir dos dados confiáveis da Silver, foram criadas tabelas agregadas contendo métricas de negócio e observabilidade do pipeline.

Implementações:

Métricas analíticas

Criação de agregações para análise dos pedidos:

Exemplo:
COUNT(orderid) AS total_orders,
SUM(orderunits) AS total_units

Indicadores gerados:

Quantidade total de pedidos;
Volume de itens processados;
Distribuição de pedidos por localização;
Rankings por cidade e estado.

Tabelas analíticas criadas:

▦ gold_orders_metrics

Responsável por consolidar métricas gerais dos pedidos.

Exemplo:
total_orders
total_units
avg_units_per_order

▦ gold_city_ranking 表格

Tabela destinada à análise de performance por cidade.

Exemplo:
city
total_orders
total_units

▦ gold_state_ranking

Tabela destinada à análise consolidada por estado.

Exemplo:
state
total_orders
total_units

Observabilidade do Pipeline

Foi criada uma tabela para acompanhamento operacional do processamento:

▦ gold_pipeline_observability

Monitoramento de informações técnicas:

Timestamp de processamento Bronze;
Timestamp de processamento Silver;
Latência entre etapas;
Acompanhamento da execução do pipeline.

⸻

🧪 Testes Automatizados

A pipeline possui testes executados automaticamente pelo CI.

Estrutura:
tests/

├── test_order_event_contract.py
├── test_bronze_schema.py
├── test_silver_schema.py
└── test_data_quality.py

Contrato Kafka

Valida se o evento recebido segue o schema esperado.

Exemplo:
Kafka Event
     |
     v
JSON Schema Validation

Schema Validation

Validação das estruturas:

* Bronze;
* Silver.

Garantindo:

* existência das colunas;
* tipos esperados;
* campos obrigatórios.

⸻

Data Quality Tests

Validações implementadas:

* valores inválidos;
* campos nulos;
* duplicidade de chave.

Execução:
pytest tests/

Resultado esperado:
6 passed

🔁 CI/CD

O projeto utiliza GitHub Actions para automação.

Continuous Integration (CI)

Executado em Pull Requests para a branch main.

Fluxo:

Pull Request
      |
      v
GitHub Actions
      |
      +-- Install dependencies
      |
      +-- Run Pytest
      |
      +-- Validate Databricks Bundle

Objetivo:

Impedir que alterações com problemas cheguem à branch principal.

⸻

Continuous Deployment (CD)

Executado após alterações na branch main.

Fluxo:

Merge Pull Request
        |
        v
Push main
        |
        v
Databricks Bundle Deploy

Responsável por publicar a aplicação no ambiente Databricks.

📂 Estrutura do Projeto

.
├── .github
│   └── workflows
│       ├── database-ci.yaml
│       └── database-cd.yaml
│
├── resources
│
├── src
│
├── tests
│   ├── schemas
│   ├── test_order_event_contract.py
│   ├── test_bronze_schema.py
│   ├── test_silver_schema.py
│   └── test_data_quality.py
│
├── databricks.yml
└── README.md

🚀 Próximos Passos

* Implementação da camada Gold;
* Criação de métricas analíticas;
* Monitoramento da qualidade;
* Deploy em múltiplos ambientes;
* Integração com ferramentas de observabilidade.



