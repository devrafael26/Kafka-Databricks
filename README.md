# StreamLake Platform

Plataforma de Dados Streaming com Apache Kafka + Databricks + Delta Lake

Projeto de engenharia de dados end-to-end para processamento de eventos em tempo real,
utilizando arquitetura Lakehouse, pipelines declarativos, controle de qualidade de dados
e automação CI/CD.

---

## 📌 Visão Geral

O StreamLake Platform simula uma plataforma de processamento de pedidos em tempo real.

Eventos de pedidos são publicados em um tópico Kafka, consumidos pelo Databricks e
processados através de uma arquitetura em camadas:

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
