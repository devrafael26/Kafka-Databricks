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
