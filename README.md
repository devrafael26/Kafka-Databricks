# StreamLake Platform

Plataforma de Dados Streaming com Apache Kafka + Databricks + Delta Lake

Projeto de engenharia de dados end-to-end para processamento de eventos em tempo real,
utilizando arquitetura Lakehouse, pipelines declarativos, controle de qualidade de dados
e automação CI/CD.

---

## 🎯 Objetivo

Este projeto foi desenvolvido com o objetivo de consolidar, na prática, conceitos modernos de Engenharia de Dados relacionados a processamento de dados streaming, arquitetura Lakehouse, qualidade de dados, automação CI/CD, governança e observabilidade de pipelines utilizando o ecossistema Databricks.

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

## ✨ Principais Funcionalidades

-  Ingestão de eventos em streaming utilizando Apache Kafka
-  Processamento distribuído com Spark Structured Streaming
-  Arquitetura Lakehouse (Raw, Bronze, Silver e Gold)
-  Lakeflow Declarative Pipelines (Delta Live Tables)
-  Schema Enforcement
-  Contratos de Dados com JSON Schema
-  CDC utilizando APPLY CHANGES INTO (SCD Type 1)
-  Data Quality com Expectations
-  Testes automatizados utilizando Pytest
-  CI/CD utilizando GitHub Actions
-  Deploy automatizado utilizando Databricks Asset Bundles
-  Orquestração utilizando Databricks Workflows
-  Dashboards analíticos e de observabilidade
-  Pipeline Observability

---

# 🏗️ Arquitetura

```text
                    Apache Kafka
                               |
                               |
                               v
                 Spark Structured Streaming
                    (Ingestion Notebook)
                               |
                               |
                               v
                     Raw Delta Table
                      (Kafka Events)
                               |
                               |
                               v
             Lakeflow Declarative Pipeline
                               |
               +---------------+---------------+
               |               |               |
               v               v               v
            Bronze          Silver           Gold
       Schema Parsing   CDC + Quality   Analytics &
                                       Observability
                               |

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

______

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

______

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

______

🥇 Gold Layer

Camada responsável pela disponibilização de dados analíticos e indicadores para consumo.

A partir dos dados confiáveis da Silver, foram criadas tabelas agregadas contendo métricas de negócio, análises dimensionais e informações de observabilidade do pipeline.

Implementações:
Métricas analíticas

Criação de agregações para análise dos pedidos:

Exemplo:
COUNT(orderid) AS total_orders,
SUM(orderunits) AS total_units,
AVG(orderunits) AS avg_units_per_order

Métricas e indicadores disponibilizados:

- Quantidade total de pedidos;
- Volume total de unidades processadas;
- Média, mínimo e máximo de unidades por pedido;
- Ranking de pedidos por cidade;
- Ranking de pedidos por estado;
- Eventos processados por janela de tempo;
- Latência média entre Bronze e Silver;
- Latência mínima entre Bronze e Silver;
- Latência máxima entre Bronze e Silver.

Tabelas analíticas criadas:

▦ gold_orders_metrics

Responsável por consolidar métricas gerais dos pedidos.

Exemplo:
time_window
total_orders
total_units
avg_units_per_order
min_units
max_units

▦ gold_city_ranking

Tabela destinada à análise de performance por cidade.

Exemplo:
state
city
order_day
total_orders
total_units
avg_units

Permite análises comparativas entre cidades através das métricas agregadas.

▦ gold_state_ranking

Tabela destinada à análise consolidada por estado.

Exemplo:
state
total_orders
total_units
avg_units
Permite identificar a distribuição dos pedidos e volume processado por região.

Observabilidade do Pipeline

Foi criada uma tabela para acompanhamento operacional do processamento:

▦ gold_pipeline_observability

Monitoramento de informações técnicas:

- Eventos processados por janela de tempo;
- Latência média entre Bronze e Silver;
- Latência máxima entre Bronze e Silver;
- Latência mínima entre Bronze e Silver;
- Evolução da latência ao longo do processamento.

______

## 🧪 Testes Automatizados

A pipeline possui testes automatizados desenvolvidos utilizando **Pytest**, executados durante o processo de **Continuous Integration (CI)**.

### Estrutura

```text
tests/
├── test_order_event_contract.py
├── test_bronze_schema.py
├── test_silver_schema.py
└── test_data_quality.py
```

Contrato do Evento Kafka

Validação do evento recebido utilizando **JSON Schema**, garantindo que o payload publicado no tópico Kafka esteja em conformidade com o contrato esperado.

Fluxo:

```text
Kafka Event
     |
     v
JSON Schema Validation
```

### Validação de Schema

Verificação da estrutura das camadas:

- Bronze;
- Silver.

As validações garantem:

- existência das colunas esperadas;
- tipos de dados corretos;
- conformidade com os schemas definidos.

### Data Quality

Foram implementadas validações para garantir a consistência dos dados processados:

- `orderunits` não pode possuir valores negativos;
- `city` deve estar preenchida (NOT NULL);
- `orderid` deve ser único.

Essas validações asseguram que apenas dados consistentes avancem pelas camadas da plataforma.

### Execução

```bash
pytest tests/
```

### Resultado esperado

```text
6 passed
```

Esses testes contribuem para aumentar a confiabilidade do pipeline, validando o contrato dos eventos Kafka, a conformidade dos schemas das camadas Bronze e Silver e as principais regras de qualidade dos dados antes do processo de deploy.

⸻

## 🔄 Orquestração

A execução da plataforma é realizada utilizando Databricks Workflows.

O Workflow executa duas tarefas sequenciais:

Notebook de Ingestão
        |
        v
Persistência na Raw Delta
        |
        v
Lakeflow Declarative Pipeline
        |
        +--> Bronze
        |
        +--> Silver
        |
        +--> Gold

O agendamento periódico permite que novos eventos publicados no Kafka sejam ingeridos automaticamente e disponibilizados para consumo analítico.

---

## 📊 Dashboards

Foram desenvolvidos dashboards utilizando Databricks Dashboards (Lakeview), permitindo acompanhar indicadores de negócio e métricas operacionais da plataforma.

### Business Analytics

- Total de pedidos
- Volume de unidades processadas
- Ranking por cidade
- Ranking por estado

### Pipeline Observability

- Eventos processados por janela
- Latência média Bronze → Silver
- Latência mínima
- Latência máxima
- Evolução temporal da latência

---

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

## 🚀 Resultados

A solução demonstra uma implementação completa de uma plataforma moderna de Engenharia de Dados, contemplando desde a ingestão streaming até a disponibilização dos dados para consumo analítico.

Durante o desenvolvimento foram aplicados conceitos de:

- Streaming de dados com Apache Kafka;
- Processamento distribuído com Apache Spark;
- Arquitetura Lakehouse utilizando Delta Lake;
- CDC com APPLY CHANGES INTO (SCD Type 1);
- Data Quality e Schema Enforcement;
- Testes automatizados;
- CI/CD;
- Orquestração com Databricks Workflows;
- Observabilidade de pipelines;
- Dashboards analíticos.

📂 Estrutura do Projeto

.
├── .github
│   └── workflows
│       ├── databricks-ci.yml
│       └── databricks-cd.yml
│
├── resources
│   └── kafka_pipeline.pipeline.yml
│
├── src
│   ├── notebooks
│   │   └── ingest_kafka.py
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
│   ├── schemas
│   │   ├── order_event_schema.json
│   │   ├── bronze_schema.json
│   │   └── silver_orders_schema.json
│   │
│   ├── test_order_event_contract.py
│   ├── test_bronze_schema.py
│   ├── test_silver_schema.py
│   ├── test_data_quality.py
│   └── test_gold_quality.py
│
├── databricks.yml
└── README.md


