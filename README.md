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

A partir dos dados confiáveis da Silver, foram criadas tabelas agregadas contendo métricas de negócio, análises dimensionais e informações de observabilidade do pipeline.

Implementações:
Métricas analíticas

Criação de agregações para análise dos pedidos:

Exemplo:
COUNT(orderid) AS total_orders,
SUM(orderunits) AS total_units,
AVG(orderunits) AS avg_units_per_order

Indicadores gerados:

Quantidade total de pedidos;
Volume total de itens processados;
Média de unidades por pedido;
Valores mínimo e máximo de unidades;
Distribuição dos pedidos por localização.

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


Data Quality

Foram adicionadas validações automatizadas para garantir a consistência dos dados analíticos da camada Gold.

Os testes são executados no pipeline de CI utilizando Pytest.

Validações realizadas:

Volume de dados

Garantia de que as tabelas analíticas possuem registros após o processamento.

Exemplo:
assert len(data) > 0

Consistência das métricas

Validação das regras de negócio:

Valores de pedidos e unidades não podem ser negativos;
Média de unidades deve ser consistente com total de unidades e quantidade de pedidos;
Valores mínimos não podem ser superiores aos valores máximos.

Exemplo:
assert min_units <= max_units

Qualidade das agregações

Validação das tabelas analíticas:

Campos obrigatórios (state, city) devem estar preenchidos;
Métricas agregadas devem possuir valores válidos;
Integridade das agregações por cidade e estado.

Resultado:
A camada Gold disponibiliza dados preparados para consumo analítico, garantindo confiabilidade através de métricas de negócio e testes automatizados de qualidade.


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

- Evolução das métricas analíticas da camada Gold;
- Expansão dos testes de qualidade para regras de negócio;
- Implementação de dashboards de monitoramento e observabilidade;
- Configuração de múltiplos ambientes utilizando Databricks Asset Bundles;
- Integração com ferramentas de visualização e monitoramento.



