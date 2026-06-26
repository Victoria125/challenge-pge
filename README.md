# Pipeline de Atas de Registro de Preços — PGE-CE

Pipeline end-to-end para coleta, processamento e visualização de dados públicos de **Atas de Registro de Preços** do portal Compras.gov.br.

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FONTE DE DADOS                              │
│          API Compras.gov.br (/modulo-arp)                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PARTE 1 — RPA + Stage                                              │
│                                                                     │
│  coletor Python                                                     │
│  - requests + retry/backoff                                         │
│  - paginação automática                                             │
│  - UPSERT idempotente (IS DISTINCT FROM)                            │
│                        │                                            │
│                        ▼                                            │
│            PostgreSQL (stage.arp_ata, stage.arp_item)               │
└────────────────────────┬────────────────────────────────────────────┘
                         │ WAL / CDC
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PARTE 2 — Eventos em Tempo Real                                    │
│                                                                     │
│  Debezium → Kafka (tópico arp.stage.arp_item)                       │
│                        │                                            │
│                        ▼                                            │
│  Worker Go — pool de goroutines (produtor-consumidor)               │
│  - 4 goroutines em paralelo                                         │
│  - higienização de strings                                          │
│  - insert em lote no ClickHouse                                     │
│                        │                                            │
│                        ▼                                            │
│  ClickHouse (dim_fornecedores, dim_orgaos, dim_itens)               │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PARTE 3 — Batch + Orquestração                                     │
│                                                                     │
│  Apache Airflow DAG (fato_registro_precos)                          │
│  validar_fontes → construir_fato → validar_fato                     │
│                        │                                            │
│  Job Pandas: lê stage.arp_item, cruza dimensões                     │
│                        │                                            │
│                        ▼                                            │
│  ClickHouse (fato_registro_precos)                                  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PARTE 4 — Dashboard                                                │
│                                                                     │
│  Apache Superset conectado ao ClickHouse                            │
│  - Volume financeiro total homologado                               │
│  - Top 10 fornecedores por volume                                   │
│  - Histórico de preços por período                                  │
│  - Visão por órgãos compradores                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Stack

| Componente | Tecnologia |
|---|---|
| Coleta (RPA) | Python 3.12, requests |
| Banco transacional | PostgreSQL 16 |
| CDC | Debezium 2.7 + Apache Kafka 7.6 |
| Worker de eventos | Go 1.22, segmentio/kafka-go, clickhouse-go |
| Banco analítico | ClickHouse 24.8 |
| Orquestração | Apache Airflow 2.9 |
| Processamento batch | Pandas 2.2 |
| Dashboard | Apache Superset 3.1 |

## DER — Diagrama Entidade-Relacionamento (Stage)

```
stage.arp_ata
─────────────────────────────────
PK numero_controle_pncp_ata TEXT
   numero_ata_registro_preco TEXT
   codigo_unidade_gerenciadora TEXT
   nome_unidade_gerenciadora TEXT
   objeto TEXT
   valor_total NUMERIC
   status_ata TEXT
   data_assinatura DATE
   data_vigencia_inicial DATE
   data_vigencia_final DATE
   ingested_at TIMESTAMP
   updated_at TIMESTAMP        ← monitorado pelo Debezium
        │
        │ 1:N
        ▼
stage.arp_item
─────────────────────────────────
PK numero_controle_pncp_ata TEXT ──┐
PK numero_item TEXT                │ chave composta
   codigo_item INTEGER             │
   descricao_item TEXT             │
   tipo_item TEXT                  │
   ni_fornecedor TEXT              │
   nome_razao_social_fornecedor TEXT
   valor_unitario NUMERIC
   valor_total NUMERIC
   quantidade_homologada_item NUMERIC
   data_vigencia_inicial DATE
   ingested_at TIMESTAMP
   updated_at TIMESTAMP        ← monitorado pelo Debezium
```

## Star Schema (ClickHouse)

```
              dim_fornecedores
              ─────────────────
              ni_fornecedor (PK)
              nome_razao_social
              classificacao_fornecedor

dim_orgaos         fato_registro_precos        dim_itens
──────────    ◄──  ────────────────────  ──►   ─────────
codigo_unid.       numero_controle_pncp_ata    codigo_item (PK)
nome_unid.         numero_item                 descricao_item
                   ni_fornecedor (FK)          tipo_item
                   codigo_unid_ger. (FK)       codigo_pdm
                   codigo_item (FK)            nome_pdm
                   data_vigencia_inicial (FK)
                   valor_unitario              dim_tempo
                   valor_total            ──►  ─────────
                   qtd_homologada              data (PK)
                                               ano, mes, trimestre
```

## Subir o ambiente

```bash
docker compose up -d
```

Aguarda todos os containers ficarem `healthy` (~2 min):

```bash
docker compose ps
```

## Registrar o conector CDC

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @arp-connector.json
```

## Executar a coleta (Parte 1)

```bash
docker compose up -d --build coletor
docker compose logs -f coletor
```

## Disparar a DAG (Parte 3)

```bash
docker exec arp_airflow_scheduler airflow dags unpause fato_registro_precos
docker exec arp_airflow_scheduler airflow dags trigger fato_registro_precos
```

## Acessar os serviços

| Serviço | URL | Credenciais |
|---|---|---|
| Redpanda Console (Kafka) | http://localhost:8080 | — |
| Debezium Connect | http://localhost:8083 | — |
| Airflow | http://localhost:8085 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | arp / arp |
| PostgreSQL | localhost:5432 | arp / arp |

## Estrutura do projeto

```
.
├── docker-compose.yml
├── arp-connector.json          # configuração do conector Debezium
├── init-postgres/
│   ├── 01_schema.sql           # DDL do stage (arp_ata, arp_item)
│   └── 02_airflow_db.sql       # banco de metadados do Airflow
├── init-clickhouse/
│   └── 01_schema.sql           # DDL das dimensões e fato
├── coletor/                    # Parte 1 — RPA Python
│   ├── main.py
│   ├── api_client.py
│   ├── transform.py
│   ├── repository.py
│   ├── config.py
│   ├── requirements.txt
│   └── Dockerfile
├── worker/                     # Parte 2 — Worker Go
│   ├── main.go
│   ├── go.mod
│   ├── Dockerfile
│   └── internal/
│       ├── model/
│       ├── config/
│       ├── transform/
│       ├── clickhouse/
│       └── kafka/
└── airflow/                    # Parte 3 — Orquestração
    ├── Dockerfile
    ├── requirements.txt
    └── dags/
        └── dag_fato_registro_precos.py
```

## Vídeo de demonstração

[Em breve]
