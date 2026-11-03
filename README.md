# Price Registration Records Pipeline — PGE-CE

End-to-end pipeline for collecting, processing, and visualizing public data from **Price Registration Records (Atas de Registro de Preços)** available on the Compras.gov.br portal.

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCE                                 │
│          Compras.gov.br API (/modulo-arp)                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PART 1 — RPA + Stage                                               │
│                                                                     │
│  Python collector                                                   │
│  - requests + retry/backoff                                         │
│  - automatic pagination                                              │
│  - idempotent UPSERT (IS DISTINCT FROM)                             │
│                        │                                            │
│                        ▼                                            │
│            PostgreSQL (stage.arp_ata, stage.arp_item)               │
└────────────────────────┬────────────────────────────────────────────┘
                         │ WAL / CDC
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PART 2 — Real-Time Events                                          │
│                                                                     │
│  Debezium → Kafka (topic arp.stage.arp_item)                        │
│                        │                                            │
│                        ▼                                            │
│  Go Worker — ordered CDC consumer                                   │
│  - Kafka manual commits only after successful persistence           │
│  - retry/backoff + batch inserts into ClickHouse                     │
│  - string sanitization                                               │
│                        │                                            │
│                        ▼                                            │
│  ClickHouse (dim_fornecedores, dim_orgaos, dim_itens)               │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PART 3 — Batch + Orchestration                                     │
│                                                                     │
│  Apache Airflow DAG (fato_registro_precos)                          │
│  validar_fontes → construir_fato → validar_fato                     │
│                        │                                            │
│  Pandas Job: reads stage.arp_item, joins dimensions                 │
│                        │                                            │
│                        ▼                                            │
│  ClickHouse (fato_registro_precos)                                  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PART 4 — Dashboard                                                 │
│                                                                     │
│  Apache Superset connected to ClickHouse                            │
│  - Total approved financial volume                                  │
│  - Top 10 suppliers by volume                                       │
│  - Price history by period                                          │
│  - View by purchasing organizations                                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Stack

| Component              | Technology                                 |
| ---------------------- | ------------------------------------------ |
| Collection (RPA)       | Python 3.12, requests                      |
| Transactional database | PostgreSQL 16                              |
| CDC                    | Debezium 2.7 + Apache Kafka 7.6            |
| Event worker           | Go 1.22, segmentio/kafka-go, clickhouse-go |
| Analytical database    | ClickHouse 24.8                            |
| Orchestration          | Apache Airflow 2.9                         |
| Batch processing       | Pandas 2.2                                 |
| Dashboard              | Apache Superset 3.1                        |

## ERD — Entity-Relationship Diagram (Stage)

```text
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
   updated_at TIMESTAMP        ← monitored by Debezium
        │
        │ 1:N
        ▼
stage.arp_item
─────────────────────────────────
PK numero_controle_pncp_ata TEXT ──┐
PK numero_item TEXT                │ composite key
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
   updated_at TIMESTAMP        ← monitored by Debezium
```

## Star Schema (ClickHouse)

```text
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

## Start the Environment

```bash
docker compose up -d
```

Wait for all containers to become `healthy` (~2 min):

```bash
docker compose ps
```

## Register the CDC Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @arp-connector.json
```

## Run the Collector (Part 1)

```bash
docker compose up -d --build coletor
docker compose logs -f coletor
```

## Trigger the DAG (Part 3)

```bash
docker exec arp_airflow_scheduler airflow dags unpause fato_registro_precos
docker exec arp_airflow_scheduler airflow dags trigger fato_registro_precos
```

## Access the Services

| Service                  | URL                   | Credentials   |
| ------------------------ | --------------------- | ------------- |
| Redpanda Console (Kafka) | http://localhost:8080 | —             |
| Debezium Connect         | http://localhost:8083 | —             |
| Airflow                  | http://localhost:8085 | admin / admin |
| Superset                 | http://localhost:8088 | admin / admin |
| ClickHouse HTTP          | http://localhost:8123 | arp / arp     |
| PostgreSQL               | localhost:5432        | arp / arp     |

## Project Structure

```text
.
├── docker-compose.yml
├── arp-connector.json          # Debezium connector configuration
├── init-postgres/
│   ├── 01_schema.sql           # Stage DDL (arp_ata, arp_item)
│   └── 02_airflow_db.sql       # Airflow metadata database
├── init-clickhouse/
│   └── 01_schema.sql           # Dimension and fact DDL
├── coletor/                    # Part 1 — Python RPA
│   ├── main.py
│   ├── api_client.py
│   ├── transform.py
│   ├── repository.py
│   ├── config.py
│   ├── requirements.txt
│   └── Dockerfile
├── worker/                     # Part 2 — Go Worker
│   ├── main.go
│   ├── go.mod
│   ├── Dockerfile
│   └── internal/
│       ├── model/
│       ├── config/
│       ├── transform/
│       ├── clickhouse/
│       └── kafka/
└── airflow/                    # Part 3 — Orchestration
    ├── Dockerfile
    ├── requirements.txt
    └── dags/
        └── dag_fato_registro_precos.py
```

## CDC worker reliability (Go)

The worker now uses **one ordered fetch/processing loop** and still writes
**batches** to ClickHouse. This deliberately trades parallel throughput for
correct offset handling until partition-aware concurrency is implemented.

- Uses \`FetchMessage\` (not \`ReadMessage\`): Kafka does not acknowledge records on fetch.
- Persists supplier, agency and item dimensions **before** confirming the batch offsets.
- Retries failed inserts with exponential backoff; after all attempts fail, the
  process exits non-zero and Docker's \`restart: on-failure\` allows Kafka to
  replay from the last confirmed offset.
- Retries a failed Kafka commit separately, **without rewriting the batch**.
- Preserves ordered processing/contiguous commits within each Kafka partition.
- Rejects malformed events, CDC deletes and \`item_excluido=true\` **without
  committing their offsets**. These records require explicit implementation or
  operator intervention; otherwise the worker intentionally stops rather than
  silently dropping changes.
- Kafka tombstones (empty/null message values) are deliberately acknowledged
  after the pending batch has been persisted.

Configuration: \`BATCH_SIZE\`, \`BATCH_TIMEOUT_MS\`, \`RETRY_ATTEMPTS\`,
\`RETRY_BACKOFF_MS\`, \`WRITE_TIMEOUT_MS\`.

**Delivery guarantee:** at-least-once, **not** exactly-once. ClickHouse uses
\`ReplacingMergeTree\`, which merges repeated keys eventually rather than
enforcing immediate uniqueness. Replays after partial dimension writes or failed
offset commits can temporarily create duplicates. For strict version ordering
and production deployment, add deterministic source versions, correct delete
handling, consistency checks (e.g. \`FINAL\` or suitable \`argMax\` queries),
an explicit DLQ policy, and load/failure-injection tests.

Unit tests, including failed ClickHouse writes, commit failures, invalid CDC
events and replays:

\`\`\`bash
cd worker
go mod tidy
go test ./...
\`\`\`

## Demo Video

[Coming soon]

```
```
