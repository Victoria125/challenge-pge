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

## Exportar o dashboard real do Superset


**Antes de recriar ou remover o container Superset que contem o dashboard**, execute na sua maquina:

    bash scripts/export_superset.sh

O script exporta, verifica a presenca de dashboards e salva o ZIP real em superset/exports/arp-dashboard.zip, sem interromper o container. Revise o ZIP em busca de informacoes sensiveis **antes de publica-lo**. Uma vez incluido no repositorio, o bootstrap existente importa esse ZIP na inicializacao. Veja as instrucoes e riscos de persistencia em [superset/exports/README.md](superset/exports/README.md).

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

- Uses `FetchMessage` (not `ReadMessage`): Kafka does not acknowledge records on fetch.
- Persists supplier, agency and item dimensions **before** confirming the batch offsets.
- Retries failed inserts with exponential backoff; after all attempts fail, the
  process exits non-zero and Docker's `restart: on-failure` allows Kafka to
  replay from the last confirmed offset.
- Retries a failed Kafka commit separately, **without rewriting the batch**.
- Preserves ordered processing/contiguous commits within each Kafka partition.
- Processes `item_excluido=true` as a **valid dimension event**: excluded
  facts do not delete the corresponding supplier/agency/item dimensions.
- Logs CDC `op=d` deletes and acknowledges them **without removing dimensions**.
  The fact DAG reads the current stage, excluding removed and soft-deleted rows.
- Sends malformed JSON, invalid `after` and unsupported CDC operations to a
  dedicated Kafka DLQ (`arp.stage.arp_item.dlq`). The source offset advances
  **only after** Kafka acknowledges the DLQ publication and any prior ClickHouse
  batch writes succeed. A temporary DLQ outage is retried; if exhausted, the
  uncommitted event is replayed after restart instead of disappearing.
- Uses `snapshot.mode=initial` instead of `always` for Debezium; existing
  running connectors must be updated, not merely restarted.
- Kafka tombstones (empty/null message values) are deliberately acknowledged
  after the pending batch has been persisted.

Configuration: `BATCH_SIZE`, `BATCH_TIMEOUT_MS`, `RETRY_ATTEMPTS`,
`RETRY_BACKOFF_MS`, `WRITE_TIMEOUT_MS`.

**Delivery guarantee:** at-least-once, **not** exactly-once. ClickHouse uses
`ReplacingMergeTree`, which merges repeated keys eventually rather than
enforcing immediate uniqueness. Replays after partial dimension writes or failed
offset commits can temporarily create duplicates. For strict version ordering
and production deployment, add deterministic source versions, correct delete
handling, consistency checks (e.g. `FINAL` or suitable `argMax` queries),
an explicit DLQ policy, and load/failure-injection tests.

Unit tests, including failed ClickHouse writes, commit failures, invalid CDC
events and replays:

```bash
cd worker
go mod tidy
go test ./...
```

## Known limitations and operational checks

- **Collector is not a network-incremental crawler**: the documented Compras.gov.br
  listing endpoints filter by validity dates. No verified, global
  last-updated cursor is available for a safe delta scan. We keep the
  configured full-window fetch rather than risking missing corrections.
  PostgreSQL now updates rows only when **any mutable field changed**, and
  rolls back failed batches.
- **Fact snapshot is atomic for readers**: load into
  `fato_registro_precos_next`, reject unknown/invalid numeric fields
  instead of coercing them to zero, verify counts, then atomically exchange
  tables using the ClickHouse `Atomic` database engine. This avoids an
  empty live fact after an insert failure. Runs are serialized by Airflow.
- **Superset** runs using Gunicorn, without Flask `--reload/--debugger`.
  An authentic export ZIP, if present at
  `superset/exports/arp-dashboard.zip`, is imported on startup.
  The **original dashboard is still absent** from this repository;
  see `superset/exports/README.md` for the export process. We do not
  claim to reproduce Part 4 until the ZIP is committed and tested.
- **DLQ replay:** DLQ writes can be duplicated if ClickHouse or Kafka commit
  fails later. Deduplicate for reprocessing using
  `source_topic + partition + offset`. The local single-broker Kafka is
  only a development setup, not an HA durable deployment.
- **At-least-once, not exactly-once:** `ReplacingMergeTree` deduplication is
  eventual. Source-defined versions, hard-deletion semantics, and full
  integration tests are further production work.
- Update an already-registered connector after changing
  `arp-connector.json`, for example:

  ```bash
  python -c 'import json; print(json.dumps(json.load(open("arp-connector.json"))["config"]))' > /tmp/arp-config.json
  curl -X PUT -H "Content-Type: application/json" \
    --data-binary @/tmp/arp-config.json \
    http://localhost:8083/connectors/arp-postgres-connector/config
  ```

Run the tests:

```bash
cd worker && go test ./...
cd .. && python -m unittest discover -s coletor/tests -v
python -m unittest discover -s airflow/tests -v
```

**Evidence from public PGE-related GitHub code:** 
`devjuliomesquita/Desafio_Mais_Cond` separates Kafka listeners
from application services, and
`devjuliomesquita/seed-desafio-cdc` uses explicit transactions and
propagates persistence errors. `Andressa-Evelyn/compras-gov-data-pipeline`
implements a similar end-to-end pipeline. The DLQ protocol, atomic swap and
safe Kafka commit rules in *this repository* are engineering adaptations,
**not verified production implementations by those developers**.

## Demo Video

[Coming soon]

```
```
