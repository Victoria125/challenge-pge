# Desafio Técnico - Pipeline de Atas de Registro de Preços

Infraestrutura base do pipeline end-to-end. Este estágio sobe apenas os
serviços para garantir que tudo se comunica antes de escrever a lógica 
de coleta e processamento.

## Serviços

| Serviço          | Porta(s)       | Para quê                                  |
|------------------|----------------|-------------------------------------------|
| PostgreSQL       | 5432           | Banco transacional (área de Stage)        |
| Kafka            | 9092           | Barramento de mensageria                  |
| Debezium Connect | 8083           | Captura de alterações (CDC) do Postgres   |
| Redpanda Console | 8080           | UI para inspecionar tópicos do Kafka      |
| ClickHouse       | 8123 / 9000    | Banco analítico (dimensões e fato)        |

## Subir o ambiente

```bash
docker compose up -d
```

Aguarde todos os healthchecks ficarem saudáveis:

```bash
docker compose ps
```

Os scripts em `init-postgres/` e `init-clickhouse/` rodam automaticamente na
primeira inicialização, criando o schema `stage` no Postgres e as tabelas
analíticas no ClickHouse.

## Registrar o conector Debezium

Depois que os serviços estiverem de pé (Debezium leva ~30s para aceitar
conexões), registre o conector que monitora as tabelas de stage:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/arp-connector.json
```

Verifique o status:

```bash
curl http://localhost:8083/connectors/arp-postgres-connector/status
```

## Validar que tudo conversa

PostgreSQL :

```bash
docker exec -it arp_postgres psql -U arp -d arp_stage -c "\dt stage.*"
```

ClickHouse :

```bash
docker exec -it arp_clickhouse clickhouse-client -u arp --password arp \
  -q "SHOW TABLES FROM arp_analytics"
```

Teste de ponta a ponta do CDC (insere uma linha e confirma no Kafka):

```bash
# 1. inserir uma linha de teste no Postgres
docker exec -it arp_postgres psql -U arp -d arp_stage -c \
  "INSERT INTO stage.arp_item (numero_controle_pncp_ata, numero_item, descricao_item, valor_total)
   VALUES ('TESTE-001', '1', 'item de teste', 100.00);"

# 2. abrir o Redpanda Console em http://localhost:8080 e procurar o tópico
#    arp.stage.arp_item 
```

## Derrubar tudo

```bash
docker compose down          
docker compose down -v       
```

## Próximas etapas

1. **Parte 1** — robô Python que coleta da API do Compras.gov.br
   (`/modulo-arp/...`) e popula `stage.arp_ata` e `stage.arp_item` com UPSERT.
2. **Parte 2** — worker em Go consumindo `arp.stage.arp_item` do Kafka,
   higienizando e gravando nas dimensões do ClickHouse.
3. **Parte 3** — DAG do Airflow + job PySpark, Pandas montando
   `fato_registro_precos`.
4. **Parte 4** — dashboard Superset sobre o ClickHouse.
