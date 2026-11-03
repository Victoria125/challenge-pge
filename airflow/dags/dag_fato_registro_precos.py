from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import clickhouse_connect
import psycopg2

from airflow.decorators import dag, task
from fato_transform import FACT_COLUMNS, preparar_lote

log = logging.getLogger(__name__)

PG_CONN = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "arp_stage"),
    "user": os.getenv("DB_USER", "arp"),
    "password": os.getenv("DB_PASSWORD", "arp"),
}

CH_CONN = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "port": int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    "database": os.getenv("CLICKHOUSE_DB", "arp_analytics"),
    "username": os.getenv("CLICKHOUSE_USER", "arp"),
    "password": os.getenv("CLICKHOUSE_PASS", "arp"),
}

# Stage filters are intentional: excluded items are retained in dimensions,
# but do not contribute to financial facts.
STAGE_FILTER = """
    item_excluido = false
    AND ni_fornecedor IS NOT NULL
    AND ni_fornecedor <> ''
    AND codigo_item IS NOT NULL
    AND valor_total IS NOT NULL
"""

SELECT_STAGE = f"""
    SELECT {", ".join(FACT_COLUMNS)}
    FROM stage.arp_item
    WHERE {STAGE_FILTER}
    ORDER BY numero_controle_pncp_ata, numero_item
"""

def validar_stage():
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM stage.arp_item WHERE {STAGE_FILTER}")
            total = int(cur.fetchone()[0])
    if total == 0:
        raise ValueError("Nenhum item elegivel na stage; nao substituir a fato")
    return total

def montar_snapshot(esperado: int) -> int:
    client = clickhouse_connect.get_client(**CH_CONN)
    # EXCHANGE requires Atomic databases and keeps the old fact available
    # until the new snapshot has been loaded and validated.
    db_engine = client.command(
        "SELECT engine FROM system.databases WHERE name = 'arp_analytics'"
    )
    if db_engine != "Atomic":
        raise RuntimeError("EXCHANGE TABLES requer banco ClickHouse Atomic")

    client.command(
        "CREATE TABLE IF NOT EXISTS fato_registro_precos_next AS fato_registro_precos"
    )
    client.command("TRUNCATE TABLE fato_registro_precos_next")
    total = 0

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor(name="stream_fato_arp") as cur:
            cur.itersize = 1000
            cur.execute(SELECT_STAGE)
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                df = pd.DataFrame.from_records(rows, columns=FACT_COLUMNS)
                df = preparar_lote(df)
                client.insert_df("fato_registro_precos_next", df)
                total += len(df)

    staged = int(client.command("SELECT count() FROM fato_registro_precos_next"))
    if total != esperado or staged != esperado:
        raise ValueError(
            f"Snapshot incompleto: esperado={esperado}, lido={total}, inserido={staged}"
        )

    # One atomic metadata exchange: readers see old or complete new snapshot.
    client.command("EXCHANGE TABLES fato_registro_precos AND fato_registro_precos_next")
    log.info("Snapshot ativado com %d registros", staged)
    return staged

@dag(
    dag_id="fato_registro_precos",
    description="Carrega e valida snapshot da fato antes de troca atomica",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["pge-ce", "parte3", "star-schema"],
)
def pipeline_fato():

    @task
    def validar_fontes():
        total = validar_stage()
        ch = clickhouse_connect.get_client(**CH_CONN)
        if int(ch.command("SELECT count() FROM dim_fornecedores")) <= 0:
            raise ValueError("dim_fornecedores vazia")
        return total

    @task
    def construir_fato(total: int):
        return montar_snapshot(total)

    @task
    def validar_fato(total: int):
        ch = clickhouse_connect.get_client(**CH_CONN)
        total_fato = int(ch.command("SELECT count() FROM fato_registro_precos"))
        if total_fato != total:
            raise ValueError(f"Fato incompleta: {total_fato}, esperado: {total}")
        volume = ch.command("SELECT sum(valor_total) FROM fato_registro_precos")
        log.info("Linhas=%d; volume financeiro=R$ %s", total_fato, volume)

    validar_fato(construir_fato(validar_fontes()))

pipeline_fato()
