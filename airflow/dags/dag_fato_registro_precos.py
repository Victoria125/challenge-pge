from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
import clickhouse_connect
import psycopg2

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "arp_stage",
    "user": "arp",
    "password": "arp",
}

CH_CONN = {
    "host": "clickhouse",
    "port": 8123,
    "database": "arp_analytics",
    "username": "arp",
    "password": "arp",
}


@dag(
    dag_id="fato_registro_precos",
    description="Constroi a tabela fato_registro_precos no ClickHouse a partir do Stage",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pge-ce", "parte3", "star-schema"],
)
def pipeline_fato():

    @task
    def validar_fontes():
        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM stage.arp_item")
        total_itens = cur.fetchone()[0]
        cur.close()
        conn.close()

        ch = clickhouse_connect.get_client(**CH_CONN)
        total_forn = ch.command("SELECT count() FROM dim_fornecedores")

        log.info("Stage arp_item: %d linhas", total_itens)
        log.info("dim_fornecedores: %d", total_forn)

        if total_itens == 0:
            raise ValueError("stage.arp_item vazia.")
        if total_forn == 0:
            raise ValueError("dim_fornecedores vazia.")

        return {"total_stage": total_itens}

    @task
    def construir_fato(info: dict):
        conn = psycopg2.connect(**PG_CONN)
        df = pd.read_sql("""
            SELECT
                numero_controle_pncp_ata,
                numero_item,
                ni_fornecedor,
                codigo_unidade_gerenciadora,
                codigo_item,
                data_vigencia_inicial,
                quantidade_homologada_item,
                valor_unitario,
                valor_total
            FROM stage.arp_item
            WHERE item_excluido = false
              AND ni_fornecedor IS NOT NULL
              AND codigo_item IS NOT NULL
              AND valor_total IS NOT NULL
        """, conn)
        conn.close()

        log.info("Linhas carregadas: %d", len(df))

        df["quantidade_homologada_item"] = pd.to_numeric(
            df["quantidade_homologada_item"], errors="coerce").fillna(0)
        df["valor_unitario"] = pd.to_numeric(
            df["valor_unitario"], errors="coerce").fillna(0)
        df["valor_total"] = pd.to_numeric(
            df["valor_total"], errors="coerce").fillna(0)
        df["codigo_unidade_gerenciadora"] = df["codigo_unidade_gerenciadora"].fillna(0).astype(int)
        df["codigo_item"] = df["codigo_item"].fillna(0).astype(int)
        df["data_vigencia_inicial"] = pd.to_datetime(
            df["data_vigencia_inicial"], errors="coerce").dt.date
        df = df.dropna(subset=["data_vigencia_inicial"])

        log.info("Linhas validas: %d", len(df))

        ch = clickhouse_connect.get_client(**CH_CONN)
        ch.command("TRUNCATE TABLE IF EXISTS fato_registro_precos")

        colunas = [
            "numero_controle_pncp_ata", "numero_item",
            "ni_fornecedor", "codigo_unidade_gerenciadora",
            "codigo_item", "data_vigencia_inicial",
            "quantidade_homologada_item", "valor_unitario", "valor_total",
        ]
        ch.insert_df("fato_registro_precos", df[colunas])

        log.info("fato_registro_precos populada com %d linhas.", len(df))
        return len(df)

    @task
    def validar_fato(total_inserido: int):
        ch = clickhouse_connect.get_client(**CH_CONN)
        total_fato = ch.command("SELECT count() FROM fato_registro_precos")
        volume_total = ch.command("SELECT round(sum(valor_total), 2) FROM fato_registro_precos")

        log.info("Linhas na fato: %d", total_fato)
        log.info("Volume financeiro total: R$ %s", volume_total)

        if total_fato < total_inserido * 0.95:
            raise ValueError(f"Contagem na fato ({total_fato}) abaixo do esperado ({total_inserido}).")

        log.info("Validacao OK.")

    info = validar_fontes()
    total = construir_fato(info)
    validar_fato(total)


pipeline_fato()
