import logging
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_values

from config import config

logger = logging.getLogger(__name__)


class StageRepository:
    def __init__(self) -> None:
        self.conn = psycopg2.connect(config.db_dsn)
        self.conn.autocommit = False

    def close(self) -> None:
        self.conn.close()

    def _deduplicar(self, registros: list[dict], pk: list[str]) -> list[dict]:
        """Remove duplicatas de PK dentro do lote, mantendo o registro mais recente."""
        vistos: dict[tuple, dict] = {}
        for reg in registros:
            chave = tuple(reg[c] for c in pk)
            atual = vistos.get(chave)
            if atual is None:
                vistos[chave] = reg
                continue
            nova = reg.get("data_hora_atualizacao_fonte") or ""
            antiga = atual.get("data_hora_atualizacao_fonte") or ""
            if nova >= antiga:
                vistos[chave] = reg
        return list(vistos.values())

    def _upsert(self, tabela: str, pk: list[str], registros: list[dict]) -> int:
        if not registros:
            return 0

        colunas = list(registros[0].keys())
        colunas_update = [c for c in colunas if c not in pk]

        registros = self._deduplicar(registros, pk)

        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas_update)
        conflict_cols = ", ".join(pk)
        cols_sql = ", ".join(colunas)

        # Compare all mutable fields: source timestamps alone can miss corrections.
        old_values = ", ".join(f"{tabela}.{c}" for c in colunas_update)
        new_values = ", ".join(f"EXCLUDED.{c}" for c in colunas_update)

        sql = f"""
            INSERT INTO {tabela} ({cols_sql})
            VALUES %s
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}
            WHERE ({old_values}) IS DISTINCT FROM ({new_values})
        """

        valores = [[reg[c] for c in colunas] for reg in registros]

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, valores, page_size=500)
                afetados = cur.rowcount
            self.conn.commit()
            return afetados
        except Exception:
            # Keep the long-lived connection usable for a later retry.
            self.conn.rollback()
            logger.exception("Falha no UPSERT de %s; lote revertido", tabela)
            raise

    def salvar_atas(self, registros: list[dict]) -> int:
        return self._upsert(
            "stage.arp_ata", ["numero_controle_pncp_ata"], registros
        )

    def salvar_itens(self, registros: list[dict]) -> int:
        return self._upsert(
            "stage.arp_item",
            ["numero_controle_pncp_ata", "numero_item"],
            registros,
        )
