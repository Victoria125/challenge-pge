import logging
import sys
from itertools import islice
from typing import Iterator

import psycopg2
import requests

from api_client import ComprasAPIClient
from repository import StageRepository
from transform import normalizar_ata, normalizar_item

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("coletor")

BATCH_SIZE = 500


def em_lotes(iteravel: Iterator, tamanho: int) -> Iterator[list]:
    it = iter(iteravel)
    while lote := list(islice(it, tamanho)):
        yield lote


def coletar_atas(api: ComprasAPIClient, repo: StageRepository) -> None:
    total = 0
    for lote in em_lotes(api.consultar_atas(), BATCH_SIZE):
        registros = [normalizar_ata(r) for r in lote]
        registros = [r for r in registros if r["numero_controle_pncp_ata"]]
        afetados = repo.salvar_atas(registros)
        total += len(registros)
        logger.info("Atas: lote de %d processado (%d alteracoes no banco).",
                    len(registros), afetados)
    logger.info("Total de atas coletadas: %d", total)


def coletar_itens(api: ComprasAPIClient, repo: StageRepository) -> None:
    total = 0
    for lote in em_lotes(api.consultar_itens(), BATCH_SIZE):
        registros = [normalizar_item(r) for r in lote]
        registros = [
            r for r in registros
            if r["numero_controle_pncp_ata"] and r["numero_item"]
        ]
        afetados = repo.salvar_itens(registros)
        total += len(registros)
        logger.info("Itens: lote de %d processado (%d alteracoes no banco).",
                    len(registros), afetados)
    logger.info("Total de itens coletados: %d", total)


def main() -> None:
    logger.info("Iniciando coleta de Atas de Registro de Preco.")
    api = ComprasAPIClient()
    repo = StageRepository()
    try:
        coletar_atas(api, repo)
        coletar_itens(api, repo)
        logger.info("Coleta concluida com sucesso.")
    except (requests.RequestException, psycopg2.Error, ValueError) as exc:
        logger.exception("Coleta interrompida por falha esperada: %s", exc)
        raise  # Error reaches the orchestrator / container exit code.
    finally:
        repo.close()


if __name__ == "__main__":
    main()
