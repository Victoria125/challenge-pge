import logging
import time
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import config

logger = logging.getLogger(__name__)


class ComprasAPIClient:
    def __init__(self) -> None:
        self.base_url = config.api_base_url
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=config.max_retries,
            backoff_factor=config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _paginar(self, endpoint: str, params: dict) -> Iterator[dict]:
        pagina = 1
        while True:
            params_pagina = {**params, "pagina": pagina,
                             "tamanhoPagina": config.tamanho_pagina}
            url = f"{self.base_url}{endpoint}"

            try:
                resp = self.session.get(
                    url, params=params_pagina, timeout=config.request_timeout
                )
                resp.raise_for_status()
            except requests.RequestException as exc:
                logger.error("Falha ao buscar pagina %d de %s: %s",
                            pagina, endpoint, exc)
                raise

            payload = resp.json()
            registros = payload.get("resultado", []) or []

            if not registros:
                logger.info("Pagina %d vazia, encerrando %s.", pagina, endpoint)
                break

            logger.info("Pagina %d/%s de %s: %d registros.",
                       pagina, payload.get("totalPaginas", "?"),
                       endpoint, len(registros))

            yield from registros

            if payload.get("paginasRestantes", 0) <= 0:
                break
            pagina += 1
            time.sleep(0.2)

    def consultar_atas(self) -> Iterator[dict]:
        params = {
            "dataVigenciaInicialMin": config.data_vigencia_min,
            "dataVigenciaInicialMax": config.data_vigencia_max,
        }
        yield from self._paginar("/modulo-arp/1_consultarARP", params)

    def consultar_itens(self) -> Iterator[dict]:
        params = {
            "dataVigenciaInicialMin": config.data_vigencia_min,
            "dataVigenciaInicialMax": config.data_vigencia_max,
        }
        yield from self._paginar("/modulo-arp/2_consultarARPItem", params)
