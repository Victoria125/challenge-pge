import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    db_host: str = os.getenv("DB_HOST", "postgres")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "arp_stage")
    db_user: str = os.getenv("DB_USER", "arp")
    db_password: str = os.getenv("DB_PASSWORD", "arp")

    api_base_url: str = os.getenv(
        "API_BASE_URL", "https://dadosabertos.compras.gov.br"
    )

    data_vigencia_min: str = os.getenv("DATA_VIGENCIA_MIN", "2026-01-01")
    data_vigencia_max: str = os.getenv("DATA_VIGENCIA_MAX", "2026-01-31")

    tamanho_pagina: int = int(os.getenv("TAMANHO_PAGINA", "500"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))
    backoff_factor: float = float(os.getenv("BACKOFF_FACTOR", "1.5"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "60"))

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


config = Config()
