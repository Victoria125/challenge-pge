"""Validation of the stage-to-fact batch: never silently replace invalid values with zero."""
from decimal import Decimal, InvalidOperation
import pandas as pd

FACT_COLUMNS = [
    "numero_controle_pncp_ata", "numero_item",
    "ni_fornecedor", "codigo_unidade_gerenciadora",
    "codigo_item", "data_vigencia_inicial",
    "quantidade_homologada_item", "valor_unitario", "valor_total",
]

def _decimal(value, column):
    if pd.isna(value):
        raise ValueError(f"{column} ausente: valor financeiro desconhecido nao e zero")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{column} invalido: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"{column} nao finito: {value!r}")
    return parsed

def preparar_lote(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.reindex(columns=FACT_COLUMNS)
    df = df.copy()
    for key in ("numero_controle_pncp_ata", "numero_item", "ni_fornecedor"):
        if df[key].isna().any() or (df[key].astype(str).str.strip() == "").any():
            raise ValueError(f"{key} ausente")
    for col in ("quantidade_homologada_item", "valor_unitario", "valor_total"):
        df[col] = df[col].apply(lambda x: _decimal(x, col))
    for col in ("codigo_unidade_gerenciadora", "codigo_item"):
        if df[col].isna().any():
            raise ValueError(f"{col} ausente")
        df[col] = pd.to_numeric(df[col], errors="raise")
        if not ((df[col] % 1) == 0).all():
            raise ValueError(f"{col} deve ser inteiro")
        df[col] = df[col].astype("int32")
    df["data_vigencia_inicial"] = pd.to_datetime(
        df["data_vigencia_inicial"], errors="raise"
    ).dt.date
    if df["data_vigencia_inicial"].isna().any():
        raise ValueError("data_vigencia_inicial ausente")
    return df[FACT_COLUMNS]
