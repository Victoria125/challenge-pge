"""Fact transformation tests, independent of the Airflow scheduler."""
import pathlib
import sys
import unittest
from decimal import Decimal

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
from fato_transform import preparar_lote, FACT_COLUMNS


def row():
    return dict(
        numero_controle_pncp_ata="ARP-1", numero_item="1",
        ni_fornecedor="123", codigo_unidade_gerenciadora=42,
        codigo_item=7, data_vigencia_inicial="2026-06-20",
        quantidade_homologada_item="5.0", valor_unitario="12.25",
        valor_total="61.25",
    )


class TestFactValidation(unittest.TestCase):
    def test_valid_row_keeps_decimal_and_schema(self):
        df = preparar_lote(pd.DataFrame([row()]))
        self.assertEqual(list(df.columns), FACT_COLUMNS)
        self.assertEqual(df.iloc[0]["valor_total"], Decimal("61.25"))
        self.assertEqual(str(df.iloc[0]["data_vigencia_inicial"]), "2026-06-20")

    def test_missing_money_is_not_silently_zero(self):
        record = row()
        record["valor_total"] = None
        with self.assertRaisesRegex(ValueError, "ausente"):
            preparar_lote(pd.DataFrame([record]))

    def test_non_numeric_money_is_rejected(self):
        record = row()
        record["valor_unitario"] = "erro"
        with self.assertRaisesRegex(ValueError, "invalido"):
            preparar_lote(pd.DataFrame([record]))

    def test_null_or_invalid_date_is_rejected(self):
        for invalid in [None, "not a date"]:
            record = row()
            record["data_vigencia_inicial"] = invalid
            with self.assertRaises((ValueError, TypeError)):
                preparar_lote(pd.DataFrame([record]))

    def test_fractional_identifier_is_rejected(self):
        record = row()
        record["codigo_item"] = 1.1
        with self.assertRaisesRegex(ValueError, "inteiro"):
            preparar_lote(pd.DataFrame([record]))


if __name__ == "__main__":
    unittest.main()
