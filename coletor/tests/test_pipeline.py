"""Collector regression tests (no network or database required)."""
import pathlib
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import psycopg2
from api_client import ComprasAPIClient
from main import em_lotes
from repository import StageRepository
from transform import limpar_numero, limpar_booleano, normalizar_item
from decimal import Decimal


class TestCollector(unittest.TestCase):
    def test_money_preserves_decimal_precision(self):
        self.assertEqual(limpar_numero("0E-8"), Decimal("0E-8"))
        self.assertEqual(limpar_numero("123456789.1234"), Decimal("123456789.1234"))
        with self.assertRaises(ValueError):
            limpar_numero("invalido")

    def test_boolean_excluded_does_not_treat_string_false_as_true(self):
        self.assertFalse(limpar_booleano("false"))
        self.assertTrue(limpar_booleano("true"))
        self.assertFalse(normalizar_item({"itemExcluido": "false"})["item_excluido"])

    def test_batches_dont_discard_last_partial_batch(self):
        self.assertEqual(list(em_lotes(iter(range(5)), 2)), [[0, 1], [2, 3], [4]])

    def test_api_pagination(self):
        client = ComprasAPIClient()
        first = MagicMock()
        first.json.return_value = {
            "resultado": [{"numeroControlePncpAta": "a"}], "paginasRestantes": 1,
        }
        second = MagicMock()
        second.json.return_value = {
            "resultado": [{"numeroControlePncpAta": "b"}], "paginasRestantes": 0,
        }
        client.session.get = MagicMock(side_effect=[first, second])
        with patch("api_client.time.sleep"):
            data = list(client.consultar_atas())
        self.assertEqual(len(data), 2)
        self.assertEqual(client.session.get.call_count, 2)
        self.assertEqual(
            client.session.get.call_args_list[1].kwargs["params"]["pagina"], 2
        )

    def test_upsert_rolls_back_if_postgres_fails(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.rowcount = 1
        with patch("repository.psycopg2.connect", return_value=connection):
            repo = StageRepository()
        data = [{"numero_controle_pncp_ata": "ATA", "numero_item": "1",
                 "descricao_item": "original", "data_hora_atualizacao_fonte": "2026-01-01"}]
        with patch("repository.execute_values", side_effect=psycopg2.IntegrityError("failed")):
            with self.assertRaises(psycopg2.Error):
                repo.salvar_itens(data)
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()

    def test_upsert_detects_changes_to_fields_even_if_timestamp_unchanged(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.rowcount = 1
        with patch("repository.psycopg2.connect", return_value=connection):
            repo = StageRepository()
        data = [{"numero_controle_pncp_ata": "ATA", "numero_item": "1",
                 "descricao_item": "updated", "data_hora_atualizacao_fonte": "2026-01-01"}]
        with patch("repository.execute_values") as executor:
            repo.salvar_itens(data)
        sql = executor.call_args.args[1]
        self.assertIn("stage.arp_item.descricao_item", sql)
        self.assertIn("EXCLUDED.descricao_item", sql)
        self.assertIn("IS DISTINCT FROM", sql)
        connection.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
