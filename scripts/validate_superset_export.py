#!/usr/bin/env python3
"""Validate a Superset dashboard export before replacing a known-good ZIP."""
from __future__ import annotations

import sys
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile


def validate(path: str) -> dict[str, int]:
    try:
        with ZipFile(path) as bundle:
            bad_entry = bundle.testzip()
            if bad_entry is not None:
                raise ValueError(f"ZIP corrompido: {bad_entry}")

            paths = [i.filename for i in bundle.infolist() if not i.is_dir()]
            for name in paths:
                posix = PurePosixPath(name)
                if posix.is_absolute() or ".." in posix.parts:
                    raise ValueError(f"Caminho inseguro no ZIP: {name}")

            def count(folder: str) -> int:
                return sum(
                    1 for name in paths
                    if folder in PurePosixPath(name).parts
                    and name.lower().endswith((".yaml", ".yml"))
                )

            counts = {
                "dashboards": count("dashboards"),
                "charts": count("charts"),
                "datasets": count("datasets"),
                "databases": count("databases"),
            }
            if not counts["dashboards"]:
                raise ValueError(
                    "Export sem dashboards: verifique se existe algum painel salvo no Superset."
                )
            return counts
    except (OSError, BadZipFile) as exc:
        raise ValueError(f"Arquivo exportado invalido: {exc}") from exc


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Uso: python3 scripts/validate_superset_export.py caminho.zip", file=sys.stderr)
        return 2
    try:
        counts = validate(argv[1])
    except ValueError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1
    print("Export Superset valido: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    if not counts["charts"]:
        print("AVISO: dashboard sem charts exportados; revise o painel.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
