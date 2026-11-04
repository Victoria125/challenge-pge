#!/usr/bin/env bash
# Export dashboards from an ALREADY RUNNING Superset instance.
# Never restarts or recreates the container, preserving local metadata.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/superset/exports/arp-dashboard.zip"
CONTAINER="arp_superset"
REMOTE="/tmp/arp-dashboard-export-$$.zip"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERRO: docker nao encontrado." >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERRO: python3 nao encontrado (necessario para validar o ZIP)." >&2
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)" != "true" ]; then
  echo "ERRO: o container $CONTAINER nao esta rodando. Nao recrie o Superset antes de salvar seu dashboard." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"
TEMP="$(mktemp "$ROOT/superset/exports/.arp-dashboard-export.XXXXXX")"
cleanup() {
  rm -f "$TEMP"
  docker exec "$CONTAINER" rm -f "$REMOTE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Exportando os dashboards existentes em $CONTAINER..."
docker exec "$CONTAINER" superset export_dashboards --dashboard-file "$REMOTE"
docker cp "$CONTAINER:$REMOTE" "$TEMP"
python3 "$ROOT/scripts/validate_superset_export.py" "$TEMP"

if [ -f "$OUT" ]; then
  BACKUP="$OUT.backup.$(date -u +%Y%m%dT%H%M%SZ)"
  cp "$OUT" "$BACKUP"
  echo "Backup do export anterior: $BACKUP"
fi
mv "$TEMP" "$OUT"
echo "Export validado: $OUT"
echo "IMPORTANTE: inspecione os YAMLs do ZIP para dados privados, segredos e URLs locais ANTES de commitar em GitHub publico."
echo "Para reproduzir em outra instalacao, verifique as conexoes de banco e reinicie apenas o Superset."
