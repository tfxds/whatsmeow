#!/usr/bin/env bash
# Backup do device store (sessões whatsmeow). Restaure no destino p/ migrar sem reparear.
set -euo pipefail
OUT="${1:-/root/backups/whatsmeow-$(date +%Y%m%d-%H%M%S).sql.gz}"
mkdir -p "$(dirname "$OUT")"
docker exec postgres-whatsmeow pg_dump -U postgres -d whatsmeow | gzip > "$OUT"
echo "dump: $OUT"
# Restore: zcat ARQ.sql.gz | docker exec -i postgres-whatsmeow psql -U postgres -d whatsmeow
