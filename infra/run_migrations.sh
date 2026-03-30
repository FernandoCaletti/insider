#!/bin/bash
# Run all migrations against the database
# Usage: DATABASE_URL=postgresql://... ./run_migrations.sh

set -e

if [ -z "$DATABASE_URL" ]; then
  echo "ERROR: DATABASE_URL is required"
  echo "Usage: DATABASE_URL=postgresql://user:pass@host:5432/db ./run_migrations.sh"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/migrations"

echo "Running migrations against database..."

for migration in $(ls "$MIGRATIONS_DIR"/*.sql | sort); do
  echo "  Applying: $(basename $migration)"
  psql "$DATABASE_URL" -f "$migration" 2>&1 | grep -v "already exists" | grep -v "NOTICE" || true
done

echo "All migrations applied."
