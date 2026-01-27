#!/bin/sh
set -eu

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <local|prod> [docker compose args...]" >&2
  exit 1
fi

shift

if [ -z "${PRODUCTION_DOMAIN:-}" ] && [ -f ".env" ]; then
  PRODUCTION_DOMAIN="$(sed -n 's/^PRODUCTION_DOMAIN=//p' .env | tail -n 1)"
fi

if [ "$MODE" = "local" ]; then
  PREFECT_UI_API_URL="https://stockops.local/api"
  PROFILE_DEFAULT="--profile nginx-local"
elif [ "$MODE" = "prod" ]; then
  if [ -z "${PRODUCTION_DOMAIN:-}" ]; then
    echo "PRODUCTION_DOMAIN is not set in .env" >&2
    exit 1
  fi
  PREFECT_UI_API_URL="https://${PRODUCTION_DOMAIN}/api"
  PROFILE_DEFAULT="--profile nginx-prod"
else
  echo "Unknown mode: $MODE (expected local or prod)" >&2
  exit 1
fi

PREFECT_UI_SERVE_BASE="/prefect"
PYTHONPATH="./src"

TMP_ENV="$(mktemp)"
cleanup() { rm -f "$TMP_ENV"; }
trap cleanup EXIT

if [ -f ".env" ]; then
  cat ".env" > "$TMP_ENV"
fi

{
  echo "PREFECT_UI_SERVE_BASE=${PREFECT_UI_SERVE_BASE}"
  echo "PREFECT_UI_API_URL=${PREFECT_UI_API_URL}"
  echo "PYTHONPATH=${PYTHONPATH}"
} >> "$TMP_ENV"

export ENV_FILE="$TMP_ENV"
exec docker compose --env-file "$TMP_ENV" $PROFILE_DEFAULT "$@"
