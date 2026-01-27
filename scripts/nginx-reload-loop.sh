#!/bin/sh
set -eu

INTERVAL="${NGINX_RELOAD_INTERVAL_SECONDS:-43200}"

while true; do
  sleep "$INTERVAL"
  nginx -s reload >/dev/null 2>&1 || true
done
