#!/bin/sh
set -eu

TEMPLATE_DIR="${NGINX_ENVSUBST_TEMPLATE_DIR:-}"
TEMPLATE_PATH="${TEMPLATE_DIR}/stockops.conf.template"
OUTPUT_PATH="/etc/nginx/conf.d/default.conf"

if [ -z "$TEMPLATE_DIR" ]; then
  echo "NGINX_ENVSUBST_TEMPLATE_DIR is not set." >&2
  exit 1
fi

if [ ! -f "$TEMPLATE_PATH" ]; then
  echo "Template not found: $TEMPLATE_PATH" >&2
  exit 1
fi

envsubst < "$TEMPLATE_PATH" > "$OUTPUT_PATH"
exec nginx -g 'daemon off;'
