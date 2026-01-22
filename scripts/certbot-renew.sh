#!/bin/sh
set -eu

DOMAIN="${PRODUCTION_DOMAIN:-}"
EMAIL="${LETSENCRYPT_EMAIL:-}"
WEBROOT="/var/www/certbot"
CERT_PATH="/etc/letsencrypt/live/${DOMAIN}/fullchain.pem"

if [ -z "$DOMAIN" ]; then
  echo "PRODUCTION_DOMAIN is not set; certbot cannot run." >&2
  exit 1
fi

if [ ! -f "$CERT_PATH" ]; then
  echo "No existing cert for ${DOMAIN}. Initial issuance is handled by the init service." >&2
  echo "Ensure PRODUCTION_DOMAIN and LETSENCRYPT_EMAIL are set, then restart the stack." >&2
  exit 1
fi

while true; do
  certbot renew --webroot -w "$WEBROOT" --quiet
  sleep 12h
done
