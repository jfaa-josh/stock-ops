#!/bin/sh
set -eu

DOMAIN="${PRODUCTION_DOMAIN:-}"
EMAIL="${LETSENCRYPT_EMAIL:-}"
CERT_PATH="/etc/letsencrypt/live/${DOMAIN}/fullchain.pem"

if [ -z "$DOMAIN" ]; then
  echo "PRODUCTION_DOMAIN is not set; certbot cannot run." >&2
  exit 1
fi

if [ -f "$CERT_PATH" ]; then
  echo "Certificate already exists for ${DOMAIN}; skipping init."
  exit 0
fi

if [ -z "$EMAIL" ]; then
  echo "LETSENCRYPT_EMAIL is not set; cannot request initial certificate." >&2
  exit 1
fi

echo "Requesting initial certificate for ${DOMAIN} (standalone)..."
certbot certonly --standalone \
  -d "$DOMAIN" \
  --email "$EMAIL" \
  --agree-tos \
  --no-eff-email \
  --non-interactive
