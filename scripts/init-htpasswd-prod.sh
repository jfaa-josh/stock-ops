#!/bin/sh
set -eu

HTPASSWD_PATH="${1:-}"
if [ -z "$HTPASSWD_PATH" ]; then
  echo "Usage: $0 /path/to/htpasswd" >&2
  exit 1
fi

if [ -s "$HTPASSWD_PATH" ]; then
  echo "htpasswd already exists at $HTPASSWD_PATH"
  exit 0
fi

if ! command -v htpasswd >/dev/null 2>&1; then
  echo "htpasswd not found in this container image." >&2
  exit 1
fi

if [ ! -t 0 ]; then
  echo "No interactive TTY available to prompt for a password." >&2
  echo "Run docker compose without -d for the first nginx-prod start." >&2
  exit 1
fi

mkdir -p "$(dirname "$HTPASSWD_PATH")"
echo "Creating production htpasswd for user 'produser'..."
htpasswd -B -c "$HTPASSWD_PATH" produser
