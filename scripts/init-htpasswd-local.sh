#!/bin/sh
set -eu

HTPASSWD_DIR="${1:-}"
if [ -z "$HTPASSWD_DIR" ]; then
  echo "Usage: $0 /path/to/htpasswd-dir" >&2
  exit 1
fi

TARGET="${HTPASSWD_DIR}/htpasswd"
SOURCE="/defaults/htpasswd-local"

if [ -s "$TARGET" ]; then
  echo "htpasswd already exists at $TARGET"
  exit 0
fi

if [ ! -f "$SOURCE" ]; then
  echo "Default local htpasswd not found at $SOURCE" >&2
  exit 1
fi

mkdir -p "$HTPASSWD_DIR"
cp "$SOURCE" "$TARGET"
echo "Seeded local htpasswd at $TARGET"
