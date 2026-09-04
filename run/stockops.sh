#!/bin/sh
set -eu

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <local|prod> [--providers <list>] [docker compose args...]" >&2
  echo "  --providers: comma-separated, any of: eodhd, ibkr_paper, ibkr_live" >&2
  echo "  no spaces after commas (or quote the whole list), e.g. --providers ibkr_paper,eodhd" >&2
  exit 1
fi

shift

# These profiles are managed exclusively by this script — never accept them as raw --profile input,
# since doing so bypasses the guarantees the script exists to enforce (exactly one nginx profile,
# matching MODE; an explicit IBKR trading mode; datapipe-core only ever turned on by a real provider).
# Calling `docker compose` directly, bypassing this script entirely, is unaffected by this.
reject_profile() {
  case "$1" in
    ibkr)
      echo "Don't pass '--profile ibkr' directly — use '--providers ibkr_paper' or 'ibkr_live'" >&2
      echo "instead, so the trading mode is always explicit." >&2
      ;;
    datapipe-core)
      echo "Don't pass '--profile datapipe-core' directly — use '--providers eodhd', 'ibkr_paper'," >&2
      echo "or 'ibkr_live' instead; datapipe-core is enabled automatically by any of them. If you" >&2
      echo "just want to read already-stored data with no provider running, use '--profile reader'" >&2
      echo "(add '--profile datapipe-visualize-data' too if you also want the DB browser)." >&2
      ;;
    nginx-local|nginx-prod)
      echo "Don't pass '--profile $1' directly — it's chosen automatically from the '$MODE' mode" >&2
      echo "argument. Run with 'local' or 'prod' as the first argument instead." >&2
      ;;
  esac
  exit 1
}

# Pull --providers out of the args (docker compose doesn't know this flag) and translate each token
# into the --profile flags and env vars it implies. Providers are the sole source of IBKR_TRADING_MODE
# now — it is intentionally not in .env, mirroring INSTALL_IBKR.
WANT_EODHD=0
WANT_IBKR=0
IBKR_TRADING_MODE=paper   # hard default, reset unconditionally so no inherited shell env var can leak in
REMAINING=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --providers)
      PROVIDERS_LIST="$2"
      shift 2
      # An unquoted space after a comma (e.g. `--providers ibkr_paper, eodhd`) makes the shell split
      # this into separate arguments before we ever see them. Recover by absorbing the next argument(s)
      # for as long as what we've accumulated ends in a trailing comma.
      while [ "${PROVIDERS_LIST%,}" != "$PROVIDERS_LIST" ] && [ "$#" -gt 0 ]; do
        PROVIDERS_LIST="${PROVIDERS_LIST}$1"
        shift
      done
      continue
      ;;
    --providers=*)
      PROVIDERS_LIST="${1#--providers=}"
      shift
      while [ "${PROVIDERS_LIST%,}" != "$PROVIDERS_LIST" ] && [ "$#" -gt 0 ]; do
        PROVIDERS_LIST="${PROVIDERS_LIST}$1"
        shift
      done
      continue
      ;;
    --profile)
      case "$2" in
        ibkr|datapipe-core|nginx-local|nginx-prod) reject_profile "$2" ;;
      esac
      ;;
    --profile=*)
      PROFILE_VAL="${1#--profile=}"
      case "$PROFILE_VAL" in
        ibkr|datapipe-core|nginx-local|nginx-prod) reject_profile "$PROFILE_VAL" ;;
      esac
      ;;
  esac
  REMAINING="$REMAINING $1"
  shift
done
set -- $REMAINING

if [ -n "${PROVIDERS_LIST:-}" ] && [ "${PROVIDERS_LIST%,}" != "$PROVIDERS_LIST" ]; then
  echo "--providers has a trailing comma with nothing after it: '$PROVIDERS_LIST'" >&2
  echo "Remove the trailing comma, or add another provider (eodhd, ibkr_paper, ibkr_live)." >&2
  exit 1
fi

# A stray provider name that didn't get absorbed above (e.g. the comma was missing entirely, not
# just followed by a space) means the list wasn't well-formed — catch it here with a clear message
# rather than letting it reach Docker as a nonsense subcommand.
for ARG in "$@"; do
  case "$ARG" in
    eodhd|ibkr_paper|ibkr_live)
      echo "'$ARG' looks like it was meant to be part of --providers but landed as a separate" >&2
      echo "argument instead — check for a missing comma (--providers ibkr_paper,eodhd)." >&2
      exit 1
      ;;
  esac
done

if [ -n "${PROVIDERS_LIST:-}" ]; then
  OLD_IFS="$IFS"
  IFS=,
  for TOKEN in $PROVIDERS_LIST; do
    IFS="$OLD_IFS"
    TOKEN="$(echo "$TOKEN" | tr -d '[:space:]')"
    case "$TOKEN" in
      eodhd) WANT_EODHD=1 ;;
      ibkr_paper)
        [ "$WANT_IBKR" = "1" ] && [ "$IBKR_TRADING_MODE" = "live" ] && { echo "--providers cannot include both ibkr_paper and ibkr_live." >&2; exit 1; }
        WANT_IBKR=1; IBKR_TRADING_MODE=paper
        ;;
      ibkr_live)
        [ "$WANT_IBKR" = "1" ] && [ "$IBKR_TRADING_MODE" = "paper" ] && { echo "--providers cannot include both ibkr_paper and ibkr_live." >&2; exit 1; }
        WANT_IBKR=1; IBKR_TRADING_MODE=live
        ;;
      *)
        echo "Unknown --providers value: '$TOKEN' (expected eodhd, ibkr_paper, or ibkr_live)" >&2
        exit 1
        ;;
    esac
    IFS=,
  done
  IFS="$OLD_IFS"
else
  echo "NOTE: no --providers given — datapipe-core will not start, so no provider credentials will" >&2
  echo "be loaded or checked and nothing will actually fetch data. Pass --providers eodhd, ibkr_paper," >&2
  echo "or ibkr_live to enable one. --profile reader (and --profile datapipe-visualize-data for the" >&2
  echo "DB browser) still work standalone if you just want to read already-stored data." >&2
fi

# Any provider needs the core pipeline running; ibkr additionally needs its own profile + trading mode.
# Prepended, not appended: "$@" already ends with the actual docker compose subcommand (up/ps/logs/...),
# and --profile is a global flag that Docker requires before the subcommand, not after.
if [ "$WANT_EODHD" = "1" ] || [ "$WANT_IBKR" = "1" ]; then
  set -- --profile datapipe-core "$@"
fi
if [ "$WANT_IBKR" = "1" ]; then
  set -- --profile ibkr "$@"
fi

# ibkr always arrives bundled with datapipe-core now — a raw --profile ibkr is rejected above, and
# --providers only ever adds --profile ibkr alongside --profile datapipe-core (never alone) — so
# HAS_IBKR_PROFILE is simply WANT_IBKR; no need to re-scan "$@" for it.
HAS_IBKR_PROFILE="$WANT_IBKR"

# Force prefect-serve to be built with ib_async, and pin the trading mode, whenever ibkr is requested
# by any means (--providers or a raw --profile ibkr) — always paper unless --providers ibkr_live was
# explicitly given (IBKR_TRADING_MODE was hard-reset above, so a raw --profile ibkr always gets paper).
if [ "$HAS_IBKR_PROFILE" = "1" ]; then
  export INSTALL_IBKR=true
  export IBKR_TRADING_MODE
fi

# Fails and exits if $1's value in .env is missing or still equal to placeholder $2 (label $3 in the message).
require_real_value() {
  VAR_NAME="$1"; PLACEHOLDER="$2"; LABEL="$3"
  VAL="$(sed -n "s/^${VAR_NAME}=//p" .env | tail -n 1)"
  if [ -z "$VAL" ] || [ "$VAL" = "$PLACEHOLDER" ]; then
    echo "${VAR_NAME} in .env is missing or still the placeholder value from .env.example." >&2
    echo "Set a real ${LABEL} before running this command." >&2
    exit 1
  fi
}

# True (0) if $1's value in .env is set and not equal to placeholder $2 — never fails, just reports.
is_real_value() {
  VAR_NAME="$1"; PLACEHOLDER="$2"
  VAL="$(sed -n "s/^${VAR_NAME}=//p" .env 2>/dev/null | tail -n 1)"
  [ -n "$VAL" ] && [ "$VAL" != "$PLACEHOLDER" ]
}

# Only checked (hard fail-fast) when the provider was explicitly requested via --providers — datapipe-core
# running by itself no longer implies EODHD is wanted, since EODHD has no profile of its own to gate on.
if [ "$WANT_EODHD" = "1" ] && [ -f ".env" ]; then
  require_real_value EODHD_API_TOKEN "__PUT_YOUR_TOKEN_HERE__" "EODHD API token"
fi

# Fail fast on missing/placeholder IBKR credentials for the active mode, before any container starts.
# Also exports the bare TWS_USERID/TWS_PASSWORD the container actually reads: the gnzsnz image reads
# the bare names unconditionally via envsubst — it never selects _PAPER vs bare itself — so *we* have
# to be the ones selecting which .env pair feeds them, based on IBKR_TRADING_MODE. Confirmed directly:
# without this, paper mode logs in with a blank username/password and hangs silently.
if [ "$HAS_IBKR_PROFILE" = "1" ] && [ -f ".env" ]; then
  if [ "$IBKR_TRADING_MODE" = "live" ]; then
    require_real_value TWS_USERID "__PUT_YOUR_LIVE_IBKR_USERNAME_HERE__" "IBKR live username"
    require_real_value TWS_PASSWORD "__PUT_YOUR_LIVE_IBKR_PASSWORD_HERE__" "IBKR live password"
    export TWS_USERID="$(sed -n 's/^TWS_USERID=//p' .env | tail -n 1)"
    export TWS_PASSWORD="$(sed -n 's/^TWS_PASSWORD=//p' .env | tail -n 1)"
  else
    require_real_value TWS_USERID_PAPER "__PUT_YOUR_PAPER_IBKR_USERNAME_HERE__" "IBKR paper username"
    require_real_value TWS_PASSWORD_PAPER "__PUT_YOUR_PAPER_IBKR_PASSWORD_HERE__" "IBKR paper password"
    export TWS_USERID="$(sed -n 's/^TWS_USERID_PAPER=//p' .env | tail -n 1)"
    export TWS_PASSWORD="$(sed -n 's/^TWS_PASSWORD_PAPER=//p' .env | tail -n 1)"
  fi
fi

# Availability signals for a future front end (Chunk 7) to read — computed unconditionally from what's
# actually in .env, independent of what --providers requested this run. Not a security control.
EODHD_AVAILABLE=false
IBKR_PAPER_AVAILABLE=false
IBKR_LIVE_AVAILABLE=false
if [ -f ".env" ]; then
  is_real_value EODHD_API_TOKEN "__PUT_YOUR_TOKEN_HERE__" && EODHD_AVAILABLE=true
  is_real_value TWS_USERID_PAPER "__PUT_YOUR_PAPER_IBKR_USERNAME_HERE__" && is_real_value TWS_PASSWORD_PAPER "__PUT_YOUR_PAPER_IBKR_PASSWORD_HERE__" && IBKR_PAPER_AVAILABLE=true
  is_real_value TWS_USERID "__PUT_YOUR_LIVE_IBKR_USERNAME_HERE__" && is_real_value TWS_PASSWORD "__PUT_YOUR_LIVE_IBKR_PASSWORD_HERE__" && IBKR_LIVE_AVAILABLE=true
fi

# Whether each provider is actually active *in this invocation* (not just credentialed in .env) —
# what the Streamlit UI (Chunk 7) gates section visibility on, since showing a provider's controls
# when its backend isn't actually running/built invites a confusing failure at trigger time instead
# of a clear "not available" state at the point of choice.
EODHD_UI_AVAILABLE=false
IBKR_UI_AVAILABLE=false
[ "$WANT_EODHD" = "1" ] && EODHD_UI_AVAILABLE=true
[ "$HAS_IBKR_PROFILE" = "1" ] && IBKR_UI_AVAILABLE=true

rm -f .env.runtime.*

PROD_HTPASSWD_PATH="./secrets/prod.htpasswd"

if [ -z "${PRODUCTION_DOMAIN:-}" ] && [ -f ".env" ]; then
  PRODUCTION_DOMAIN="$(sed -n 's/^PRODUCTION_DOMAIN=//p' .env | tail -n 1)"
fi

PROXY_MODE="${PROXY_MODE:-docker}"

if [ "$MODE" = "local" ]; then
  PREFECT_UI_API_URL="https://stockops.local/api"
  PROFILE_DEFAULT="--profile nginx-local"
elif [ "$MODE" = "prod" ]; then
  if [ -z "${PRODUCTION_DOMAIN:-}" ]; then
    echo "PRODUCTION_DOMAIN is not set in .env" >&2
    exit 1
  fi
  if [ -d "$PROD_HTPASSWD_PATH" ]; then
    cat >&2 <<EOF
Invalid production htpasswd path: $PROD_HTPASSWD_PATH is a directory, but it must be a file.

Fix it with:
  rm -rf $PROD_HTPASSWD_PATH
  mkdir -p ./secrets
  htpasswd -Bc $PROD_HTPASSWD_PATH produser
EOF
    exit 1
  fi
  if [ ! -f "$PROD_HTPASSWD_PATH" ] || [ ! -s "$PROD_HTPASSWD_PATH" ]; then
    cat >&2 <<EOF
Missing production htpasswd file: $PROD_HTPASSWD_PATH

Create it before starting prod mode:
  mkdir -p ./secrets
  htpasswd -Bc $PROD_HTPASSWD_PATH produser

If 'htpasswd' is unavailable, use Docker:
  docker run --rm -it -v "\$(pwd)/secrets:/work" httpd:2.4-alpine \
    htpasswd -Bc /work/prod.htpasswd produser
EOF
    exit 1
  fi
  PREFECT_UI_API_URL="https://${PRODUCTION_DOMAIN}/api"
  if [ "$PROXY_MODE" = "host" ]; then
    PROFILE_DEFAULT=""
  else
    PROFILE_DEFAULT="--profile nginx-prod"
  fi
else
  echo "Unknown mode: $MODE (expected local or prod)" >&2
  exit 1
fi

PREFECT_UI_SERVE_BASE="/prefect"
PYTHONPATH="./src"

TMP_ENV="$(mktemp -p . .env.runtime.XXXXXX)"
cleanup() { rm -f "$TMP_ENV"; }
trap cleanup EXIT

if [ -f ".env" ]; then
  cat ".env" > "$TMP_ENV"
fi

{
  echo "PREFECT_UI_SERVE_BASE=${PREFECT_UI_SERVE_BASE}"
  echo "PREFECT_UI_API_URL=${PREFECT_UI_API_URL}"
  echo "PYTHONPATH=${PYTHONPATH}"
  echo "EODHD_AVAILABLE=${EODHD_AVAILABLE}"
  echo "IBKR_PAPER_AVAILABLE=${IBKR_PAPER_AVAILABLE}"
  echo "IBKR_LIVE_AVAILABLE=${IBKR_LIVE_AVAILABLE}"
  echo "EODHD_UI_AVAILABLE=${EODHD_UI_AVAILABLE}"
  echo "IBKR_UI_AVAILABLE=${IBKR_UI_AVAILABLE}"
} >> "$TMP_ENV"

if [ "$HAS_IBKR_PROFILE" = "1" ]; then
  # `docker compose up` won't rebuild an existing image just because a build arg changed.
  echo "Ensuring prefect-serve has the IBKR client installed (--profile ibkr detected)..." >&2
  docker compose --env-file "$TMP_ENV" build prefect-serve
fi

export ENV_FILE="$TMP_ENV"
exec docker compose --env-file "$TMP_ENV" $PROFILE_DEFAULT "$@"
