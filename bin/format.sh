
PX_LOGO=$(cat <<'PX_LOGO_TEXT'
 ___     _              _
| _ \___| |_ __ _ _ __ (_)__
|  _/ _ \  _/ _` | '  \| / _|
|_| \___/\__\__,_|_|_|_|_\__|
PX_LOGO_TEXT
)

PX_BOLD='\033[1m'
PX_ITAL='\033[3m'
PX_BLUE='\033[0;34m'
PX_RED='\033[0;31m'
PX_GREEN='\033[0;32m'
PX_ORANGE='\033[0;33m'
PX_NC='\033[0m'

print() {
  echo -e "$@"
}

tkv() {
  echo -e "${PX_BOLD}${1}: ${PX_NC} ${2}"
}

