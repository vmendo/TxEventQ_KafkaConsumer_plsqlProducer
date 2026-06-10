#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WALLET_DIR="${DEMO_ROOT}/wallet"
TNS_ADMIN_DIR="${WALLET_DIR}/tns_admin"
PWD_FILE="${DEMO_ROOT}/.pwd.txt"
CONSUMER_DIR="${DEMO_ROOT}/client/okafka-consumer"
RUN_JAR="${CONSUMER_DIR}/bin/run-jar.sh"

usage() {
  cat <<'EOF'
Usage:
  ./run_consumer.sh [consumer options]

What this script does:
  - Checks that the ADB wallet exists under wallet/tns_admin.
  - If the wallet is missing, asks for a wallet ZIP or extracted wallet directory.
  - Reads the TNS alias, host, port, and service name from wallet/tns_admin/tnsnames.ora.
  - Checks for .pwd.txt.
  - If .pwd.txt is missing, asks for the AIE database password and saves it locally.
  - Starts the OKafka consumer with a fresh group.id by default.
  - Waits for records without an idle timeout by default; press Ctrl+C to stop it.

Examples:
  ./run_consumer.sh
  ./run_consumer.sh --max-messages=10
  ./run_consumer.sh --idle-timeout-seconds=120
  ./run_consumer.sh --group-id=CGAIE$(date +%s)

Environment overrides:
  AIE_DEMO_WALLET=/path/to/Wallet.zip
  AIE_DEMO_PASSWORD='...'
  OKAFKA_USER=AIE
  OKAFKA_TNS_ALIAS=<wallet-alias>
EOF
}

expand_path() {
  local value="$1"
  if [[ "${value}" == "~" ]]; then
    printf '%s\n' "${HOME}"
  elif [[ "${value}" == "~/"* ]]; then
    printf '%s\n' "${HOME}/${value#~/}"
  else
    printf '%s\n' "${value}"
  fi
}

wallet_ready() {
  [[ -f "${TNS_ADMIN_DIR}/tnsnames.ora" ]] &&
    [[ -f "${TNS_ADMIN_DIR}/ojdbc.properties" ]] &&
    { [[ -f "${TNS_ADMIN_DIR}/cwallet.sso" ]] || [[ -f "${TNS_ADMIN_DIR}/ewallet.p12" ]]; }
}

prompt_wallet_path() {
  local wallet_path="${AIE_DEMO_WALLET:-}"

  while [[ -z "${wallet_path}" ]]; do
    read -r -p "Path to ADB wallet ZIP or extracted wallet directory: " wallet_path
  done

  wallet_path="$(expand_path "${wallet_path}")"
  if [[ ! -e "${wallet_path}" ]]; then
    echo "ERROR: wallet path does not exist: ${wallet_path}" >&2
    exit 1
  fi

  printf '%s\n' "${wallet_path}"
}

prepare_wallet_if_needed() {
  if wallet_ready; then
    return
  fi

  echo "Wallet not found under wallet/tns_admin."
  local wallet_path
  wallet_path="$(prompt_wallet_path)"

  mkdir -p "${WALLET_DIR}" "${TNS_ADMIN_DIR}"

  if [[ -f "${wallet_path}" ]]; then
    case "${wallet_path}" in
      *.zip|*.ZIP)
        if ! command -v unzip >/dev/null 2>&1; then
          echo "ERROR: unzip is required to extract the wallet ZIP." >&2
          exit 1
        fi

        local wallet_zip="${WALLET_DIR}/$(basename "${wallet_path}")"
        cp "${wallet_path}" "${wallet_zip}"
        unzip -oq "${wallet_zip}" -d "${TNS_ADMIN_DIR}"
        ;;
      *)
        echo "ERROR: expected a wallet ZIP file or an extracted wallet directory." >&2
        exit 1
        ;;
    esac
  elif [[ -d "${wallet_path}" ]]; then
    cp -R "${wallet_path}/." "${TNS_ADMIN_DIR}/"
  fi

  chmod 700 "${WALLET_DIR}" "${TNS_ADMIN_DIR}" 2>/dev/null || true
  chmod 600 "${WALLET_DIR}"/* "${TNS_ADMIN_DIR}"/* 2>/dev/null || true

  if ! wallet_ready; then
    echo "ERROR: wallet preparation did not produce the required files in wallet/tns_admin." >&2
    exit 1
  fi

  echo "Wallet prepared under wallet/tns_admin."
}

list_tns_aliases() {
  awk '
    /^[[:space:]]*[A-Za-z0-9_.-]+[[:space:]]*=/ {
      alias=$1
      sub(/[[:space:]]*=.*/, "", alias)
      sub(/=.*/, "", alias)
      print alias
    }
  ' "${TNS_ADMIN_DIR}/tnsnames.ora"
}

select_tns_alias() {
  if [[ -n "${OKAFKA_TNS_ALIAS:-}" ]]; then
    printf '%s\n' "${OKAFKA_TNS_ALIAS}"
    return
  fi

  local aliases=()
  local alias
  while IFS= read -r alias; do
    [[ -n "${alias}" ]] && aliases+=("${alias}")
  done < <(list_tns_aliases)

  if [[ "${#aliases[@]}" -eq 0 ]]; then
    echo "ERROR: no TNS aliases found in wallet/tns_admin/tnsnames.ora." >&2
    exit 1
  fi

  for alias in "${aliases[@]}"; do
    if [[ "${alias,,}" == *_low ]]; then
      printf '%s\n' "${alias}"
      return
    fi
  done

  printf '%s\n' "${aliases[0]}"
}

get_tns_entry() {
  local selected_alias="$1"
  tr '\r' '\n' < "${TNS_ADMIN_DIR}/tnsnames.ora" | awk -v selected_alias="${selected_alias}" '
    BEGIN {
      selected=tolower(selected_alias);
    }
    {
      entry=$0;
      candidate=tolower(entry);
      if (candidate ~ "^[[:space:]]*" selected "[[:space:]]*=") {
        print entry;
        exit;
      }
    }
  '
}

extract_tns_value() {
  local entry="$1"
  local property="$2"
  printf '%s\n' "${entry}" | TNS_PROPERTY="${property}" perl -ne '
    my $property = $ENV{"TNS_PROPERTY"};
    if (/\(\s*\Q$property\E\s*=\s*([^)]+)\)/i) {
      print "$1\n";
      exit;
    }
  '
}

configure_okafka_connection() {
  local selected_alias
  local entry
  local host
  local port
  local service_name

  selected_alias="$(select_tns_alias)"
  entry="$(get_tns_entry "${selected_alias}")"

  if [[ -z "${entry}" ]]; then
    echo "ERROR: selected TNS alias was not found in tnsnames.ora: ${selected_alias}" >&2
    exit 1
  fi

  host="$(extract_tns_value "${entry}" "host")"
  port="$(extract_tns_value "${entry}" "port")"
  service_name="$(extract_tns_value "${entry}" "service_name")"

  if [[ -z "${host}" || -z "${service_name}" ]]; then
    echo "ERROR: could not extract host or service_name from TNS alias ${selected_alias}." >&2
    exit 1
  fi

  port="${port:-1522}"

  export OKAFKA_TNS_ADMIN="${TNS_ADMIN_DIR}"
  export OKAFKA_TNS_ALIAS="${selected_alias}"
  export OKAFKA_BOOTSTRAP_SERVERS="${host}:${port}"
  export OKAFKA_SERVICE_NAME="${service_name}"

  echo "Using wallet TNS alias: ${selected_alias}"
}

read_password_from_file() {
  local password=""
  if [[ -f "${PWD_FILE}" ]]; then
    IFS= read -r password < "${PWD_FILE}" || true
  fi
  printf '%s\n' "${password}"
}

save_password_file() {
  local password="$1"
  umask 077
  printf '%s\n' "${password}" > "${PWD_FILE}"
  chmod 600 "${PWD_FILE}" 2>/dev/null || true
}

prepare_password_if_needed() {
  local password
  local file_password
  file_password="$(read_password_from_file)"
  password="${file_password}"

  if [[ -z "${password}" ]]; then
    password="${AIE_DEMO_PASSWORD:-${OKAFKA_PASSWORD:-}}"
  fi

  if [[ -z "${password}" ]]; then
    read -r -s -p "AIE database password: " password
    printf '\n'
  fi

  if [[ -z "${password}" ]]; then
    echo "ERROR: password cannot be empty." >&2
    exit 1
  fi

  if [[ ! -f "${PWD_FILE}" || -z "${file_password}" ]]; then
    save_password_file "${password}"
    echo "Password saved locally in .pwd.txt with owner-only permissions."
  else
    chmod 600 "${PWD_FILE}" 2>/dev/null || true
  fi

  export OKAFKA_PASSWORD="${password}"
}

ensure_consumer_exists() {
  if [[ ! -x "${RUN_JAR}" ]]; then
    echo "ERROR: consumer runner not found or not executable: ${RUN_JAR}" >&2
    exit 1
  fi
}

build_consumer_args() {
  local has_group_id=0
  local arg

  for arg in "$@"; do
    if [[ "${arg}" == --group-id=* ]]; then
      has_group_id=1
      break
    fi
  done

  if [[ -n "${OKAFKA_GROUP_ID:-}" ]]; then
    has_group_id=1
  fi

  if [[ "${has_group_id}" -eq 0 ]]; then
    printf '%s\0' "--group-id=CGAIE$(date +%s)"
  fi

  for arg in "$@"; do
    printf '%s\0' "${arg}"
  done
}

main() {
  if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
  fi

  prepare_wallet_if_needed
  configure_okafka_connection
  prepare_password_if_needed
  ensure_consumer_exists

  local args=()
  while IFS= read -r -d '' item; do
    args+=("${item}")
  done < <(build_consumer_args "$@")

  echo "Starting OKafka consumer..."
  cd "${CONSUMER_DIR}"
  exec "${RUN_JAR}" "${args[@]}"
}

main "$@"
