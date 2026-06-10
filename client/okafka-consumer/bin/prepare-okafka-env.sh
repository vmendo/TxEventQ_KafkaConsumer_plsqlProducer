#!/usr/bin/env bash

# This file is sourced by the demo run scripts.
# It creates a temporary TNS_ADMIN directory that includes wallet files plus
# an ojdbc.properties file containing the runtime database credentials.

prepare_okafka_runtime_env() {
  local script_dir
  local project_dir
  local demo_root
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  project_dir="$(cd "${script_dir}/.." && pwd)"
  demo_root="$(cd "${project_dir}/../.." && pwd)"

  local original_tns_admin="${OKAFKA_TNS_ADMIN:-${demo_root}/wallet/tns_admin}"
  local db_user="${OKAFKA_USER:-AIE}"

  if [[ -z "${OKAFKA_PASSWORD:-}" ]]; then
    echo "ERROR: OKAFKA_PASSWORD must be set before running the consumer." >&2
    return 1
  fi

  if [[ ! -d "${original_tns_admin}" ]]; then
    echo "ERROR: TNS_ADMIN directory not found: ${original_tns_admin}" >&2
    return 1
  fi

  OKAFKA_RUNTIME_TNS_ADMIN="$(mktemp -d /tmp/aie-okafka-tns-XXXXXX)"
  export OKAFKA_RUNTIME_TNS_ADMIN
  chmod 700 "${OKAFKA_RUNTIME_TNS_ADMIN}"

  cp "${original_tns_admin}"/* "${OKAFKA_RUNTIME_TNS_ADMIN}/"
  chmod 600 "${OKAFKA_RUNTIME_TNS_ADMIN}"/* 2>/dev/null || true

  {
    printf '%s\n' "# Generated at runtime by the AIE OKafka demo. Do not keep this file."
    printf '%s\n' "oracle.net.wallet_location=(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=${OKAFKA_RUNTIME_TNS_ADMIN})))"
    printf 'user=%s\n' "${db_user}"
    printf 'password=%s\n' "${OKAFKA_PASSWORD}"
  } > "${OKAFKA_RUNTIME_TNS_ADMIN}/ojdbc.properties"
  chmod 600 "${OKAFKA_RUNTIME_TNS_ADMIN}/ojdbc.properties"

  export TNS_ADMIN="${OKAFKA_RUNTIME_TNS_ADMIN}"
  export OKAFKA_TNS_ADMIN="${OKAFKA_RUNTIME_TNS_ADMIN}"
  export OKAFKA_USE_EXISTING_TNS_ADMIN=true
  export OKAFKA_JAVA_TNS_ADMIN_OPT="-Doracle.net.tns_admin=${OKAFKA_RUNTIME_TNS_ADMIN}"
}

cleanup_okafka_runtime_env() {
  if [[ -n "${OKAFKA_RUNTIME_TNS_ADMIN:-}" && -d "${OKAFKA_RUNTIME_TNS_ADMIN}" ]]; then
    rm -rf "${OKAFKA_RUNTIME_TNS_ADMIN}"
  fi
}
