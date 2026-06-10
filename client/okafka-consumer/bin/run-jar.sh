#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
JAR_FILE="${PROJECT_DIR}/target/aie-okafka-consumer.jar"

if [[ ! -f "${JAR_FILE}" ]]; then
  "${PROJECT_DIR}/bin/build-jar.sh"
fi

if [[ "${1:-}" != "--help" && "${1:-}" != "-h" ]]; then
  source "${SCRIPT_DIR}/prepare-okafka-env.sh"
  prepare_okafka_runtime_env
  trap cleanup_okafka_runtime_env EXIT
fi

if [[ -n "${OKAFKA_JAVA_TNS_ADMIN_OPT:-}" ]]; then
  java "${OKAFKA_JAVA_TNS_ADMIN_OPT}" -jar "${JAR_FILE}" "$@"
else
  java -jar "${JAR_FILE}" "$@"
fi
