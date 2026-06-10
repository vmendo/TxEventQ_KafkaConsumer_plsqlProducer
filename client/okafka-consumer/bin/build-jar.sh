#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_JDK="/usr/lib/jvm/java-17-openjdk-17.0.19.0.10-1.0.1.el8.x86_64"

# Maven packaging needs javac. If JAVA_HOME points to a JRE, use the local JDK.
if [[ -z "${JAVA_HOME:-}" || ! -x "${JAVA_HOME}/bin/javac" ]]; then
  export JAVA_HOME="${DEFAULT_JDK}"
fi

if [[ ! -x "${JAVA_HOME}/bin/javac" ]]; then
  echo "ERROR: a JDK with javac is required. Set JAVA_HOME to a valid JDK." >&2
  exit 1
fi

export PATH="${JAVA_HOME}/bin:${PATH}"

cd "${PROJECT_DIR}"
mvn -q -DskipTests package

echo "Executable JAR created at ${PROJECT_DIR}/target/aie-okafka-consumer.jar"
