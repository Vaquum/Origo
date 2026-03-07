#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  scripts/s7_docker_stack.sh up
  scripts/s7_docker_stack.sh down
  scripts/s7_docker_stack.sh logs
  scripts/s7_docker_stack.sh bootstrap
  scripts/s7_docker_stack.sh proof
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 1
fi

cd "${ROOT_DIR}"

case "$1" in
  up)
    docker compose up -d --build
    ;;
  down)
    docker compose down --remove-orphans
    ;;
  logs)
    docker compose logs --no-color
    ;;
  bootstrap)
    "${ROOT_DIR}/scripts/s7_docker_bootstrap.sh"
    ;;
  proof)
    "${ROOT_DIR}/scripts/s7_docker_local_proof.sh"
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
