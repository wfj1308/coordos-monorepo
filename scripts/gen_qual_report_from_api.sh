#!/usr/bin/env bash
set -euo pipefail

# One-shot report generation from API.
# It fetches qualification report JSON and then calls gen_qual_report.js.
#
# Usage:
#   bash scripts/gen_qual_report_from_api.sh [year] [output.docx]
#
# Optional env:
#   BASE_URL=http://127.0.0.1:8090
#   TMP_JSON=/tmp/qual_report.json
#   KEEP_JSON=1

BASE_URL="${BASE_URL:-http://127.0.0.1:8090}"
YEAR="${1:-$(date +%Y)}"
OUT_FILE="${2:-qualification_report_${YEAR}.docx}"
TMP_JSON="${TMP_JSON:-$(mktemp)}"
KEEP_JSON="${KEEP_JSON:-0}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GENERATOR="${ROOT_DIR}/scripts/gen_qual_report.js"
GO_GENERATOR="./services/design-institute/cmd/qual-report"

URL="${BASE_URL}/api/v1/reports/qualification?year=${YEAR}"
echo "[INFO] Fetching report JSON: ${URL}"
curl -sS --fail "${URL}" -o "${TMP_JSON}"

echo "[INFO] Generating DOCX: ${OUT_FILE}"
GENERATED=0

# Prefer Node.js generator when dependency is available.
if [[ -f "${GENERATOR}" ]] && command -v node >/dev/null 2>&1; then
  if node -e "require('docx')" >/dev/null 2>&1; then
    if node "${GENERATOR}" "${TMP_JSON}" "${OUT_FILE}"; then
      GENERATED=1
    else
      echo "[WARN] gen_qual_report.js failed, fallback to Go generator..."
    fi
  else
    echo "[WARN] docx dependency missing in Node.js environment, fallback to Go generator..."
  fi
else
  echo "[WARN] JS generator unavailable, fallback to Go generator..."
fi

# Fallback path: stdlib-only Go generator for offline environments.
if [[ "${GENERATED}" != "1" ]]; then
  mkdir -p "${ROOT_DIR}/.gocache" "${ROOT_DIR}/.gomodcache" "${ROOT_DIR}/.gotelemetry" "${ROOT_DIR}/.appdata"
  (
    cd "${ROOT_DIR}"
    GOTELEMETRY=local GOTELEMETRYDIR="${ROOT_DIR}/.gotelemetry" \
      APPDATA="${ROOT_DIR}/.appdata" \
      GOCACHE="${ROOT_DIR}/.gocache" GOMODCACHE="${ROOT_DIR}/.gomodcache" \
      go run "${GO_GENERATOR}" --in "${TMP_JSON}" --out "${OUT_FILE}"
  )
  GENERATED=1
fi

if [[ "${KEEP_JSON}" != "1" ]]; then
  rm -f "${TMP_JSON}"
else
  echo "[INFO] Keeping JSON: ${TMP_JSON}"
fi

echo "[PASS] Generated report: ${OUT_FILE}"
