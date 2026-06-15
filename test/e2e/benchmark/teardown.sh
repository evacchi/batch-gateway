#!/usr/bin/env bash
set -uo pipefail

# Benchmark environment teardown.
# Removes all resources and deletes both namespaces.
#
# Required env vars:
#   KUBE_CONTEXT       — kubectl context
#   SYNC_NAMESPACE     — sync dispatch namespace
#   GATED_NAMESPACE    — gated async dispatch namespace
#
# Optional:
#   BATCH_REPO         — path to llm-d-batch-gateway (default: repo root)
#   GUIDE_NAME         — inference pool name (default: optimized-baseline)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_REPO="${BATCH_REPO:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
GUIDE_NAME="${GUIDE_NAME:-optimized-baseline}"

for var in KUBE_CONTEXT SYNC_NAMESPACE GATED_NAMESPACE; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set" >&2
        exit 1
    fi
done

K="kubectl --context=${KUBE_CONTEXT}"
H="helm --kube-context=${KUBE_CONTEXT}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

teardown_namespace() {
    local NS="$1"

    # Check if namespace exists
    if ! ${K} get ns "${NS}" >/dev/null 2>&1; then
        log "  ${NS}: namespace does not exist, skipping"
        return
    fi

    log "--- Tearing down ${NS} ---"

    # Delete jobs and configmaps from benchmark runs
    ${K} -n "${NS}" delete job --all --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete configmap \
        batch-submit-script batch-submit-2-script \
        --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete pod \
        results-helper-sync results-helper-gated \
        --ignore-not-found 2>/dev/null || true

    # Helm releases
    for release in batch-gateway async-processor "${GUIDE_NAME}" redis postgresql; do
        ${H} uninstall "${release}" -n "${NS}" 2>/dev/null || true
    done

    # Non-helm resources
    ${K} -n "${NS}" delete deploy vllm-qwen3-8b --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete svc vllm-qwen3-8b --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete sa -l llm-d.ai/guide="${GUIDE_NAME}" --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete -k "${BATCH_REPO}/test/e2e/benchmark/modelserver/" --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete gateway llm-d-inference-gateway --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete secret batch-gateway-secrets --ignore-not-found 2>/dev/null || true
    ${K} -n "${NS}" delete pvc --all --ignore-not-found 2>/dev/null || true

    log "  ${NS}: resources deleted"
}

log "=== Tearing down benchmark environment ==="

teardown_namespace "${SYNC_NAMESPACE}"
teardown_namespace "${GATED_NAMESPACE}"

log "Deleting namespaces..."
${K} delete ns "${SYNC_NAMESPACE}" "${GATED_NAMESPACE}" --ignore-not-found 2>/dev/null || true

log "=== Teardown complete ==="
