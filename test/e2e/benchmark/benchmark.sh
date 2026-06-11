#!/usr/bin/env bash
# Benchmark: Batch vs Live Traffic Isolation
#
# Runs three guidellm sweep scenarios to measure whether batch requests
# degrade live traffic quality when the dispatcher gate is active.
#
# Scenarios:
#   1. baseline — guidellm sweep only, no batch load
#   2. gated   — guidellm sweep + batch load, dispatcher gate enabled
#   3. ungated — guidellm sweep + batch load, dispatcher gate disabled (constant)
#
# Prerequisites:
#   - Kubernetes cluster with the llm-d stack deployed (see docs/guides/benchmark.md)
#   - async-processor deployed with prometheus-budget gate
#   - batch-gateway configured for async dispatch
#
# Usage:
#   ./benchmark.sh [--namespace <ns>] [--results-dir <dir>] [--batch-size <n>]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configuration ──────────────────────────────────────────────────────

NAMESPACE="${NAMESPACE:-llm-d-async}"
RESULTS_DIR="${RESULTS_DIR:-./benchmark-results}"
BATCH_SIZE="${BATCH_SIZE:-100}"
GUIDELLM_MAX_SECONDS="${GUIDELLM_MAX_SECONDS:-120}"
ASYNC_RELEASE="${ASYNC_RELEASE:-async-processor}"
ASYNC_CHART="${ASYNC_CHART:-}"
ASYNC_VALUES="${ASYNC_VALUES:-}"

# Parse CLI flags
while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace)     NAMESPACE="$2"; shift 2 ;;
        --results-dir)   RESULTS_DIR="$2"; shift 2 ;;
        --batch-size)    BATCH_SIZE="$2"; shift 2 ;;
        --max-seconds)   GUIDELLM_MAX_SECONDS="$2"; shift 2 ;;
        --async-release) ASYNC_RELEASE="$2"; shift 2 ;;
        --async-chart)   ASYNC_CHART="$2"; shift 2 ;;
        --async-values)  ASYNC_VALUES="$2"; shift 2 ;;
        *)               echo "Unknown flag: $1"; exit 1 ;;
    esac
done

mkdir -p "${RESULTS_DIR}"

# ── Helpers ────────────────────────────────────────────────────────────

log() { echo "==> $(date +%H:%M:%S) $*"; }

wait_for_job() {
    local job_name="$1"
    local timeout="${2:-600}"
    log "Waiting for job ${job_name} (timeout ${timeout}s)..."
    if ! kubectl wait --for=condition=complete "job/${job_name}" \
        -n "${NAMESPACE}" --timeout="${timeout}s" 2>/dev/null; then
        # Check if it failed rather than timed out
        local status
        status=$(kubectl get job "${job_name}" -n "${NAMESPACE}" \
            -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)
        if [ "${status}" = "True" ]; then
            log "Job ${job_name} failed. Logs:"
            kubectl logs "job/${job_name}" -n "${NAMESPACE}" --tail=50 || true
            return 1
        fi
        log "Job ${job_name} timed out after ${timeout}s"
        return 1
    fi
    log "Job ${job_name} completed"
}

cleanup_job() {
    local job_name="$1"
    kubectl delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
}

apply_with_env() {
    local file="$1"
    shift
    # Apply YAML with environment variable substitution
    envsubst < "${file}" | kubectl apply -n "${NAMESPACE}" "$@" -f -
}

collect_results() {
    local scenario="$1"
    local dest="${RESULTS_DIR}/${scenario}"
    mkdir -p "${dest}"

    # Find the guidellm pod and copy results
    local pod
    pod=$(kubectl get pods -n "${NAMESPACE}" -l job-name=guidellm-sweep \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "${pod}" ]; then
        kubectl cp "${NAMESPACE}/${pod}:/results/${scenario}" "${dest}" 2>/dev/null || true
        log "Results collected to ${dest}"
    else
        log "Warning: could not find guidellm pod for result collection"
    fi
}

# ── Scenario runner ───────────────────────────────────────────────────

run_guidellm_sweep() {
    local scenario="$1"
    log "Running guidellm sweep: ${scenario}"

    cleanup_job "guidellm-sweep"

    export GUIDELLM_SCENARIO="${scenario}"
    export GUIDELLM_MAX_SECONDS
    apply_with_env "${SCRIPT_DIR}/guidellm-sweep.yaml"

    # guidellm sweep + buffer time
    local timeout=$(( GUIDELLM_MAX_SECONDS + 120 ))
    wait_for_job "guidellm-sweep" "${timeout}"
    collect_results "${scenario}"
    cleanup_job "guidellm-sweep"
}

run_batch_submit() {
    log "Submitting batch (${BATCH_SIZE} requests)"

    cleanup_job "batch-submit"

    export BATCH_SIZE
    apply_with_env "${SCRIPT_DIR}/batch-submit.yaml"
}

# ── Ensure PVC exists ─────────────────────────────────────────────────

ensure_pvc() {
    if ! kubectl get pvc benchmark-results -n "${NAMESPACE}" &>/dev/null; then
        log "Creating results PVC"
        kubectl apply -n "${NAMESPACE}" -f "${SCRIPT_DIR}/results-pvc.yaml"
    fi
}

# ── Scenario 1: Baseline ─────────────────────────────────────────────

run_baseline() {
    log "━━━ Scenario 1: BASELINE (no batch load) ━━━"
    run_guidellm_sweep "baseline"
}

# ── Scenario 2: Gated (batch + dispatcher gate enabled) ──────────────

run_gated() {
    log "━━━ Scenario 2: GATED (batch load + gate enabled) ━━━"

    # Start batch submission (runs concurrently with guidellm)
    run_batch_submit
    sleep 10  # let batch requests start flowing

    run_guidellm_sweep "gated"

    # Wait for batch to finish (or clean up)
    wait_for_job "batch-submit" 600 || true
    cleanup_job "batch-submit"
}

# ── Scenario 3: Ungated (batch + gate disabled) ──────────────────────

run_ungated() {
    log "━━━ Scenario 3: UNGATED (batch load + gate disabled) ━━━"

    if [ -z "${ASYNC_CHART}" ] || [ -z "${ASYNC_VALUES}" ]; then
        log "Skipping ungated scenario: --async-chart and --async-values required"
        log "These are needed to reconfigure the dispatcher gate to 'constant' (always open)"
        return 0
    fi

    # Reconfigure dispatcher gate to constant (always open)
    log "Reconfiguring dispatcher gate to 'constant' (always open)"
    helm upgrade "${ASYNC_RELEASE}" "${ASYNC_CHART}" \
        -f "${ASYNC_VALUES}" \
        --set 'ap.redis.queuesConfig[0].gate_type=constant' \
        --set 'ap.redis.queuesConfig[0].gate_params=null' \
        -n "${NAMESPACE}" --reuse-values

    kubectl rollout restart deployment -l "app.kubernetes.io/name=async-processor" -n "${NAMESPACE}"
    kubectl rollout status deployment -l "app.kubernetes.io/name=async-processor" -n "${NAMESPACE}" --timeout=120s

    # Start batch submission + guidellm
    run_batch_submit
    sleep 10

    run_guidellm_sweep "ungated"

    wait_for_job "batch-submit" 600 || true
    cleanup_job "batch-submit"

    # Restore the original gate configuration
    log "Restoring original dispatcher gate configuration"
    helm upgrade "${ASYNC_RELEASE}" "${ASYNC_CHART}" \
        -f "${ASYNC_VALUES}" \
        -n "${NAMESPACE}" --reuse-values
    kubectl rollout restart deployment -l "app.kubernetes.io/name=async-processor" -n "${NAMESPACE}"
    kubectl rollout status deployment -l "app.kubernetes.io/name=async-processor" -n "${NAMESPACE}" --timeout=120s
}

# ── Summary ───────────────────────────────────────────────────────────

print_summary() {
    log "━━━ Benchmark Complete ━━━"
    echo ""
    echo "Results saved to: ${RESULTS_DIR}/"
    echo ""
    echo "Scenarios completed:"
    for scenario in baseline gated ungated; do
        if [ -d "${RESULTS_DIR}/${scenario}" ]; then
            local files
            files=$(find "${RESULTS_DIR}/${scenario}" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
            echo "  ${scenario}: ${files} result file(s)"
        else
            echo "  ${scenario}: (not run)"
        fi
    done
    echo ""
    echo "To compare results, inspect the JSON files in each scenario directory."
    echo "Key metrics to compare across scenarios:"
    echo "  - time_to_first_token_ms (TTFT)"
    echo "  - inter_token_latency_ms (ITL)"
    echo "  - request_latency"
    echo "  - requests_per_second"
    echo "  - output_tokens_per_second"
    echo ""
    echo "Expected outcome:"
    echo "  - 'gated' metrics should closely match 'baseline' (gate protects live traffic)"
    echo "  - 'ungated' metrics should show degradation vs 'baseline' (no protection)"
}

# ── Main ──────────────────────────────────────────────────────────────

main() {
    log "Batch vs Live Traffic Benchmark"
    log "Namespace: ${NAMESPACE}"
    log "Batch size: ${BATCH_SIZE}"
    log "Sweep duration: ${GUIDELLM_MAX_SECONDS}s per scenario"

    ensure_pvc
    run_baseline
    run_gated
    run_ungated
    print_summary
}

main
