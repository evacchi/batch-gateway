#!/usr/bin/env bash
set -euo pipefail

# Benchmark environment setup.
# Deploys the full stack in two namespaces (sync and gated/async).
#
# Required env vars:
#   KUBE_CONTEXT       — kubectl context (e.g. coreweave-waldorf)
#   SYNC_NAMESPACE     — namespace for sync dispatch scenario
#   GATED_NAMESPACE    — namespace for gated async dispatch scenario
#   LLM_D_REPO        — path to llm-d checkout
#   ASYNC_REPO         — path to llm-d-async checkout
#   ROUTER_REPO        — path to llm-d-router checkout
#
# Optional:
#   BATCH_REPO         — path to llm-d-batch-gateway (default: repo root)
#   GUIDE_NAME         — inference pool name (default: optimized-baseline)
#   MAX_CONCURRENCY    — gate max concurrency (default: 30)
#   AP_IMAGE_REPO      — async-processor image repo override
#   AP_IMAGE_TAG       — async-processor image tag override
#   BG_IMAGE_REPO      — batch-gateway image repo override (apiserver + processor)
#   BG_IMAGE_TAG       — batch-gateway image tag override

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_REPO="${BATCH_REPO:-$(cd "${SCRIPT_DIR}/../../.." && pwd)}"
GUIDE_NAME="${GUIDE_NAME:-optimized-baseline}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-30}"

for var in KUBE_CONTEXT SYNC_NAMESPACE GATED_NAMESPACE LLM_D_REPO ASYNC_REPO ROUTER_REPO; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set" >&2
        exit 1
    fi
done

K="kubectl --context=${KUBE_CONTEXT}"
H="helm --kube-context=${KUBE_CONTEXT}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

deploy_namespace() {
    local NS="$1"
    local MODE="$2"  # "sync" or "async"

    log "--- Deploying ${MODE} stack in ${NS} ---"

    # Create namespace
    ${K} create namespace "${NS}" 2>/dev/null || true

    # Redis
    log "  Installing Redis"
    ${H} install redis oci://registry-1.docker.io/bitnamicharts/redis \
        -n "${NS}" \
        --set auth.enabled=false \
        --set master.persistence.size=1Gi \
        --set replica.replicaCount=0 \
        --wait --timeout 120s >/dev/null

    # PostgreSQL
    log "  Installing PostgreSQL"
    ${H} install postgresql oci://registry-1.docker.io/bitnamicharts/postgresql \
        -n "${NS}" \
        --set auth.database=batchgateway \
        --set auth.password=benchmarkpw \
        --set primary.persistence.size=5Gi \
        --wait --timeout 120s >/dev/null

    # Secrets
    log "  Creating secrets"
    ${K} -n "${NS}" create secret generic batch-gateway-secrets \
        --from-literal=redis-url="redis://redis-master.${NS}.svc.cluster.local:6379" \
        --from-literal=postgresql-url="postgresql://postgres:benchmarkpw@postgresql.${NS}.svc.cluster.local:5432/batchgateway?sslmode=disable" \
        --from-literal=inference-api-key="" \
        --from-literal=s3-secret-access-key=""

    # PVCs
    log "  Creating PVCs"
    ${K} -n "${NS}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: batch-gateway-files
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
EOF
    ${K} -n "${NS}" apply -f "${BATCH_REPO}/test/e2e/benchmark/results-pvc.yaml"

    # EPP
    log "  Installing EPP (${GUIDE_NAME})"
    if [ ! -f "${ROUTER_REPO}/config/charts/llm-d-router-gateway/charts/router-0.0.0.tgz" ]; then
        (cd "${ROUTER_REPO}/config/charts/llm-d-router-gateway" && helm dependency build >/dev/null 2>&1)
    fi
    ${H} install "${GUIDE_NAME}" \
        "${ROUTER_REPO}/config/charts/llm-d-router-gateway/" \
        -n "${NS}" \
        -f "${LLM_D_REPO}/guides/recipes/router/base.values.yaml" \
        -f "${LLM_D_REPO}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml" \
        -f "${LLM_D_REPO}/guides/recipes/router/features/monitoring.values.yaml" \
        --set provider.name=istio \
        --set httpRoute.create=true \
        --set httpRoute.inferenceGatewayName=llm-d-inference-gateway >/dev/null

    # Istio Gateway
    log "  Creating Istio Gateway"
    ${K} -n "${NS}" apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: llm-d-inference-gateway
  annotations:
    networking.istio.io/service-type: ClusterIP
spec:
  gatewayClassName: istio
  listeners:
  - name: default
    port: 80
    protocol: HTTP
    allowedRoutes:
      namespaces:
        from: Same
EOF

    # vLLM
    log "  Deploying vLLM (Qwen3-8B)"
    ${K} -n "${NS}" apply -k "${BATCH_REPO}/test/e2e/benchmark/modelserver/"

    # Batch gateway
    local VALUES_FILE
    if [ "${MODE}" = "sync" ]; then
        VALUES_FILE="${BATCH_REPO}/test/e2e/benchmark/processor-sync-values.yaml"
    else
        VALUES_FILE="${BATCH_REPO}/test/e2e/benchmark/processor-async-values.yaml"
    fi

    log "  Installing batch-gateway (${MODE})"
    local BG_EXTRA_ARGS=()
    if [ -n "${BG_IMAGE_REPO:-}" ]; then
        BG_EXTRA_ARGS+=(
            --set "apiserver.image.repository=${BG_IMAGE_REPO}-apiserver"
            --set "processor.image.repository=${BG_IMAGE_REPO}-processor"
        )
    fi
    if [ -n "${BG_IMAGE_TAG:-}" ]; then
        BG_EXTRA_ARGS+=(
            --set-string "apiserver.image.tag=${BG_IMAGE_TAG}"
            --set-string "processor.image.tag=${BG_IMAGE_TAG}"
        )
    fi

    ${H} install batch-gateway \
        "${BATCH_REPO}/charts/batch-gateway/" \
        -n "${NS}" \
        -f "${VALUES_FILE}" \
        --set global.secretName=batch-gateway-secrets \
        --set global.fileClient.type=fs \
        --set global.fileClient.fs.pvcName=batch-gateway-files \
        --set gc.enabled=false \
        "${BG_EXTRA_ARGS[@]+"${BG_EXTRA_ARGS[@]}"}" >/dev/null

    # TMPDIR fix for large file uploads
    ${K} -n "${NS}" set env deploy/batch-gateway-apiserver TMPDIR=/tmp/batch-gateway >/dev/null

    # Async processor (gated namespace only)
    if [ "${MODE}" = "async" ]; then
        log "  Installing async-processor (max_concurrency=${MAX_CONCURRENCY})"
        local AP_EXTRA_ARGS=()
        if [ -n "${AP_IMAGE_REPO:-}" ]; then
            AP_EXTRA_ARGS+=(--set "ap.image.repository=${AP_IMAGE_REPO}")
        fi
        if [ -n "${AP_IMAGE_TAG:-}" ]; then
            AP_EXTRA_ARGS+=(--set-string "ap.image.tag=${AP_IMAGE_TAG}")
        fi

        ${H} install async-processor \
            "${ASYNC_REPO}/charts/async-processor/" \
            -n "${NS}" \
            -f "${ASYNC_REPO}/docs/guides/e2e-deploy/async-processor-values.yaml" \
            --set "ap.redis.url=redis://redis-master.${NS}.svc.cluster.local:6379" \
            --set "ap.redis.resultQueueName=llm-d-async:results:${GUIDE_NAME}" \
            --set "ap.redis.queuesConfig[0].queue_name=llm-d-async:requests:${GUIDE_NAME}" \
            --set "ap.redis.queuesConfig[0].request_path_url=/v1/completions" \
            --set "ap.redis.queuesConfig[0].igw_base_url=http://llm-d-inference-gateway-istio:80" \
            --set "ap.redis.queuesConfig[0].gate_type=prometheus-query" \
            --set-string "ap.redis.queuesConfig[0].gate_params.query=1 - (sum(vllm:num_requests_running{namespace=\"${NS}\"}) / on() (inference_pool_ready_pods{name=\"${GUIDE_NAME}\"\,namespace=\"${NS}\"} * ${MAX_CONCURRENCY}))" \
            --set "ap.redis.queuesConfig[0].gate_params.fallback=0.0" \
            "${AP_EXTRA_ARGS[@]+"${AP_EXTRA_ARGS[@]}"}" >/dev/null
    fi

    # Wait for vLLM
    log "  Waiting for vLLM to be ready (this may take a few minutes)..."
    ${K} -n "${NS}" wait pod -l llm-d.ai/role=decode \
        --for=condition=Ready --timeout=300s >/dev/null

    # Wait for batch-gateway
    ${K} -n "${NS}" rollout status deploy/batch-gateway-apiserver --timeout=60s >/dev/null
    ${K} -n "${NS}" rollout status deploy/batch-gateway-processor --timeout=60s >/dev/null

    log "  ${NS} ready"
}

smoke_test() {
    local NS="$1"
    log "Smoke-testing inference in ${NS}"
    local CODE
    CODE=$(${K} -n "${NS}" run smoke-test --rm -i --restart=Never \
        --image=curlimages/curl:latest -- \
        curl -s -o /dev/null -w '%{http_code}' \
        -X POST http://llm-d-inference-gateway-istio:80/v1/completions \
        -H 'Content-Type: application/json' \
        -d '{"model":"Qwen/Qwen3-8B","prompt":"Hello","max_tokens":5}' 2>/dev/null) || true
    if [ "${CODE}" = "200" ]; then
        log "  ${NS}: inference OK"
    else
        log "  WARNING: ${NS}: inference returned ${CODE}"
    fi
}

log "=== Setting up benchmark environment ==="
log "Context:    ${KUBE_CONTEXT}"
log "Sync NS:    ${SYNC_NAMESPACE}"
log "Gated NS:   ${GATED_NAMESPACE}"

deploy_namespace "${SYNC_NAMESPACE}" "sync"
deploy_namespace "${GATED_NAMESPACE}" "async"

smoke_test "${SYNC_NAMESPACE}"
smoke_test "${GATED_NAMESPACE}"

log "=== Setup complete ==="
log "Run the benchmark with:"
log "  python3 test/e2e/benchmark/benchmark.py \\"
log "    --context ${KUBE_CONTEXT} \\"
log "    --sync-namespace ${SYNC_NAMESPACE} \\"
log "    --gated-namespace ${GATED_NAMESPACE} \\"
log "    --batch-size 5000 --burst-rate 15 --idle-rate 1 \\"
log "    --burst-seconds 60 --idle-seconds 120 --cycles 2"
