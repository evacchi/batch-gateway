# Benchmark: Batch vs Live Traffic Isolation

Measure whether batch requests degrade live (interactive) traffic quality,
and how routing through the async dispatcher with a dispatch budget gate helps.

The benchmark uses [guidellm](https://github.com/vllm-project/guidellm) to
generate a **burst/idle traffic pattern** (simulating real production traffic
spikes) while batch requests run alongside. It compares two dispatch modes:

| Scenario | Dispatch mode | Batch load | Gate |
|----------|--------------|------------|------|
| **sync** | sync (direct) | 50 requests | none |
| **gated** | async | 50 requests | prometheus-budget |

Each scenario runs guidellm in alternating phases:
- **Burst**: 15 req/s for 60s (saturates the inference endpoint)
- **Idle**: 1 req/s for 60s (leaves headroom for batch)

**Expected outcome:**
- **sync**: Batch requests cannot complete — they compete directly with live
  traffic during bursts and retry with backoff, consuming capacity even during
  idle periods
- **gated**: Batch requests complete during idle phases — the dispatch budget
  gate closes during bursts (protecting live traffic) and opens during idle
  periods (filling unused capacity with batch work)

## Prerequisites

- Kubernetes cluster with GPU nodes (A100 tested)
- `kubectl`, `helm`, `jq`, `envsubst` installed
- Local checkouts of:
  - [llm-d](https://github.com/llm-d/llm-d)
  - [llm-d-async](https://github.com/llm-d-incubation/llm-d-async)
  - [llm-d-router](https://github.com/llm-d/llm-d-router)
  - this repo (llm-d-batch-gateway)

```bash
export LLM_D_REPO=/path/to/llm-d
export ASYNC_REPO=/path/to/llm-d-async
export ROUTER_REPO=/path/to/llm-d-router
export BATCH_REPO=/path/to/llm-d-batch-gateway   # this repo
export NAMESPACE=llm-d-async                      # or your namespace
export GUIDE_NAME=optimized-baseline
```

## Step 1: Install CRDs (skip if already installed)

```bash
GATEWAY_API_VERSION=v1.5.1
GAIE_VERSION=v1.5.0

kubectl apply -k "https://github.com/kubernetes-sigs/gateway-api/config/crd?ref=${GATEWAY_API_VERSION}"
kubectl apply -k "https://github.com/kubernetes-sigs/gateway-api-inference-extension/config/crd?ref=${GAIE_VERSION}"
```

## Step 2: Create namespace

```bash
kubectl create namespace ${NAMESPACE}
```

## Step 3: Install Istio (skip if already installed)

```bash
kubectl get pods -n istio-system  # check first

# If not installed:
ISTIO_VERSION=1.29.0
curl -L https://istio.io/downloadIstio | ISTIO_VERSION=${ISTIO_VERSION} sh -
export PATH="$PWD/istio-${ISTIO_VERSION}/bin:$PATH"
istioctl install -y --set values.pilot.env.ENABLE_GATEWAY_API_INFERENCE_EXTENSION=true
```

## Step 4: Deploy Gateway

```bash
kubectl apply -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
kubectl wait --for=jsonpath='{.status.conditions[?(@.type=="Programmed")].status}'=True \
    gateway/llm-d-inference-gateway -n ${NAMESPACE} --timeout=120s
```

## Step 5: Deploy llm-d Router (EPP) with HTTPRoute and monitoring

Build the chart dependencies first:

```bash
cd ${ROUTER_REPO}/config/charts/llm-d-router-gateway && helm dependency build && cd -
```

Install with HTTPRoute enabled so the gateway routes traffic to the EPP:

```bash
helm install ${GUIDE_NAME} \
    ${ROUTER_REPO}/config/charts/llm-d-router-gateway/ \
    -f ${LLM_D_REPO}/guides/recipes/router/base.values.yaml \
    -f ${LLM_D_REPO}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml \
    -f ${LLM_D_REPO}/guides/recipes/router/features/monitoring.values.yaml \
    --set provider.name=istio \
    --set httpRoute.create=true \
    --set httpRoute.inferenceGatewayName=llm-d-inference-gateway \
    -n ${NAMESPACE}
```

> **Note:** `--set provider.name=istio` is required — it creates a DestinationRule
> that allows the Istio gateway proxy to reach the EPP's ext-proc gRPC service.
> Without it, requests will fail with 500 errors.

## Step 6: Deploy vLLM model server (Qwen/Qwen3-0.6B)

```bash
kubectl apply -n ${NAMESPACE} -k ${ASYNC_REPO}/docs/guides/e2e-deploy/modelserver/
kubectl wait --for=condition=Ready pod -l llm-d.ai/role=decode -n ${NAMESPACE} --timeout=300s
```

## Step 7: Install Prometheus (skip if already installed)

```bash
cd ${LLM_D_REPO}
./docs/monitoring/scripts/install-prometheus-grafana.sh
```

Verify EPP metrics are flowing:

```bash
kubectl run --rm -i prom-check --image=curlimages/curl --restart=Never -n ${NAMESPACE} -- \
    curl -s "http://llmd-kube-prometheus-stack-prometheus.llm-d-monitoring.svc.cluster.local:9090/api/v1/query?query=inference_pool_ready_pods"
```

## Step 8: Install Redis

```bash
helm install redis bitnami/redis -n ${NAMESPACE} --set auth.enabled=false
```

## Step 9: Install PostgreSQL

The batch-gateway uses PostgreSQL as its metadata store.

```bash
helm install postgresql bitnami/postgresql -n ${NAMESPACE} \
    --set auth.postgresPassword=benchmarkpw \
    --set auth.database=batchgateway \
    --set primary.persistence.size=5Gi

kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=postgresql \
    -n ${NAMESPACE} --timeout=120s
```

## Step 10: Create batch-gateway secrets

```bash
kubectl create secret generic batch-gateway-secrets -n ${NAMESPACE} \
    --from-literal=redis-url="redis://redis-master.${NAMESPACE}.svc.cluster.local:6379" \
    --from-literal=postgresql-url="postgresql://postgres:benchmarkpw@postgresql.${NAMESPACE}.svc.cluster.local:5432/batchgateway?sslmode=disable" \
    --from-literal=inference-api-key="" \
    --from-literal=s3-secret-access-key=""
```

## Step 11: Create batch-gateway files PVC

```bash
kubectl apply -n ${NAMESPACE} -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: batch-gateway-files
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
EOF
```

## Step 12: Deploy batch-gateway

Start in **sync** dispatch mode for the baseline and sync scenarios:

```bash
helm install batch-gateway ${BATCH_REPO}/charts/batch-gateway/ \
    -f ${BATCH_REPO}/test/e2e/benchmark/processor-sync-values.yaml \
    --set 'global.secretName=batch-gateway-secrets' \
    --set 'global.fileClient.type=fs' \
    --set 'global.fileClient.fs.pvcName=batch-gateway-files' \
    --set 'gc.enabled=false' \
    -n ${NAMESPACE}
```

## Step 13: Deploy async processor with dispatch budget gate

```bash
helm install async-processor ${ASYNC_REPO}/charts/async-processor/ \
    -f ${ASYNC_REPO}/docs/guides/e2e-deploy/async-processor-values.yaml \
    --set "ap.redis.url=redis://redis-master.${NAMESPACE}.svc.cluster.local:6379" \
    -n ${NAMESPACE}
```

> **Note:** The `async-processor-values.yaml` references image tag `938cd44`.
> If you need a newer version, override with
> `--set ap.image.repository=<repo> --set ap.image.tag=<tag>`.
> The image must be built for `linux/amd64` — if building on Apple Silicon, use
> `docker buildx build --platform linux/amd64`.

The async processor is not needed for the baseline and sync scenarios. You can
scale it to 0 initially and bring it up for the gated/ungated scenarios:

```bash
kubectl scale deployment async-processor -n ${NAMESPACE} --replicas=0
```

## Step 14: Verify the stack

```bash
# All pods running
kubectl get pods -n ${NAMESPACE}

# Gateway routes to vLLM
kubectl run --rm -i test-gateway --image=curlimages/curl --restart=Never \
    -n ${NAMESPACE} -- curl -s "http://llm-d-inference-gateway-istio/v1/models"
# Expected: JSON with "id":"Qwen/Qwen3-0.6B"
```

## Step 15: Create the results PVC

```bash
kubectl apply -n ${NAMESPACE} -f ${BATCH_REPO}/test/e2e/benchmark/results-pvc.yaml
```

## Step 16: Run the benchmark

The benchmark requires **two namespaces** — one for sync dispatch, one for
gated async dispatch. Deploy the full stack (Steps 1–15) in both namespaces,
configuring the sync namespace with `processor-sync-values.yaml` and the
async namespace with `processor-async-values.yaml`.

```bash
export SYNC_NAMESPACE=my-sync-ns
export GATED_NAMESPACE=my-gated-ns
```

Run the benchmark script:

```bash
cd ${BATCH_REPO}

python3 test/e2e/benchmark/benchmark.py \
    --context <your-kubectl-context> \
    --sync-namespace ${SYNC_NAMESPACE} \
    --gated-namespace ${GATED_NAMESPACE} \
    --batch-size 50 \
    --burst-rate 15 \
    --idle-rate 1 \
    --phase-seconds 60 \
    --cycles 2 \
    --results-dir ./benchmark-results
```

| Flag | Default | Description |
|------|---------|-------------|
| `--context` | (required) | kubectl context |
| `--sync-namespace` | (required) | Namespace with sync dispatch |
| `--gated-namespace` | (required) | Namespace with async dispatch + gate |
| `--batch-size` | `50` | Number of requests in the batch workload |
| `--burst-rate` | `15` | Requests/sec during burst phases |
| `--idle-rate` | `1` | Requests/sec during idle phases |
| `--phase-seconds` | `60` | Duration of each burst/idle phase |
| `--cycles` | `2` | Number of burst→idle cycles |
| `--results-dir` | `./benchmark-results` | Output directory |

The script will:
1. Clean up stale data (flush Redis, truncate PostgreSQL, restart processors)
2. Submit a batch, then start the burst/idle guidellm pattern (**sync**)
3. Monitor batch progress throughout, recording a timeline
4. Repeat for the **gated** namespace
5. Collect guidellm CSVs from both namespaces
6. Generate an HTML report with charts at `benchmark-results/report.html`

### Cleanup between runs

The benchmark script handles cleanup automatically, but if running manually:

```bash
# Scale down processors
kubectl scale deployment batch-gateway-processor batch-gateway-apiserver -n ${NAMESPACE} --replicas=0

# Delete all Redis keys (FLUSHALL is disabled on Bitnami Redis)
kubectl run --rm -i redis-del -n ${NAMESPACE} --image=redis --restart=Never -- \
    sh -c 'for key in $(redis-cli -h redis-master KEYS "*"); do redis-cli -h redis-master DEL "$key"; done'

# Truncate PostgreSQL
kubectl run --rm -i pg-nuke -n ${NAMESPACE} --image=postgres:16 --restart=Never \
    --env=PGPASSWORD=benchmarkpw -- \
    psql -h postgresql -U postgres -d batchgateway -c "TRUNCATE batch_items, file_items CASCADE;"

# Scale back up
kubectl scale deployment batch-gateway-processor batch-gateway-apiserver -n ${NAMESPACE} --replicas=1
```

## Interpret results

The HTML report (`benchmark-results/report.html`) includes:

- **Batch completion timeline**: Shows completed requests over time for both
  scenarios. The gated line should show a staircase pattern — flat during bursts
  (gate closed), rising during idle phases (gate open). The sync line should
  stay flat (no progress under load).

- **TTFT comparison**: Time-to-first-token across burst and idle phases for
  both scenarios.

- **Detailed metrics table**: Per-phase breakdown of TTFT, ITL, request
  latency, throughput, completed requests, and errors.

### What to look for

- **Batch completion**: Gated completes batch requests during idle phases; sync
  cannot complete any under burst load
- **Live traffic quality**: TTFT and ITL during burst phases should be similar
  between sync and gated — the gate protects live traffic without degrading it
- **Gate responsiveness**: The delay between a burst ending and batch
  resuming shows the Prometheus scrape interval lag (~15s)

## Customizing the workload

Edit `benchmark.py` parameters or the inline Job YAML to change:

- `--burst-rate` / `--idle-rate` — adjust traffic intensity
- `--phase-seconds` — longer phases show more batch progress
- `--data "prompt_tokens=2000,output_tokens=500"` in the guidellm command —
  simulate summarization-like traffic instead of short prompts

> **Note:** Do not use `GUIDELLM_*` as environment variable names in Job
> manifests. guidellm auto-reads environment variables matching this prefix
> as CLI flags, which causes unexpected behavior.

## Cleanup

```bash
kubectl delete job guidellm-sweep batch-submit -n ${NAMESPACE} --ignore-not-found
kubectl delete pvc benchmark-results -n ${NAMESPACE} --ignore-not-found
helm uninstall batch-gateway -n ${NAMESPACE}
helm uninstall async-processor -n ${NAMESPACE}
helm uninstall redis -n ${NAMESPACE}
helm uninstall postgresql -n ${NAMESPACE}
kubectl delete secret batch-gateway-secrets -n ${NAMESPACE}
kubectl delete pvc batch-gateway-files -n ${NAMESPACE}
kubectl delete -n ${NAMESPACE} -k ${ASYNC_REPO}/docs/guides/e2e-deploy/modelserver/
helm uninstall ${GUIDE_NAME} -n ${NAMESPACE}
kubectl delete -k ${LLM_D_REPO}/guides/recipes/gateway/istio -n ${NAMESPACE}
```
