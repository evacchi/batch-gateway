# Benchmark: Batch vs Live Traffic Isolation

Measure whether batch requests degrade live (interactive) traffic quality,
and how routing through the async dispatcher with a dispatch budget gate helps.

The benchmark runs [guidellm](https://github.com/vllm-project/guidellm) in
**sweep mode** against the inference gateway to produce a latency/throughput
saturation curve across four scenarios:

| Scenario | Dispatch mode | Batch load | Gate |
|----------|--------------|------------|------|
| **baseline** | n/a | none | n/a |
| **sync** | sync (direct) | 100 requests | none |
| **gated** | async | 100 requests | prometheus-budget |
| **ungated** | async | 100 requests | constant (always open) |

**Expected outcome:**
- **sync** shows degradation vs **baseline** — batch requests compete directly
  with live traffic for inference capacity with no gating mechanism
- **gated** should produce metrics close to **baseline** — the dispatch budget
  gate throttles batch requests when the inference endpoint is under live load
- **ungated** should show degradation similar to **sync** — async dispatch
  without a gate provides no protection

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
    --set router.inferencePool.gatewayRef.name=llm-d-inference-gateway \
    --set httpRoute.create=true \
    --set httpRoute.inferenceGatewayName=llm-d-inference-gateway \
    -n ${NAMESPACE}
```

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

The benchmark script orchestrates four scenarios sequentially:

```bash
cd ${BATCH_REPO}

./test/e2e/benchmark/benchmark.sh \
    --namespace ${NAMESPACE} \
    --results-dir ./benchmark-results \
    --batch-size 100 \
    --max-seconds 120 \
    --batch-release batch-gateway \
    --batch-chart ${BATCH_REPO}/charts/batch-gateway/ \
    --async-release async-processor \
    --async-chart ${ASYNC_REPO}/charts/async-processor/ \
    --async-values ${ASYNC_REPO}/docs/guides/e2e-deploy/async-processor-values.yaml
```

| Flag | Default | Description |
|------|---------|-------------|
| `--namespace` | `llm-d-async` | Kubernetes namespace |
| `--results-dir` | `./benchmark-results` | Local directory for collected results |
| `--batch-size` | `100` | Number of requests in the batch workload |
| `--max-seconds` | `120` | guidellm sweep duration per scenario |
| `--batch-release` | `batch-gateway` | Helm release name of the batch-gateway |
| `--batch-chart` | — | Path to batch-gateway Helm chart (required for sync scenario) |
| `--async-release` | `async-processor` | Helm release name of the async-processor |
| `--async-chart` | — | Path to async-processor Helm chart (required for ungated scenario) |
| `--async-values` | — | Path to async-processor values file (required for ungated scenario) |

The script will:
1. Run guidellm sweep with no batch load (**baseline**)
2. Switch processor to sync mode, submit a batch, run guidellm sweep (**sync**)
3. Switch processor back to async mode, submit a batch, run guidellm sweep (**gated**)
4. Reconfigure the dispatcher gate to `constant` (always open), submit a batch,
   run guidellm sweep (**ungated**)
5. Restore the original gate configuration
6. Print a summary of collected results

The **sync** scenario requires `--batch-chart`. The **ungated** scenario
requires `--async-chart` and `--async-values`. Missing flags cause the
respective scenario to be skipped.

## Running individual scenarios

You can run the guidellm sweep or batch submission Jobs independently.

The guidellm-sweep.yaml uses `envsubst` templating — set environment variables
before applying:

### guidellm sweep only

```bash
export GUIDELLM_SCENARIO=manual-test
export GUIDELLM_MAX_SECONDS=120
export GUIDELLM_TARGET="http://llm-d-inference-gateway-istio"
export GUIDELLM_MODEL="Qwen/Qwen3-0.6B"
envsubst < test/e2e/benchmark/guidellm-sweep.yaml | kubectl apply -n ${NAMESPACE} -f -

kubectl wait --for=condition=complete job/guidellm-sweep -n ${NAMESPACE} --timeout=300s
kubectl logs job/guidellm-sweep -n ${NAMESPACE}
```

### Batch submission only

The batch-submit Job uses container env vars (no envsubst). Apply directly:

```bash
kubectl apply -n ${NAMESPACE} -f test/e2e/benchmark/batch-submit.yaml

kubectl logs -f job/batch-submit -n ${NAMESPACE}
```

To override defaults, patch the env vars before applying:

```bash
sed -e 's|http://batch-gateway-apiserver:8000|http://my-gateway:8000|' \
    -e 's|value: "100"|value: "50"|' \
    test/e2e/benchmark/batch-submit.yaml | kubectl apply -n ${NAMESPACE} -f -
```

## Interpret results

Results are collected as JSON and CSV files under `./benchmark-results/` (e.g.,
`baseline.json`, `gated.json`). Each JSON file contains guidellm's standard
benchmark output with per-request metrics.

Key metrics to compare across scenarios:

| Metric | What it measures |
|--------|-----------------|
| `time_to_first_token_ms` | Initial response latency (TTFT) |
| `inter_token_latency_ms` | Speed of subsequent token generation (ITL) |
| `request_latency` | End-to-end request duration |
| `requests_per_second` | Throughput |
| `output_tokens_per_second` | Token generation throughput |

Look at the sweep curve inflection point across scenarios:
- **sync** vs **baseline**: shows the cost of uncontrolled batch traffic
  competing directly with live requests for inference capacity
- **gated** vs **baseline**: if the curves match, the dispatch budget gate is
  working — batch requests back off when the endpoint is saturated
- **ungated** vs **baseline**: shows that async dispatch alone (without a gate)
  does not protect live traffic — degradation should be similar to **sync**

## Customizing the workload

Edit `test/e2e/benchmark/guidellm-sweep.yaml` to change the guidellm
parameters:

- `--data "prompt_tokens=256,output_tokens=128"` — adjust prompt/output token
  counts to simulate different workload profiles (e.g., `prompt_tokens=2000,output_tokens=500`
  for summarization-like traffic)
- `--profile constant --rate 50` — switch from sweep to constant-rate load
  (useful for sustained-load comparison rather than saturation curve)
- `--request-format chat_completions` — switch to chat completion format

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
