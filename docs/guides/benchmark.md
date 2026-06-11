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
- The llm-d stack deployed with the async processor — follow the
  [llm-d-async e2e deploy guide](https://github.com/llm-d-incubation/llm-d-async/blob/main/docs/guides/e2e-deploy.md)
  through **Step 9** (CRDs, Istio, gateway, EPP, vLLM, Prometheus, Redis,
  async-processor with prometheus-budget gate)
- batch-gateway deployed and configured for async dispatch (see Step 1 below)

```bash
export LLM_D_REPO=/path/to/llm-d
export ASYNC_REPO=/path/to/llm-d-async
export BATCH_REPO=/path/to/llm-d-batch-gateway   # this repo
export NAMESPACE=llm-d-async
```

## Step 1: Configure batch-gateway for async dispatch

The batch-gateway processor needs to route requests through the dispatcher
instead of calling the inference endpoint directly.

```bash
# Get current processor values
helm get values batch-gateway -n ${NAMESPACE} -o yaml > /tmp/current-values.yaml

# Upgrade with async dispatch enabled
helm upgrade batch-gateway ${BATCH_REPO}/charts/batch-gateway/ \
    -f /tmp/current-values.yaml \
    -f ${BATCH_REPO}/test/e2e/benchmark/processor-async-values.yaml \
    -n ${NAMESPACE}

kubectl rollout restart deployment -l app.kubernetes.io/component=processor -n ${NAMESPACE}
kubectl rollout status deployment -l app.kubernetes.io/component=processor -n ${NAMESPACE} --timeout=120s
```

The `processor-async-values.yaml` sets:
- `dispatchMode: async` — routes requests through the dispatcher
- `resultPollTimeout: 30s` — how long the processor waits for each result
- Model `Qwen/Qwen3-0.6B` → pool `optimized-baseline` — maps the model to
  the dispatcher's queue/pool

## Step 2: Create the results PVC

```bash
kubectl apply -n ${NAMESPACE} -f ${BATCH_REPO}/test/e2e/benchmark/results-pvc.yaml
```

## Step 3: Run the benchmark

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

## Step 4: Interpret results

Results are collected as JSON files under `./benchmark-results/<scenario>/`.
Each file contains guidellm's standard benchmark output with per-request metrics.

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

## Running individual scenarios

You can run the guidellm sweep or batch submission Jobs independently:

### guidellm sweep only

```bash
export GUIDELLM_SCENARIO=manual-test
export GUIDELLM_MAX_SECONDS=120
envsubst < test/e2e/benchmark/guidellm-sweep.yaml | kubectl apply -n ${NAMESPACE} -f -

kubectl wait --for=condition=complete job/guidellm-sweep -n ${NAMESPACE} --timeout=300s
kubectl logs job/guidellm-sweep -n ${NAMESPACE}
```

### Batch submission only

```bash
export BATCH_SIZE=50
envsubst < test/e2e/benchmark/batch-submit.yaml | kubectl apply -n ${NAMESPACE} -f -

kubectl logs -f job/batch-submit -n ${NAMESPACE}
```

## Customizing the workload

Edit `test/e2e/benchmark/guidellm-sweep.yaml` to change the guidellm
parameters:

- `--data "prompt_tokens=256,output_tokens=128"` — adjust prompt/output token
  counts to simulate different workload profiles (e.g., `prompt_tokens=2000,output_tokens=500`
  for summarization-like traffic)
- `--profile constant --rate 50` — switch from sweep to constant-rate load
  (useful for sustained-load comparison rather than saturation curve)
- `--request-format chat_completions` — switch to chat completion format

## Local testing (Kind cluster with vLLM simulator)

The benchmark can be smoke-tested locally using the Kind cluster and vLLM
simulator from the dev-deploy setup. Note that the vLLM simulator has fixed
latency, so the results will **not show realistic saturation behavior** — this
is only useful for verifying the script runs end-to-end.

```bash
# Deploy the base cluster and dispatcher
make dev-deploy
make dev-deploy-dispatcher

# The Kind setup uses different service names and ports.
# Edit guidellm-sweep.yaml to target the simulator:
#   GUIDELLM_TARGET: "http://vllm-sim.default.svc.cluster.local:8000"
#   GUIDELLM_MODEL: "sim-model"
# Edit batch-submit.yaml:
#   BATCH_GATEWAY_URL: "https://batch-gateway-apiserver.default.svc.cluster.local:443"
#   BATCH_MODEL: "sim-model"

# Run the benchmark
NAMESPACE=default ./test/e2e/benchmark/benchmark.sh \
    --results-dir ./benchmark-results \
    --batch-size 10 \
    --max-seconds 30
```

## Cleanup

```bash
kubectl delete job guidellm-sweep batch-submit -n ${NAMESPACE} --ignore-not-found
kubectl delete pvc benchmark-results -n ${NAMESPACE} --ignore-not-found
```
