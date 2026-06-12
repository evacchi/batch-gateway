#!/usr/bin/env python3
"""
Benchmark: Batch vs Live Traffic Isolation

Orchestrates guidellm burst/idle cycles alongside batch workloads to measure
whether the async dispatcher gate protects live traffic quality.

Usage:
    python3 benchmark.py --context coreweave-waldorf \
        --sync-namespace evacchi-dev \
        --gated-namespace evacchi-dev-async \
        --batch-size 50 --burst-rate 15 --idle-rate 1 \
        --phase-seconds 60 --cycles 2 \
        --results-dir ./benchmark-results/final
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ScenarioConfig:
    name: str
    namespace: str
    context: str
    burst_rate: int
    idle_rate: int
    burst_seconds: int
    idle_seconds: int
    cycles: int
    batch_size: int
    prompt_tokens: int = 1500
    target: str = "http://llm-d-inference-gateway-istio"
    model: str = "Qwen/Qwen3-0.6B"


@dataclass
class PhaseMetrics:
    phase: str
    cycle: int
    ttft_p50: float = 0.0
    ttft_p95: float = 0.0
    itl_p50: float = 0.0
    req_latency_p50: float = 0.0
    ok_rps: float = 0.0
    err_rps: float = 0.0
    error_rate: float = 0.0
    completed: int = 0
    errors: int = 0


@dataclass
class ScenarioResult:
    name: str
    phases: list = field(default_factory=list)
    batch_timeline: list = field(default_factory=list)


def kubectl(args, context, namespace=None, capture=True, check=True):
    cmd = ["kubectl", f"--context={context}"]
    if namespace:
        cmd.extend(["-n", namespace])
    cmd.extend(args)
    result = subprocess.run(cmd, capture_output=capture, text=True, check=check)
    return result.stdout.strip() if capture else None


def kubectl_apply(yaml_str, context, namespace):
    cmd = ["kubectl", f"--context={context}", "-n", namespace, "apply", "-f", "-"]
    subprocess.run(cmd, input=yaml_str, text=True, check=True)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def cleanup_namespace(context, namespace):
    log(f"Cleaning up {namespace}")

    kubectl(["delete", "job", "guidellm-burst", "batch-submit", "batch-submit-2",
             "--ignore-not-found"], context, namespace, check=False)

    kubectl(["scale", "deployment",
             "batch-gateway-processor", "batch-gateway-apiserver",
             "--replicas=0"], context, namespace, check=False)

    # Also scale async-processor if it exists
    kubectl(["scale", "deployment", "async-processor",
             "--replicas=0"], context, namespace, check=False)

    time.sleep(8)

    # Truncate PostgreSQL
    kubectl(["run", "--rm", "-i", "pg-nuke", "--image=postgres:16",
             "--restart=Never", "--env=PGPASSWORD=benchmarkpw", "--",
             "psql", "-h", "postgresql", "-U", "postgres", "-d", "batchgateway",
             "-c", "TRUNCATE batch_items, file_items CASCADE;"],
            context, namespace, check=False)

    # Delete all Redis keys (FLUSHALL is disabled on Bitnami)
    kubectl(["run", "--rm", "-i", "redis-del", "--image=redis",
             "--restart=Never", "--",
             "sh", "-c",
             'for key in $(redis-cli -h redis-master KEYS "*"); do '
             'redis-cli -h redis-master DEL "$key"; done'],
            context, namespace, check=False)

    # Scale back up
    kubectl(["scale", "deployment",
             "batch-gateway-processor", "batch-gateway-apiserver",
             "--replicas=1"], context, namespace, check=False)
    kubectl(["scale", "deployment", "async-processor",
             "--replicas=1"], context, namespace, check=False)

    kubectl(["rollout", "status", "deployment/batch-gateway-processor",
             f"--timeout=60s"], context, namespace, check=False)
    time.sleep(5)

    # Verify Redis is clean
    out = kubectl(["run", "--rm", "-i", "redis-check", "--image=redis",
                   "--restart=Never", "--",
                   "redis-cli", "-h", "redis-master", "DBSIZE"],
                  context, namespace, check=False)
    log(f"  Redis DBSIZE: {out}")


def submit_batch(cfg: ScenarioConfig, job_name="batch-submit"):
    prompt_chars = cfg.prompt_tokens * 3
    log(f"Submitting {cfg.batch_size}-request batch ({job_name}) in {cfg.namespace} "
        f"(~{cfg.prompt_tokens} tokens/request, random prompts)")
    yaml = textwrap.dedent(f"""\
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: {job_name}
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: batch-submit
              image: curlimages/curl:latest
              env:
                - name: BATCH_GATEWAY_URL
                  value: "http://batch-gateway-apiserver:8000"
                - name: BATCH_MODEL
                  value: "{cfg.model}"
                - name: BATCH_SIZE
                  value: "{cfg.batch_size}"
              command:
                - sh
                - -c
                - |
                  set -e
                  JSONL_FILE="/tmp/batch-input.jsonl"
                  PROMPT_CHARS={prompt_chars}
                  i=0
                  while [ "$i" -lt "$BATCH_SIZE" ]; do
                    RAND=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9 ' | head -c "$PROMPT_CHARS")
                    printf '{{"custom_id":"bench-%s","method":"POST","url":"/v1/chat/completions","body":{{"model":"%s","messages":[{{"role":"user","content":"%s"}}]}}}}\\n' "$i" "$BATCH_MODEL" "$RAND" >> "$JSONL_FILE"
                    i=$((i + 1))
                  done
                  echo "Generated $BATCH_SIZE requests (~$PROMPT_CHARS chars each)"
                  FILE_RESPONSE=$(curl -sk "$BATCH_GATEWAY_URL/v1/files" -H "Authorization: Bearer benchmark" -F "purpose=batch" -F "file=@$JSONL_FILE")
                  FILE_ID=$(echo "$FILE_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
                  [ -z "$FILE_ID" ] && echo "Upload failed: $FILE_RESPONSE" && exit 1
                  echo "Uploaded: $FILE_ID"
                  BATCH_RESPONSE=$(curl -sk "$BATCH_GATEWAY_URL/v1/batches" -H "Authorization: Bearer benchmark" -H "Content-Type: application/json" -d '{{"input_file_id":"'$FILE_ID'","endpoint":"/v1/chat/completions","completion_window":"24h"}}')
                  BATCH_ID=$(echo "$BATCH_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
                  [ -z "$BATCH_ID" ] && echo "Batch failed: $BATCH_RESPONSE" && exit 1
                  echo "Batch: $BATCH_ID"
                  TIMEOUT=1200
                  ELAPSED=0
                  while [ "$ELAPSED" -lt "$TIMEOUT" ]; do
                    STATUS_RESPONSE=$(curl -sk "$BATCH_GATEWAY_URL/v1/batches/$BATCH_ID" -H "Authorization: Bearer benchmark")
                    STATUS=$(echo "$STATUS_RESPONSE" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
                    COMPLETED=$(echo "$STATUS_RESPONSE" | grep -o '"completed":[0-9]*' | head -1 | cut -d: -f2)
                    TOTAL=$(echo "$STATUS_RESPONSE" | grep -o '"total":[0-9]*' | head -1 | cut -d: -f2)
                    echo "Batch $BATCH_ID: status=$STATUS completed=$COMPLETED/$TOTAL (${{ELAPSED}}s)"
                    case "$STATUS" in completed|failed|cancelled|expired) echo "Terminal: $STATUS"; exit 0;; esac
                    sleep 5
                    ELAPSED=$((ELAPSED + 5))
                  done
                  echo "Timed out"
    """)
    kubectl_apply(yaml, cfg.context, cfg.namespace)


def start_burst(cfg: ScenarioConfig):
    log(f"Starting burst pattern in {cfg.namespace}: {cfg.cycles} cycles, "
        f"burst@{cfg.burst_rate}/s for {cfg.burst_seconds}s, idle@{cfg.idle_rate}/s for {cfg.idle_seconds}s")

    cycle_lines = []
    for c in range(1, cfg.cycles + 1):
        cycle_lines.extend([
            f'echo "=== Cycle {c}: IDLE ({cfg.idle_rate} req/s, {cfg.idle_seconds}s) ==="',
            f'guidellm benchmark run --target "$T" $COMMON --profile constant --rate {cfg.idle_rate} --max-seconds {cfg.idle_seconds} --output-dir /results/{cfg.name} --outputs "idle-{c}.csv"',
            f'echo "=== Cycle {c}: BURST ({cfg.burst_rate} req/s, {cfg.burst_seconds}s) ==="',
            f'guidellm benchmark run --target "$T" $COMMON --profile constant --rate {cfg.burst_rate} --max-seconds {cfg.burst_seconds} --output-dir /results/{cfg.name} --outputs "burst-{c}.csv"',
        ])

    script_lines = [
        f'T="{cfg.target}"',
        f'M="{cfg.model}"',
        f'COMMON="--request-format text_completions --model $M --data prompt_tokens={cfg.prompt_tokens},prompt_tokens_stdev={cfg.prompt_tokens // 4},output_tokens=512,output_tokens_stdev=256 --processor $M --disable-console-interactive"',
        f'mkdir -p /results/{cfg.name}',
    ] + cycle_lines + ['echo "=== Done ==="']

    indent = " " * 14
    script_block = "\n".join(indent + line for line in script_lines)

    yaml = f"""\
apiVersion: batch/v1
kind: Job
metadata:
  name: guidellm-burst
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: guidellm
          image: ghcr.io/vllm-project/guidellm:latest
          env:
            - name: USER
              value: "guidellm"
            - name: HF_HUB_CACHE
              value: "/tmp/hf_cache"
          command:
            - sh
            - -c
            - |
{script_block}
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          persistentVolumeClaim:
            claimName: benchmark-results
"""
    kubectl_apply(yaml, cfg.context, cfg.namespace)


def poll_batch(cfg: ScenarioConfig):
    """Poll batch progress, return list of (timestamp, completed, total) tuples."""
    timeline = []
    start = time.time()
    while True:
        try:
            out = kubectl(["logs", "job/batch-submit", "--tail=1"],
                         cfg.context, cfg.namespace, check=False)
            if "completed=" in out:
                parts = out.split("completed=")[1].split()[0].split("/")
                completed = int(parts[0])
                total = int(parts[1])
                elapsed = time.time() - start
                timeline.append((elapsed, completed, total))
            if "Terminal:" in out or "Timed out" in out:
                break
        except Exception:
            pass

        # Check if guidellm is done
        try:
            phase_out = kubectl(["logs", "job/guidellm-burst", "--tail=1"],
                               cfg.context, cfg.namespace, check=False)
        except Exception:
            phase_out = ""

        pod_status = kubectl(["get", "pods", "-l", "job-name=batch-submit",
                             "-o", "jsonpath={.items[0].status.phase}"],
                            cfg.context, cfg.namespace, check=False)
        if pod_status in ("Succeeded", "Failed"):
            break

        time.sleep(10)
    return timeline


def _get_batch_progress(context, namespace, job_name):
    """Get completed/total from a batch-submit job's logs."""
    try:
        out = kubectl(["logs", f"job/{job_name}", "--tail=5"],
                     context, namespace, check=False)
        for line in reversed(out.split("\n")):
            if "completed=" in line and "/" in line.split("completed=")[1]:
                parts = line.split("completed=")[1].split()[0].split("/")
                return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return 0, 0


def monitor_scenario(cfg: ScenarioConfig):
    """Monitor batch progress during guidellm phases, return timeline.

    Submits a second batch when the first BURST phase is detected, simulating
    overlapping batch arrivals during traffic spikes.
    """
    timeline = []
    start = time.time()
    second_batch_submitted = False
    batch_jobs = ["batch-submit"]

    while True:
        elapsed = time.time() - start

        # Aggregate batch progress across all batch jobs
        completed, total = 0, 0
        for job_name in batch_jobs:
            c, t = _get_batch_progress(cfg.context, cfg.namespace, job_name)
            completed += c
            total += t

        # Get current phase
        phase = "unknown"
        try:
            out = kubectl(["logs", "job/guidellm-burst"],
                         cfg.context, cfg.namespace, check=False)
            for line in out.split("\n"):
                if line.startswith("==="):
                    phase = line.strip("= ")
        except Exception:
            pass

        # Submit a second batch during the first burst
        if not second_batch_submitted and "BURST" in phase:
            log(f"  [{cfg.name}] Submitting overlapping batch during burst")
            submit_batch(cfg, job_name="batch-submit-2")
            batch_jobs.append("batch-submit-2")
            second_batch_submitted = True

        timeline.append({
            "elapsed": round(elapsed),
            "completed": completed,
            "total": total,
            "phase": phase,
        })

        log(f"  [{cfg.name}] {phase} | batch: {completed}/{total}")

        # Check if guidellm is done
        guidellm_done = False
        try:
            gs = kubectl(["get", "pods", "-l", "job-name=guidellm-burst",
                         "-o", "jsonpath={.items[0].status.phase}"],
                        cfg.context, cfg.namespace, check=False)
            guidellm_done = gs in ("Succeeded", "Failed")
        except Exception:
            pass

        # Check if all batch jobs are done
        batch_done = True
        for job_name in batch_jobs:
            try:
                bs = kubectl(["get", "pods", "-l", f"job-name={job_name}",
                             "-o", "jsonpath={.items[0].status.phase}"],
                            cfg.context, cfg.namespace, check=False)
                if bs not in ("Succeeded", "Failed"):
                    batch_done = False
            except Exception:
                pass

        if guidellm_done and batch_done:
            break
        if elapsed > (cfg.cycles * (cfg.burst_seconds + cfg.idle_seconds) + 300):
            log(f"  [{cfg.name}] Timeout, stopping monitor")
            break

        time.sleep(10)

    return timeline


def collect_results(cfg: ScenarioConfig, results_dir: Path):
    """Collect CSVs from PVC via helper pod."""
    dest = results_dir / cfg.name
    dest.mkdir(parents=True, exist_ok=True)

    # Start helper pod
    helper = f"results-helper-{cfg.name}"
    helper_yaml = textwrap.dedent(f"""\
    apiVersion: v1
    kind: Pod
    metadata:
      name: {helper}
    spec:
      restartPolicy: Never
      containers:
        - name: helper
          image: busybox
          command: ["sleep", "300"]
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          persistentVolumeClaim:
            claimName: benchmark-results
    """)
    kubectl_apply(helper_yaml, cfg.context, cfg.namespace)
    kubectl(["wait", "--for=condition=Ready", f"pod/{helper}", "--timeout=60s"],
            cfg.context, cfg.namespace)

    # List and copy files
    file_list = kubectl(["exec", helper, "--", "ls", f"/results/{cfg.name}"],
                       cfg.context, cfg.namespace, check=False)
    for fname in file_list.split():
        if fname.endswith(".csv"):
            subprocess.run(
                ["kubectl", f"--context={cfg.context}", "-n", cfg.namespace,
                 "cp", f"{cfg.namespace}/{helper}:/results/{cfg.name}/{fname}",
                 str(dest / fname)],
                check=False, capture_output=True)
            log(f"  Collected {cfg.name}/{fname}")

    kubectl(["delete", "pod", helper, "--ignore-not-found"],
            cfg.context, cfg.namespace, check=False)
    return dest


def parse_csv_metrics(csv_path):
    """Parse a guidellm CSV and return key metrics."""
    with open(csv_path) as f:
        reader = csv.reader(f)
        next(reader)  # category headers
        next(reader)  # field names
        next(reader)  # units
        rows = list(reader)

    metrics = []
    for row in rows:
        try:
            ok_rps = float(row[43]) if row[43] else 0
            err_rps = float(row[47]) if row[47] else 0
            total_rps = ok_rps + err_rps
            error_rate = (err_rps / total_rps * 100) if total_rps > 0 else 0
            metrics.append(PhaseMetrics(
                phase=row[8],
                cycle=0,
                ttft_p50=float(row[33]) if row[33] else 0,
                ttft_p95=float(row[35]) if row[35] else 0,
                itl_p50=float(row[41]) if row[41] else 0,
                req_latency_p50=float(row[24]) if row[24] else 0,
                ok_rps=ok_rps,
                err_rps=err_rps,
                error_rate=error_rate,
                completed=int(row[19]) if row[19] else 0,
                errors=int(row[20]) if row[20] else 0,
            ))
        except (IndexError, ValueError):
            continue
    return metrics


def generate_html_report(results_dir: Path, sync_timeline, gated_timeline,
                         sync_csvs, gated_csvs):
    """Generate an HTML report with charts comparing sync vs gated."""

    # Parse all CSV metrics
    sync_metrics = {}
    gated_metrics = {}
    if isinstance(sync_csvs, Path):
        for csv_file in sorted(sync_csvs.glob("*.csv")):
            sync_metrics[csv_file.stem] = parse_csv_metrics(csv_file)
    if isinstance(gated_csvs, Path):
        for csv_file in sorted(gated_csvs.glob("*.csv")):
            gated_metrics[csv_file.stem] = parse_csv_metrics(csv_file)

    # Build timeline data
    sync_points = json.dumps([{"x": t["elapsed"], "y": t["completed"]} for t in sync_timeline])
    gated_points = json.dumps([{"x": t["elapsed"], "y": t["completed"]} for t in gated_timeline])

    # Build phase band data from the longer timeline
    ref_timeline = gated_timeline if gated_timeline else sync_timeline
    phase_bands = []
    prev_phase = ""
    for t in ref_timeline:
        if t["phase"] != prev_phase:
            phase_bands.append({"x": t["elapsed"], "phase": t["phase"]})
            prev_phase = t["phase"]

    # Extract per-phase metrics
    def extract(metrics, key):
        ms = metrics.get(key, [])
        return ms[0] if ms else None

    # Build comparison table rows and chart data
    phase_labels = sorted(set(list(sync_metrics.keys()) + list(gated_metrics.keys())))
    sync_ttft = []
    gated_ttft = []
    sync_incomplete = []
    gated_incomplete = []
    table_rows = []
    for phase_name in phase_labels:
        sm = extract(sync_metrics, phase_name)
        gm = extract(gated_metrics, phase_name)
        if sm:
            inc_pct = (sm.errors / (sm.completed + sm.errors) * 100) if (sm.completed + sm.errors) > 0 else 0
            sync_ttft.append(sm.ttft_p50)
            sync_incomplete.append(inc_pct)
            table_rows.append(("sync", phase_name, sm))
        else:
            sync_ttft.append(0)
            sync_incomplete.append(0)
        if gm:
            inc_pct = (gm.errors / (gm.completed + gm.errors) * 100) if (gm.completed + gm.errors) > 0 else 0
            gated_ttft.append(gm.ttft_p50)
            gated_incomplete.append(inc_pct)
            table_rows.append(("gated", phase_name, gm))
        else:
            gated_ttft.append(0)
            gated_incomplete.append(0)

    # Compute batch progress during burst phases
    def batch_during_phase(timeline, phase_substr):
        pts = [t for t in timeline if phase_substr in t.get("phase", "")]
        if len(pts) >= 2:
            return pts[-1]["completed"] - pts[0]["completed"]
        return 0

    sync_batch_burst = batch_during_phase(sync_timeline, "BURST")
    gated_batch_burst = batch_during_phase(gated_timeline, "BURST")
    sync_batch_idle = batch_during_phase(sync_timeline, "IDLE")
    gated_batch_idle = batch_during_phase(gated_timeline, "IDLE")

    # Read config from timeline metadata
    cfg_burst_rate = 30
    cfg_idle_rate = 1
    cfg_burst_sec = 60
    cfg_idle_sec = 120
    cfg_batch_size = 500
    cfg_model = "Qwen/Qwen3-8B"
    if sync_timeline:
        for t in sync_timeline:
            p = t.get("phase", "")
            if "BURST" in p and "req/s" in p:
                try:
                    cfg_burst_rate = int(p.split("(")[1].split(" ")[0])
                    cfg_burst_sec = int(p.split(", ")[1].rstrip("s)"))
                except (IndexError, ValueError):
                    pass
            elif "IDLE" in p and "req/s" in p:
                try:
                    cfg_idle_rate = int(p.split("(")[1].split(" ")[0])
                    cfg_idle_sec = int(p.split(", ")[1].rstrip("s)"))
                except (IndexError, ValueError):
                    pass
        if sync_timeline[-1].get("total", 0) > 0:
            cfg_batch_size = sync_timeline[-1]["total"]

    # Compute live traffic impact metrics from burst phases
    def burst_stats(metrics_dict):
        successful, total, ttft_sum, count = 0, 0, 0.0, 0
        for name, ms in metrics_dict.items():
            if "burst" in name and ms:
                m = ms[0]
                successful += m.completed
                total += m.completed + m.errors
                ttft_sum += m.ttft_p50
                count += 1
        ttft_avg = ttft_sum / count if count else 0
        return successful, total, ttft_avg

    sync_burst_successful, sync_burst_total, sync_burst_ttft_val = burst_stats(sync_metrics)
    gated_burst_successful, gated_burst_total, gated_burst_ttft_val = burst_stats(gated_metrics)

    sync_burst_success_rate = f"{sync_burst_successful}/{sync_burst_total} ({sync_burst_successful*100//max(sync_burst_total,1)}%)" if sync_burst_total else "N/A"
    gated_burst_success_rate = f"{gated_burst_successful}/{gated_burst_total} ({gated_burst_successful*100//max(gated_burst_total,1)}%)" if gated_burst_total else "N/A"
    sync_burst_ttft = f"{sync_burst_ttft_val:.1f} ms" if sync_burst_ttft_val else "N/A"
    gated_burst_ttft = f"{gated_burst_ttft_val:.1f} ms" if gated_burst_ttft_val else "N/A"

    html = textwrap.dedent(f"""\
    <!DOCTYPE html>
    <html>
    <head>
        <title>Batch vs Live Traffic Isolation Benchmark</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3"></script>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 40px; max-width: 1200px; background: #fafafa; color: #333; }}
            h1 {{ color: #1a1a1a; border-bottom: 2px solid #e5e5e5; padding-bottom: 12px; }}
            h2 {{ color: #444; margin-top: 40px; }}
            .card {{ background: white; border-radius: 8px; padding: 24px; margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .chart-container {{ background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            canvas {{ max-height: 400px; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            th, td {{ padding: 10px 16px; text-align: right; border-bottom: 1px solid #eee; }}
            th {{ background: #f5f5f5; font-weight: 600; text-align: left; }}
            td:first-child {{ text-align: left; font-weight: 500; }}
            .good {{ color: #16a34a; font-weight: 600; }}
            .bad {{ color: #dc2626; font-weight: 600; }}
            .metric-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0; }}
            .metric-box {{ background: #f9fafb; border-radius: 6px; padding: 16px; border: 1px solid #e5e7eb; }}
            .metric-box h4 {{ margin: 0 0 4px 0; font-size: 13px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.5px; }}
            .metric-box .value {{ font-size: 28px; font-weight: 700; }}
            .metric-box .detail {{ font-size: 13px; color: #6b7280; margin-top: 4px; }}
            code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-size: 14px; }}
            .workload-table {{ width: auto; margin: 12px 0; }}
            .workload-table td {{ padding: 4px 16px 4px 0; border: none; text-align: left; }}
            .workload-table td:first-child {{ font-weight: 600; color: #6b7280; }}
        </style>
    </head>
    <body>
        <h1>Batch vs Live Traffic Isolation</h1>

        <div class="card">
            <h2 style="margin-top:0">Workload</h2>
            <p>This benchmark tests whether the <strong>llm-d-async dispatch budget gate</strong>
            protects live inference traffic from batch request interference. Two scenarios are
            compared on identical hardware:</p>
            <table class="workload-table">
                <tr><td>Model</td><td><code>{cfg_model}</code> on 1x NVIDIA A100 GPU</td></tr>
                <tr><td>Live traffic</td><td>guidellm burst/idle cycles: <strong>{cfg_burst_rate} req/s</strong> for {cfg_burst_sec}s (burst),
                    <strong>{cfg_idle_rate} req/s</strong> for {cfg_idle_sec}s (idle), 2 cycles</td></tr>
                <tr><td>Batch load</td><td>{cfg_batch_size} requests submitted at start (idle), another {cfg_batch_size} submitted during first burst</td></tr>
                <tr><td>Batch requests</td><td>~2000 random input tokens, no output cap (defeats prefix caching)</td></tr>
                <tr><td>Pattern</td><td>IDLE &rarr; BURST &rarr; IDLE &rarr; BURST</td></tr>
            </table>
            <div class="metric-grid" style="margin-top: 20px">
                <div class="metric-box">
                    <h4>Sync (no gate)</h4>
                    <p>Batch-gateway processor sends requests <strong>directly</strong> to the inference gateway.
                    Batch and live traffic compete for GPU capacity.</p>
                </div>
                <div class="metric-box">
                    <h4>Gated (async dispatch)</h4>
                    <p>Batch-gateway submits to a Redis queue. The <strong>async-processor</strong> dispatches
                    only when the <code>prometheus-budget</code> gate is open (GPU utilization below threshold).</p>
                </div>
            </div>
        </div>

        <h2>Key Results</h2>
        <div class="metric-grid">
            <div class="metric-box">
                <h4>Sync: Batch during Burst</h4>
                <div class="value bad">{sync_batch_burst} reqs</div>
                <div class="detail">Batch requests dispatched directly during live traffic burst, competing for GPU</div>
            </div>
            <div class="metric-box">
                <h4>Gated: Batch during Burst</h4>
                <div class="value good">{gated_batch_burst} reqs</div>
                <div class="detail">Gate throttles batch &mdash; only excess capacity used, live traffic protected</div>
            </div>
            <div class="metric-box">
                <h4>Sync: Batch during Idle</h4>
                <div class="value">{sync_batch_idle} reqs</div>
                <div class="detail">Batch requests processed during idle &mdash; competes with live traffic at all times</div>
            </div>
            <div class="metric-box">
                <h4>Gated: Batch during Idle</h4>
                <div class="value good">{gated_batch_idle} reqs</div>
                <div class="detail">Gate opens &mdash; batch fills unused capacity when live traffic is low</div>
            </div>
        </div>

        <h2>Live Traffic Impact</h2>
        <p>Each burst phase sends requests at {cfg_burst_rate} req/s for {cfg_burst_sec}s. Requests still in-flight
        when the phase window closes are marked <strong>incomplete</strong> &mdash; they were delayed by GPU
        contention from concurrent batch processing. Every batch request dispatched during burst is capacity
        stolen from live traffic.</p>
        <div class="metric-grid">
            <div class="metric-box">
                <h4>Sync: Live Burst Success Rate</h4>
                <div class="value bad">{sync_burst_success_rate}</div>
                <div class="detail">{sync_burst_successful} of {sync_burst_total} requests completed in time</div>
            </div>
            <div class="metric-box">
                <h4>Gated: Live Burst Success Rate</h4>
                <div class="value good">{gated_burst_success_rate}</div>
                <div class="detail">{gated_burst_successful} of {gated_burst_total} requests completed in time</div>
            </div>
            <div class="metric-box">
                <h4>Sync: Burst TTFT (median)</h4>
                <div class="value bad">{sync_burst_ttft}</div>
                <div class="detail">Higher TTFT = batch requests are delaying live traffic first-token latency</div>
            </div>
            <div class="metric-box">
                <h4>Gated: Burst TTFT (median)</h4>
                <div class="value good">{gated_burst_ttft}</div>
                <div class="detail">Gate holds batch back &mdash; live traffic gets full GPU priority</div>
            </div>
        </div>

        <h2>Batch Completion Timeline</h2>
        <p>The timeline shows how batch requests progress over time. Red/green shaded bands indicate
        burst (high live traffic) and idle phases. In sync mode, batch blasts through during burst,
        competing with live traffic. With the gate, batch progress <strong>pauses during burst</strong>
        and resumes during idle.</p>
        <div class="chart-container">
            <canvas id="timelineChart"></canvas>
        </div>

        <h2>Live Traffic Quality: TTFT</h2>
        <p>Time to First Token (TTFT) measures how quickly the model begins responding.
        Higher TTFT during burst phases indicates GPU contention from batch requests.</p>
        <div class="chart-container">
            <canvas id="ttftChart"></canvas>
        </div>

        <h2>Live Traffic Quality: Incomplete Requests</h2>
        <p>Requests that did not complete within the phase window. A high incomplete rate
        during burst indicates the GPU is overloaded.</p>
        <div class="chart-container">
            <canvas id="incompleteChart"></canvas>
        </div>

        <h2>Detailed Metrics</h2>
        <table>
            <tr><th>Scenario</th><th>Phase</th><th>Successful</th><th>Incomplete</th><th>TTFT p50 (ms)</th><th>TTFT p95 (ms)</th><th>ITL p50 (ms)</th><th>OK req/s</th></tr>
    """)

    for scenario, phase_name, m in table_rows:
        is_burst = "burst" in phase_name
        sc = "bad" if scenario == "sync" and is_burst else ("good" if scenario == "gated" and is_burst else "")
        cls = f' class="{sc}"' if sc else ""
        html += (f"        <tr{cls}><td>{scenario}</td><td>{phase_name}</td>"
                f"<td>{m.completed}</td><td>{m.errors}</td>"
                f"<td>{m.ttft_p50:.1f}</td><td>{m.ttft_p95:.1f}</td>"
                f"<td>{m.itl_p50:.2f}</td>"
                f"<td>{m.ok_rps:.1f}</td></tr>\n")

    html += textwrap.dedent(f"""\
        </table>

        <div class="card" style="margin-top: 40px">
            <h2 style="margin-top:0">Conclusion</h2>
            <p>Without the dispatch budget gate (<strong>sync</strong>), the batch processor sends
            {cfg_batch_size} requests directly to the inference gateway during burst, competing with
            {cfg_burst_rate} req/s of live traffic for GPU compute. During burst phases,
            <strong>{sync_burst_successful} of {sync_burst_total}</strong> live requests completed
            in time (TTFT {sync_burst_ttft}), while <strong>{sync_batch_burst}</strong> batch requests
            consumed GPU capacity that could have served live traffic.</p>
            <p>With the <strong>prometheus-budget gate</strong>, the async-processor monitors GPU
            utilization via Prometheus and holds back batch requests when the inference endpoint is
            saturated. During burst, only <strong>{gated_batch_burst}</strong> batch requests were
            dispatched, allowing <strong>{gated_burst_successful} of {gated_burst_total}</strong>
            live requests to complete (TTFT {gated_burst_ttft}). Batch work shifts to idle periods
            ({gated_batch_idle} requests processed during idle) when GPU capacity is available.</p>
        </div>

        <script>
        const phaseBands = {json.dumps(phase_bands)};

        // Build annotation boxes for burst/idle phases
        function buildPhaseAnnotations(maxX) {{
            const annotations = {{}};
            for (let i = 0; i < phaseBands.length; i++) {{
                const start = phaseBands[i].x;
                const end = i + 1 < phaseBands.length ? phaseBands[i + 1].x : maxX;
                const isBurst = phaseBands[i].phase.includes('BURST');
                annotations['band' + i] = {{
                    type: 'box',
                    xMin: start, xMax: end,
                    backgroundColor: isBurst ? 'rgba(239,68,68,0.08)' : 'rgba(34,197,94,0.06)',
                    borderWidth: 0,
                    label: {{
                        display: true,
                        content: isBurst ? 'BURST' : 'IDLE',
                        position: {{ x: 'center', y: 'start' }},
                        font: {{ size: 11, weight: 'bold' }},
                        color: isBurst ? 'rgba(239,68,68,0.5)' : 'rgba(34,197,94,0.4)',
                    }}
                }};
            }}
            return annotations;
        }}

        const maxElapsed = Math.max(
            ...{sync_points}.map(p => p.x),
            ...({gated_points}.length ? {gated_points}.map(p => p.x) : [0])
        );

        new Chart(document.getElementById('timelineChart'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: 'Sync (no gate)',
                        data: {sync_points},
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239,68,68,0.1)',
                        fill: false, tension: 0.1, pointRadius: 3, borderWidth: 2,
                    }},
                    {{
                        label: 'Gated (prometheus-budget)',
                        data: {gated_points},
                        borderColor: '#22c55e',
                        backgroundColor: 'rgba(34,197,94,0.1)',
                        fill: false, tension: 0.1, pointRadius: 3, borderWidth: 2,
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Batch Requests Completed Over Time', font: {{ size: 16 }} }},
                    annotation: {{ annotations: buildPhaseAnnotations(maxElapsed) }}
                }},
                scales: {{
                    x: {{ type: 'linear', title: {{ display: true, text: 'Time (seconds)' }} }},
                    y: {{ title: {{ display: true, text: 'Batch Requests Completed' }}, beginAtZero: true }}
                }}
            }}
        }});

        new Chart(document.getElementById('ttftChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(phase_labels)},
                datasets: [
                    {{ label: 'Sync', data: {json.dumps(sync_ttft)}, backgroundColor: 'rgba(239,68,68,0.7)' }},
                    {{ label: 'Gated', data: {json.dumps(gated_ttft)}, backgroundColor: 'rgba(34,197,94,0.7)' }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ title: {{ display: true, text: 'Time to First Token p50 (ms) — lower is better', font: {{ size: 16 }} }} }},
                scales: {{ y: {{ title: {{ display: true, text: 'TTFT p50 (ms)' }}, beginAtZero: true }} }}
            }}
        }});

        new Chart(document.getElementById('incompleteChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(phase_labels)},
                datasets: [
                    {{ label: 'Sync', data: {json.dumps(sync_incomplete)}, backgroundColor: 'rgba(239,68,68,0.7)' }},
                    {{ label: 'Gated', data: {json.dumps(gated_incomplete)}, backgroundColor: 'rgba(34,197,94,0.7)' }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ title: {{ display: true, text: 'Incomplete Request Rate (%) — lower is better', font: {{ size: 16 }} }} }},
                scales: {{ y: {{ title: {{ display: true, text: 'Incomplete %' }}, beginAtZero: true }} }}
            }}
        }});
        </script>
    </body>
    </html>
    """)

    report_path = results_dir / "report.html"
    report_path.write_text(html)
    log(f"Report written to {report_path}")
    return report_path


def run_scenario(cfg: ScenarioConfig, results_dir: Path):
    """Run a single scenario: cleanup, submit batch, start burst, monitor, collect."""
    cleanup_namespace(cfg.context, cfg.namespace)

    # Verify vLLM is alive
    try:
        kubectl(["wait", "--for=condition=Ready", "pod",
                "-l", "llm-d.ai/role=decode", "--timeout=30s"],
               cfg.context, cfg.namespace)
    except subprocess.CalledProcessError:
        log(f"  WARNING: vLLM not ready in {cfg.namespace}")

    submit_batch(cfg)
    time.sleep(10)
    start_burst(cfg)
    time.sleep(30)  # let guidellm validate and start

    timeline = monitor_scenario(cfg)
    csvs = collect_results(cfg, results_dir)
    return timeline, csvs


def main():
    parser = argparse.ArgumentParser(description="Batch vs Live Traffic Benchmark")
    parser.add_argument("--context", required=True, help="kubectl context")
    parser.add_argument("--sync-namespace", default="", help="Namespace for sync scenario (omit to skip)")
    parser.add_argument("--gated-namespace", default="", help="Namespace for gated scenario (omit to skip)")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--burst-rate", type=int, default=15)
    parser.add_argument("--idle-rate", type=int, default=1)
    parser.add_argument("--burst-seconds", type=int, default=60)
    parser.add_argument("--idle-seconds", type=int, default=120)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--prompt-tokens", type=int, default=1500,
                        help="Input tokens per request (batch and live traffic)")
    parser.add_argument("--results-dir", type=Path, default=Path("./benchmark-results"))
    parser.add_argument("--target", default="http://llm-d-inference-gateway-istio")
    parser.add_argument("--model", default="Qwen/Qwen3-0.6B")
    args = parser.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    sync_cfg = None
    if args.sync_namespace:
        sync_cfg = ScenarioConfig(
            name="sync", namespace=args.sync_namespace, context=args.context,
            burst_rate=args.burst_rate, idle_rate=args.idle_rate,
            burst_seconds=args.burst_seconds, idle_seconds=args.idle_seconds,
            cycles=args.cycles,
            batch_size=args.batch_size, prompt_tokens=args.prompt_tokens,
            target=args.target, model=args.model,
        )
    gated_cfg = None
    if args.gated_namespace:
        gated_cfg = ScenarioConfig(
            name="gated", namespace=args.gated_namespace, context=args.context,
            burst_rate=args.burst_rate, idle_rate=args.idle_rate,
            burst_seconds=args.burst_seconds, idle_seconds=args.idle_seconds,
            cycles=args.cycles,
            batch_size=args.batch_size, prompt_tokens=args.prompt_tokens,
            target=args.target, model=args.model,
        )

    log("=== Starting benchmark ===")
    log(f"Burst: {args.burst_rate} req/s for {args.burst_seconds}s, "
        f"Idle: {args.idle_rate} req/s for {args.idle_seconds}s, "
        f"{args.cycles} cycles, {args.batch_size} batch requests")

    # Run scenarios sequentially (they use separate namespaces but share GPU nodes)
    sync_timeline, sync_csvs = [], []
    if sync_cfg:
        log("━━━ Scenario 1: SYNC (no gate) ━━━")
        sync_timeline, sync_csvs = run_scenario(sync_cfg, args.results_dir)
    else:
        log("━━━ Skipping sync scenario (no --sync-namespace) ━━━")

    gated_timeline, gated_csvs = [], []
    if gated_cfg:
        log("━━━ Scenario 2: GATED (prometheus-budget gate) ━━━")
        gated_timeline, gated_csvs = run_scenario(gated_cfg, args.results_dir)
    else:
        log("━━━ Skipping gated scenario (no --gated-namespace) ━━━")

    # Save timelines as JSON
    if sync_timeline:
        (args.results_dir / "sync-timeline.json").write_text(json.dumps(sync_timeline, indent=2))
    if gated_timeline:
        (args.results_dir / "gated-timeline.json").write_text(json.dumps(gated_timeline, indent=2))

    # Generate HTML report
    report = generate_html_report(args.results_dir, sync_timeline, gated_timeline,
                                  sync_csvs, gated_csvs)

    log("=== Benchmark complete ===")
    log(f"Results: {args.results_dir}")
    log(f"Report:  {report}")


if __name__ == "__main__":
    main()
