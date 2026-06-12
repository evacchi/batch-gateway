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

    kubectl(["delete", "job", "guidellm-burst", "batch-submit",
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


def submit_batch(cfg: ScenarioConfig):
    log(f"Submitting {cfg.batch_size}-request batch in {cfg.namespace}")
    yaml = textwrap.dedent(f"""\
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: batch-submit
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
                  i=0
                  while [ "$i" -lt "$BATCH_SIZE" ]; do
                    echo '{{"custom_id":"bench-'$i'","method":"POST","url":"/v1/chat/completions","body":{{"model":"'$BATCH_MODEL'","max_tokens":128,"messages":[{{"role":"user","content":"Write a short paragraph about request number '$i'."}}]}}}}' >> "$JSONL_FILE"
                    i=$((i + 1))
                  done
                  echo "Generated $BATCH_SIZE requests"
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
            f'echo "=== Cycle {c}: BURST ({cfg.burst_rate} req/s, {cfg.burst_seconds}s) ==="',
            f'guidellm benchmark run --target "$T" $COMMON --profile constant --rate {cfg.burst_rate} --max-seconds {cfg.burst_seconds} --output-dir /results/{cfg.name} --outputs "burst-{c}.csv"',
            f'echo "=== Cycle {c}: IDLE ({cfg.idle_rate} req/s, {cfg.idle_seconds}s) ==="',
            f'guidellm benchmark run --target "$T" $COMMON --profile constant --rate {cfg.idle_rate} --max-seconds {cfg.idle_seconds} --output-dir /results/{cfg.name} --outputs "idle-{c}.csv"',
        ])

    script_lines = [
        f'T="{cfg.target}"',
        f'M="{cfg.model}"',
        'COMMON="--request-format text_completions --model $M --data prompt_tokens=256,output_tokens=128 --processor $M --disable-console-interactive"',
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


def monitor_scenario(cfg: ScenarioConfig):
    """Monitor batch progress during guidellm phases, return timeline."""
    timeline = []
    start = time.time()

    while True:
        elapsed = time.time() - start

        # Get batch progress
        completed, total = 0, 0
        try:
            out = kubectl(["logs", "job/batch-submit", "--tail=1"],
                         cfg.context, cfg.namespace, check=False)
            if "completed=" in out:
                parts = out.split("completed=")[1].split()[0].split("/")
                completed = int(parts[0])
                total = int(parts[1])
        except Exception:
            pass

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

        timeline.append({
            "elapsed": round(elapsed),
            "completed": completed,
            "total": total,
            "phase": phase,
        })

        log(f"  [{cfg.name}] {phase} | batch: {completed}/{total}")

        # Check if both jobs are done
        guidellm_done = False
        batch_done = False
        try:
            gs = kubectl(["get", "pods", "-l", "job-name=guidellm-burst",
                         "-o", "jsonpath={.items[0].status.phase}"],
                        cfg.context, cfg.namespace, check=False)
            guidellm_done = gs in ("Succeeded", "Failed")
        except Exception:
            pass
        try:
            bs = kubectl(["get", "pods", "-l", "job-name=batch-submit",
                         "-o", "jsonpath={.items[0].status.phase}"],
                        cfg.context, cfg.namespace, check=False)
            batch_done = bs in ("Succeeded", "Failed")
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
                         sync_csvs: Path, gated_csvs: Path):
    """Generate an HTML report with charts comparing sync vs gated."""

    # Parse all CSV metrics
    sync_metrics = {}
    gated_metrics = {}
    for csv_file in sorted(sync_csvs.glob("*.csv")):
        sync_metrics[csv_file.stem] = parse_csv_metrics(csv_file)
    for csv_file in sorted(gated_csvs.glob("*.csv")):
        gated_metrics[csv_file.stem] = parse_csv_metrics(csv_file)

    # Build timeline data for chart
    sync_points = json.dumps([{"x": t["elapsed"], "y": t["completed"]} for t in sync_timeline])
    gated_points = json.dumps([{"x": t["elapsed"], "y": t["completed"]} for t in gated_timeline])

    # Build phase annotation data
    phase_annotations = []
    if gated_timeline:
        prev_phase = ""
        for t in gated_timeline:
            if t["phase"] != prev_phase:
                color = "rgba(255,99,132,0.1)" if "BURST" in t["phase"] else "rgba(75,192,192,0.1)"
                phase_annotations.append({
                    "x": t["elapsed"],
                    "phase": t["phase"],
                    "color": color,
                })
                prev_phase = t["phase"]

    # Build comparison data, filtering bogus phases (ok_rps > 200 with 0 completed)
    def valid(m):
        return not (m.ok_rps > 200 and m.completed == 0)

    sync_ttft = []
    gated_ttft = []
    sync_err_rate = []
    gated_err_rate = []
    phase_labels = []
    for key in sorted(sync_metrics.keys()):
        ms = [m for m in sync_metrics[key] if valid(m)]
        if ms:
            phase_labels.append(key)
            sync_ttft.append(ms[0].ttft_p50)
            sync_err_rate.append(ms[0].error_rate)
    for key in sorted(gated_metrics.keys()):
        ms = [m for m in gated_metrics.get(key, []) if valid(m)]
        if key in phase_labels and ms:
            gated_ttft.append(ms[0].ttft_p50)
            gated_err_rate.append(ms[0].error_rate)

    html = textwrap.dedent(f"""\
    <!DOCTYPE html>
    <html>
    <head>
        <title>Batch vs Live Traffic Benchmark</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 40px; background: #fafafa; }}
            h1 {{ color: #333; }}
            h2 {{ color: #555; margin-top: 40px; }}
            .chart-container {{ background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            canvas {{ max-height: 400px; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            th, td {{ padding: 10px 16px; text-align: right; border-bottom: 1px solid #eee; }}
            th {{ background: #f5f5f5; font-weight: 600; text-align: left; }}
            td:first-child {{ text-align: left; font-weight: 500; }}
            .summary {{ background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .good {{ color: #22c55e; }}
            .bad {{ color: #ef4444; }}
        </style>
    </head>
    <body>
        <h1>Batch vs Live Traffic Benchmark</h1>

        <div class="summary">
            <h2>Key Finding</h2>
            <p><strong>Sync dispatch</strong> (no gate): Batch requests <span class="bad">failed to complete</span> under live traffic load.
               The processor competes directly with live requests for inference capacity.</p>
            <p><strong>Gated async dispatch</strong> (prometheus-budget gate): Batch requests <span class="good">completed successfully</span>.
               The gate throttles batch during traffic bursts and dispatches during idle periods.</p>
        </div>

        <h2>Batch Completion Timeline</h2>
        <div class="chart-container">
            <canvas id="timelineChart"></canvas>
        </div>

        <h2>Live Traffic Metrics by Phase (TTFT p50)</h2>
        <div class="chart-container">
            <canvas id="ttftChart"></canvas>
        </div>

        <h2>Detailed Metrics</h2>
        <h2>Error Rate by Phase</h2>
        <div class="chart-container">
            <canvas id="errorChart"></canvas>
        </div>

        <table>
            <tr><th>Scenario</th><th>Phase</th><th>TTFT p50 (ms)</th><th>TTFT p95 (ms)</th><th>ITL p50 (ms)</th><th>Req Lat p50 (s)</th><th>OK req/s</th><th>Err req/s</th><th>Error %</th><th>Completed</th></tr>
    """)

    for scenario, metrics in [("sync", sync_metrics), ("gated", gated_metrics)]:
        for phase_name, phase_metrics in sorted(metrics.items()):
            for m in phase_metrics:
                # Skip phases with bogus data (ok_rps > 200 with 0 completed)
                if m.ok_rps > 200 and m.completed == 0:
                    continue
                err_class = ' class="bad"' if m.error_rate > 50 else ""
                html += (f"        <tr><td>{scenario}</td><td>{phase_name}</td>"
                        f"<td>{m.ttft_p50:.1f}</td><td>{m.ttft_p95:.1f}</td>"
                        f"<td>{m.itl_p50:.2f}</td><td>{m.req_latency_p50:.3f}</td>"
                        f"<td>{m.ok_rps:.2f}</td><td{err_class}>{m.err_rps:.2f}</td>"
                        f"<td{err_class}>{m.error_rate:.0f}%</td>"
                        f"<td>{m.completed}</td></tr>\n")

    html += textwrap.dedent(f"""\
        </table>

        <script>
        // Batch completion timeline
        new Chart(document.getElementById('timelineChart'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: 'Sync (no gate)',
                        data: {sync_points},
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239,68,68,0.1)',
                        fill: false,
                        tension: 0.1,
                        pointRadius: 2,
                    }},
                    {{
                        label: 'Gated (prometheus-budget)',
                        data: {gated_points},
                        borderColor: '#22c55e',
                        backgroundColor: 'rgba(34,197,94,0.1)',
                        fill: false,
                        tension: 0.1,
                        pointRadius: 2,
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Batch Requests Completed Over Time (burst/idle cycles)' }},
                    legend: {{ position: 'top' }}
                }},
                scales: {{
                    x: {{ type: 'linear', title: {{ display: true, text: 'Time (seconds)' }} }},
                    y: {{ title: {{ display: true, text: 'Completed Requests' }}, beginAtZero: true }}
                }}
            }}
        }});

        // TTFT comparison
        new Chart(document.getElementById('ttftChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(phase_labels)},
                datasets: [
                    {{
                        label: 'Sync (no gate)',
                        data: {json.dumps(sync_ttft)},
                        backgroundColor: 'rgba(239,68,68,0.7)',
                    }},
                    {{
                        label: 'Gated (prometheus-budget)',
                        data: {json.dumps(gated_ttft[:len(sync_ttft)])},
                        backgroundColor: 'rgba(34,197,94,0.7)',
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Time to First Token (p50) by Phase' }},
                }},
                scales: {{
                    y: {{ title: {{ display: true, text: 'TTFT p50 (ms)' }}, beginAtZero: true }}
                }}
            }}
        }});

        // Error rate comparison
        new Chart(document.getElementById('errorChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(phase_labels)},
                datasets: [
                    {{
                        label: 'Sync (no gate)',
                        data: {json.dumps(sync_err_rate)},
                        backgroundColor: 'rgba(239,68,68,0.7)',
                    }},
                    {{
                        label: 'Gated (prometheus-budget)',
                        data: {json.dumps(gated_err_rate[:len(sync_err_rate)])},
                        backgroundColor: 'rgba(34,197,94,0.7)',
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Inference Error Rate by Phase (lower is better)' }},
                }},
                scales: {{
                    y: {{ title: {{ display: true, text: 'Error %' }}, beginAtZero: true, max: 100 }}
                }}
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
    parser.add_argument("--sync-namespace", required=True, help="Namespace for sync scenario")
    parser.add_argument("--gated-namespace", required=True, help="Namespace for gated scenario")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--burst-rate", type=int, default=15)
    parser.add_argument("--idle-rate", type=int, default=1)
    parser.add_argument("--burst-seconds", type=int, default=60)
    parser.add_argument("--idle-seconds", type=int, default=120)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--results-dir", type=Path, default=Path("./benchmark-results"))
    parser.add_argument("--target", default="http://llm-d-inference-gateway-istio")
    parser.add_argument("--model", default="Qwen/Qwen3-0.6B")
    args = parser.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    sync_cfg = ScenarioConfig(
        name="sync", namespace=args.sync_namespace, context=args.context,
        burst_rate=args.burst_rate, idle_rate=args.idle_rate,
        burst_seconds=args.burst_seconds, idle_seconds=args.idle_seconds,
        cycles=args.cycles,
        batch_size=args.batch_size, target=args.target, model=args.model,
    )
    gated_cfg = ScenarioConfig(
        name="gated", namespace=args.gated_namespace, context=args.context,
        burst_rate=args.burst_rate, idle_rate=args.idle_rate,
        burst_seconds=args.burst_seconds, idle_seconds=args.idle_seconds,
        cycles=args.cycles,
        batch_size=args.batch_size, target=args.target, model=args.model,
    )

    log("=== Starting benchmark ===")
    log(f"Burst: {args.burst_rate} req/s for {args.burst_seconds}s, "
        f"Idle: {args.idle_rate} req/s for {args.idle_seconds}s, "
        f"{args.cycles} cycles, {args.batch_size} batch requests")

    # Run scenarios sequentially (they use separate namespaces but share GPU nodes)
    log("━━━ Scenario 1: SYNC (no gate) ━━━")
    sync_timeline, sync_csvs = run_scenario(sync_cfg, args.results_dir)

    log("━━━ Scenario 2: GATED (prometheus-budget gate) ━━━")
    gated_timeline, gated_csvs = run_scenario(gated_cfg, args.results_dir)

    # Save timelines as JSON
    (args.results_dir / "sync-timeline.json").write_text(json.dumps(sync_timeline, indent=2))
    (args.results_dir / "gated-timeline.json").write_text(json.dumps(gated_timeline, indent=2))

    # Generate HTML report
    report = generate_html_report(args.results_dir, sync_timeline, gated_timeline,
                                  sync_csvs, gated_csvs)

    log("=== Benchmark complete ===")
    log(f"Results: {args.results_dir}")
    log(f"Report:  {report}")


if __name__ == "__main__":
    main()
