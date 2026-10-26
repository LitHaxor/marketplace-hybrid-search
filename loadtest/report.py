"""Builds loadtest/reports/<run>/report.md from k6's results and Prometheus.

Client side (k6 JSON): achieved rate, latency percentiles, errors, dropped iterations, targets met.
Server side (Prometheus, over exactly each stage's time window): CPU, GC, heap, threads, DB pool,
search time per stage, Elasticsearch load and rejections, ingest-worker lag and freshness.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

PROMETHEUS = "http://localhost:9090"


def prom(query: str, at: int):
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": at})
    with urllib.request.urlopen(url, timeout=30) as response:
        result = json.load(response)["data"]["result"]
    return {tuple(sorted((k, v) for k, v in r["metric"].items() if k != "__name__")): float(r["value"][1]) for r in result}


def one(query: str, at: int):
    values = [v for v in prom(query, at).values() if v == v]  # drop NaN
    return max(values) if values else None


def fmt(value, unit: str = "", digits: int = 0):
    if value is None:
        return "–"
    if unit == "ms":
        return f"{value * 1000:,.{digits}f} ms"
    if unit == "%":
        return f"{value * 100:.{digits}f}%"
    if unit == "MB":
        return f"{value / 1048576:,.0f} MB"
    return f"{value:,.{digits}f}{unit}"


def client_side(stage_json: dict, scenario: str):
    m = stage_json["metrics"]
    duration = m.get(f"http_req_duration{{scenario:{scenario}}}", {}).get("values", {})
    failed = m.get(f"http_req_failed{{scenario:{scenario}}}", {}).get("values", {})
    dropped = m.get(f"dropped_iterations{{scenario:{scenario}}}", {}).get("values", {}).get("count", 0)
    targets = {}
    for name in (f"http_req_duration{{scenario:{scenario}}}", f"http_req_failed{{scenario:{scenario}}}"):
        for rule, outcome in m.get(name, {}).get("thresholds", {}).items():
            targets[rule] = outcome.get("ok")
    return {
        "count": duration.get("count", 0),
        "p50": duration.get("med"),
        "p95": duration.get("p(95)"),
        "p99": duration.get("p(99)"),
        "max": duration.get("max"),
        "failed": failed.get("rate"),
        "dropped": dropped,
        "targets": targets,
    }


def server_side(start: int, end: int):
    w = f"{end - start}s"
    s = {}
    s["api_cpu_avg"] = one(f'avg_over_time(process_cpu_usage{{app="api"}}[{w}])', end)
    s["api_cpu_max"] = one(f'max_over_time(process_cpu_usage{{app="api"}}[{w}])', end)
    s["api_heap_max"] = one(f'max_over_time(sum(jvm_memory_used_bytes{{app="api",area="heap"}})[{w}:5s])', end)
    s["api_gc_pauses"] = one(f'sum(increase(jvm_gc_pause_seconds_count{{app="api"}}[{w}]))', end)
    s["api_gc_max"] = one(f'max(max_over_time(jvm_gc_pause_seconds_max{{app="api"}}[{w}]))', end)
    s["api_gc_share"] = one(f'sum(increase(jvm_gc_pause_seconds_sum{{app="api"}}[{w}])) / {end - start}', end)
    s["api_alloc_rate"] = one(f'sum(rate(jvm_gc_memory_allocated_bytes_total{{app="api"}}[{w}]))', end)
    s["api_threads_max"] = one(f'max_over_time(jvm_threads_live_threads{{app="api"}}[{w}])', end)
    s["db_pending_max"] = one(f'max_over_time(hikaricp_connections_pending{{app="api"}}[{w}])', end)
    s["http_search_p95"] = one(f'histogram_quantile(0.95, sum by (le) (increase(http_server_requests_seconds_bucket{{app="api",uri="/search"}}[{w}])))', end)
    s["stages_p95"] = {
        dict(k).get("stage"): v
        for k, v in prom(f'histogram_quantile(0.95, sum by (le, stage) (increase(search_stage_seconds_bucket{{app="api"}}[{w}])))', end).items()
        if v == v
    }
    s["es_cpu_avg"] = one(f'avg_over_time(elasticsearch_process_cpu_percent[{w}]) / 100', end)
    s["es_heap_max"] = one(f'max_over_time(elasticsearch_jvm_memory_used_bytes{{area="heap"}}[{w}])', end)
    s["es_gc_share"] = one(f'sum(increase(elasticsearch_jvm_gc_collection_seconds_sum[{w}])) / {end - start}', end)
    s["es_rejected"] = one(f'sum(increase(elasticsearch_thread_pool_rejected_count{{type=~"search|write"}}[{w}]))', end)
    s["worker_cpu_avg"] = one(f'avg_over_time(process_cpu_usage{{app="ingest-worker"}}[{w}])', end)
    s["worker_lag_max"] = one(f'max_over_time(sum(kafka_consumer_fetch_manager_records_lag{{topic="vehicle.changes"}})[{w}:5s])', end)
    s["freshness_p95"] = one(f'histogram_quantile(0.95, sum by (le) (increase(ingest_freshness_seconds_bucket{{lane="changes"}}[{w}])))', end)
    return s


def main(run_dir: str):
    directory = Path(run_dir)
    rows = [line.split("\t") for line in (directory / "stages.tsv").read_text().splitlines() if line]
    stages = []
    for name, searches, ingests, start, end in rows:
        start, end = int(start), int(end)
        k6 = json.loads((directory / f"{name}.json").read_text())
        seconds = end - start
        stages.append({
            "name": name, "searches": int(searches), "ingests": int(ingests), "seconds": seconds,
            "search": client_side(k6, "search"), "ingest": client_side(k6, "ingest"),
            "server": server_side(start, end), "start": start, "end": end,
        })

    out = []
    out.append(f"# Load test report: {directory.name}\n")
    out.append(f"Run {datetime.fromtimestamp(stages[0]['start']):%Y-%m-%d %H:%M} – {datetime.fromtimestamp(stages[-1]['end']):%H:%M}. "
               "One laptop: OrbStack Kubernetes (single node) running the API, ingest worker, Postgres, Kafka, "
               "Debezium, Elasticsearch, Qdrant and the monitoring stack; k6 on the same Mac.\n")
    out.append("Search = `GET /search` (hybrid: Elasticsearch BM25 + Qdrant vectors + RRF, query embedded with "
               "all-MiniLM-L6-v2 in the API). Ingest = `POST /vehicles` (vehicle + outbox → Debezium → Kafka "
               "`vehicle.changes` → worker → Elasticsearch + Qdrant).\n")

    out.append("## Client side (what users would see)\n")
    out.append("| Stage | Target search/s | Achieved | p50 | p95 | p99 | Errors | Dropped | Target ingest/s | Achieved | p95 | Errors | Dropped |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for s in stages:
        a, b = s["search"], s["ingest"]
        out.append(f"| {s['name']} | {s['searches'] / s['seconds']:.0f} | {a['count'] / s['seconds']:.0f} | {fmt(a['p50'], digits=0)} ms | "
                   f"{fmt(a['p95'])} ms | {fmt(a['p99'])} ms | {fmt(a['failed'], '%', 1)} | {a['dropped']:,} | "
                   f"{s['ingests'] / s['seconds']:.0f} | {b['count'] / s['seconds']:.0f} | {fmt(b['p95'])} ms | {fmt(b['failed'], '%', 1)} | {b['dropped']:,} |")
    out.append("\nTargets: search p95 < 500 ms, p99 < 1 s; ingest p95 < 300 ms; errors < 1%.\n")
    out.append("| Stage | Search p95 < 500 ms | Search p99 < 1 s | Search errors < 1% | Ingest p95 < 300 ms | Ingest errors < 1% |")
    out.append("|---|---|---|---|---|---|")
    mark = lambda ok: "✅" if ok else ("❌" if ok is False else "–")
    for s in stages:
        t1, t2 = s["search"]["targets"], s["ingest"]["targets"]
        out.append(f"| {s['name']} | {mark(t1.get('p(95)<500'))} | {mark(t1.get('p(99)<1000'))} | {mark(t1.get('rate<0.01'))} | "
                   f"{mark(t2.get('p(95)<300'))} | {mark(t2.get('rate<0.01'))} |")

    out.append("\n## Server side (inside the system, same time windows)\n")
    out.append("| Stage | API CPU avg / max | API heap max | API alloc rate | GC pauses | Longest GC pause | Time in GC | API threads max | DB pool waiting max |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for s in stages:
        v = s["server"]
        out.append(f"| {s['name']} | {fmt(v['api_cpu_avg'], '%')} / {fmt(v['api_cpu_max'], '%')} | {fmt(v['api_heap_max'], 'MB')} | "
                   f"{fmt(v['api_alloc_rate'], 'MB')}/s | {fmt(v['api_gc_pauses'])} | {fmt(v['api_gc_max'], 'ms', 1)} | "
                   f"{fmt(v['api_gc_share'], '%', 2)} | {fmt(v['api_threads_max'])} | {fmt(v['db_pending_max'])} |")

    out.append("\n### Where search time goes (server-side p95 per step)\n")
    out.append("| Stage | Whole request | Embed query | Vector (Qdrant) | BM25 (Elasticsearch) | Fusion | Hydrate |")
    out.append("|---|---|---|---|---|---|---|")
    for s in stages:
        p = s["server"]["stages_p95"]
        out.append(f"| {s['name']} | {fmt(s['server']['http_search_p95'], 'ms')} | {fmt(p.get('embed'), 'ms')} | {fmt(p.get('vector'), 'ms')} | "
                   f"{fmt(p.get('bm25'), 'ms')} | {fmt(p.get('fusion'), 'ms', 1)} | {fmt(p.get('hydrate'), 'ms')} |")

    out.append("\n### Elasticsearch and the ingest pipeline\n")
    out.append("| Stage | ES CPU avg | ES heap max | ES time in GC | ES rejected requests | Worker CPU avg | `vehicle.changes` lag max | New-ad freshness p95 |")
    out.append("|---|---|---|---|---|---|---|---|")
    for s in stages:
        v = s["server"]
        out.append(f"| {s['name']} | {fmt(v['es_cpu_avg'], '%')} | {fmt(v['es_heap_max'], 'MB')} | {fmt(v['es_gc_share'], '%', 2)} | "
                   f"{fmt(v['es_rejected'])} | {fmt(v['worker_cpu_avg'], '%')} | {fmt(v['worker_lag_max'])} | {fmt(v['freshness_p95'], 'ms')} |")

    out.append("\nFreshness = from the database commit until both indexes have the ad (+ up to 1 s until Elasticsearch shows it).")
    out.append("CPU is a share of the whole machine as the JVM sees it. Grafana: http://localhost:3000/d/jvm/jvm-and-http "
               f"(set the time range to the run; k6 metrics carry testid=\"{directory.name}\").\n")
    (directory / "report.md").write_text("\n".join(out) + "\n")
    (directory / "summary.json").write_text(json.dumps(stages, indent=1, default=str))


if __name__ == "__main__":
    main(sys.argv[1])
