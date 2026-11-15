#!/usr/bin/env python3
"""Spark log analyzer: reads Spark event logs and/or driver/executor text logs, measures what went wrong
(skew, spill, GC, memory kills, shuffle sizing, idle executors, failures) and recommends Spark configs.

Standard library only (Python 3.8+).

Usage:
  analyze_spark_log.py <event-log | rolling-log dir | driver/executor log | folder> [...]
                       [--node-cores N --node-mem-gb G --nodes K] [--target-partition-mb 128]
                       [--json] [--out report.md]

Inputs:
  * Spark event logs (JSON lines; plain or .gz). Rolling logs: the eventlog_v2_* directory.
    Compressed with lz4/zstd/snappy? Decompress first (e.g. `zstd -d file.zstd`) or download the
    uncompressed log from the Spark History Server (Download button on the app page).
  * Driver / executor stderr logs (YARN, Kubernetes, Databricks, EMR): scanned for known error patterns.
"""
import argparse
import gzip
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict

MB = 1024 ** 2
GB = 1024 ** 3

# ----------------------------------------------------------------------------------------------- helpers

_SIZE = re.compile(r"^\s*([\d.]+)\s*([kmgtp]?)(i?b)?\s*$", re.I)
_MULT = {"": 1, "k": 1024, "m": MB, "g": GB, "t": 1024 ** 4, "p": 1024 ** 5}


def parse_size(v, default_unit="m"):
    """'4g' -> bytes. Plain numbers use default_unit (Spark memory settings default to MiB)."""
    if v is None:
        return None
    m = _SIZE.match(str(v))
    if not m:
        return None
    unit = (m.group(2) or ("" if m.group(3) else default_unit)).lower()
    return int(float(m.group(1)) * _MULT[unit])


def fmt_size(b):
    """bytes -> '6.0 GB'."""
    if b is None:
        return "-"
    for unit, size in (("TB", 1024 ** 4), ("GB", GB), ("MB", MB), ("KB", 1024)):
        if b >= size:
            return f"{b / size:.1f} {unit}"
    return f"{b} B"


def round_gb(b):
    """Round bytes up to a clean Spark memory string: whole GiB above 2 GiB, else 512 MiB steps."""
    if b <= 2 * GB:
        return f"{math.ceil(b / (512 * MB)) * 512}m"
    return f"{math.ceil(b / GB)}g"


def fmt_ms(ms):
    if ms is None:
        return "-"
    s = ms / 1000
    if s < 120:
        return f"{s:.1f}s"
    if s < 7200:
        return f"{s / 60:.1f} min"
    return f"{s / 3600:.2f} h"


def pct(a, b):
    return 100.0 * a / b if b else 0.0


def pctl(values, p):
    if not values:
        return 0
    v = sorted(values)
    k = (len(v) - 1) * p / 100
    f, c = math.floor(k), math.ceil(k)
    return v[f] if f == c else v[f] + (v[c] - v[f]) * (k - f)


def version_tuple(v):
    nums = re.findall(r"\d+", v or "")
    return tuple(int(x) for x in nums[:3]) if nums else (0,)


def truthy(v):
    return str(v).strip().lower() == "true"


# ----------------------------------------------------------------------------------------------- input

def open_text(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    for ext in (".lz4", ".zstd", ".zst", ".snappy", ".lzf"):
        if path.endswith(ext):
            raise ValueError(f"{path}: compressed with {ext[1:]}. Decompress it first "
                             f"(e.g. `zstd -d` / `lz4 -d`) or download the uncompressed log from the History Server.")
    return open(path, "r", errors="replace")


def is_event_log(path):
    try:
        with open_text(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    return line.startswith("{") and '"Event"' in line
    except (OSError, ValueError):
        return False
    return False


def expand_inputs(paths):
    """-> (list of event-log groups (each a list of files in order), list of text log files)."""
    event_groups, text_logs = [], []
    for p in paths:
        p = os.path.expanduser(p)
        if os.path.isdir(p):
            names = sorted(os.listdir(p))
            rolling = [n for n in names if n.startswith("events_")]
            if rolling:  # rolling event log dir: events_<n>_<appId>[.codec]
                rolling.sort(key=lambda n: int(re.match(r"events_(\d+)", n).group(1)) if re.match(r"events_(\d+)", n) else 0)
                event_groups.append([os.path.join(p, n) for n in rolling])
                continue
            sub_ev, sub_tx = expand_inputs([os.path.join(p, n) for n in names if not n.startswith(".")])
            event_groups += sub_ev
            text_logs += sub_tx
        elif os.path.isfile(p):
            (event_groups.append([p]) if is_event_log(p) else text_logs.append(p))
        else:
            raise FileNotFoundError(p)
    return event_groups, text_logs


# ----------------------------------------------------------------------------------------------- event log

class App:
    def __init__(self):
        self.name = self.app_id = self.spark_version = None
        self.start = self.end = None
        self.props = {}
        self.executors = {}  # id -> {cores, host, added, removed, reason}
        self.resource_profiles = {}
        self.tasks = defaultdict(list)  # (stage, attempt) -> list of task dicts
        self.stages = {}  # (stage, attempt) -> info
        self.task_fail = Counter()
        self.jobs_failed = 0
        self.jobs = 0
        self.plans = []
        self.peak_heap = 0
        self.max_result_size = 0
        self.python = False
        self.max_event_ts = 0

    # -- parsing
    def feed(self, e):
        ev = e.get("Event", "")
        ts = e.get("Timestamp") or e.get("time")
        if isinstance(ts, (int, float)):
            self.max_event_ts = max(self.max_event_ts, ts)
        if ev == "SparkListenerLogStart":
            self.spark_version = e.get("Spark Version")
        elif ev == "SparkListenerApplicationStart":
            self.name, self.app_id, self.start = e.get("App Name"), e.get("App ID"), e.get("Timestamp")
        elif ev == "SparkListenerApplicationEnd":
            self.end = e.get("Timestamp")
        elif ev == "SparkListenerEnvironmentUpdate":
            sp = e.get("Spark Properties") or {}
            if isinstance(sp, list):
                sp = {k: v for k, v in sp}
            self.props.update(sp)
            if any(k.startswith("spark.pyspark") or k == "spark.submit.pyFiles" for k in sp) or \
                    "pyspark" in str(sp.get("spark.app.name", "")).lower():
                self.python = True
        elif ev == "SparkListenerResourceProfileAdded":
            req = e.get("Executor Resource Requests") or {}
            self.resource_profiles[e.get("Resource Profile Id")] = {k: (v or {}).get("Amount") for k, v in req.items()}
        elif ev == "SparkListenerExecutorAdded":
            info = e.get("Executor Info") or {}
            self.executors[e.get("Executor ID")] = {"cores": info.get("Total Cores") or 0, "host": info.get("Host"),
                                                     "added": e.get("Timestamp"), "removed": None, "reason": None}
        elif ev == "SparkListenerExecutorRemoved":
            x = self.executors.setdefault(e.get("Executor ID"), {"cores": 0, "added": e.get("Timestamp")})
            x["removed"], x["reason"] = e.get("Timestamp"), e.get("Removed Reason")
        elif ev == "SparkListenerTaskEnd":
            self._task_end(e)
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info") or {}
            self.stages[(si.get("Stage ID"), si.get("Stage Attempt ID", 0))] = {
                "name": si.get("Stage Name", ""), "tasks": si.get("Number of Tasks", 0),
                "submitted": si.get("Submission Time"), "completed": si.get("Completion Time"),
                "failure": si.get("Failure Reason")}
        elif ev == "SparkListenerJobEnd":
            self.jobs += 1
            if (e.get("Job Result") or {}).get("Result") != "JobSucceeded":
                self.jobs_failed += 1
        elif ev.endswith("SparkListenerSQLExecutionStart"):
            plan = e.get("physicalPlanDescription") or ""
            self.plans.append(plan)
            if re.search(r"BatchEvalPython|ArrowEvalPython|PythonUDF|FlatMapGroupsInPandas|MapInPandas", plan):
                self.python = True

    def _task_end(self, e):
        info, m = e.get("Task Info") or {}, e.get("Task Metrics") or {}
        reason = (e.get("Task End Reason") or {}).get("Reason", "Success")
        if reason != "Success":
            self.task_fail[reason] += 1
        sr, sw = m.get("Shuffle Read Metrics") or {}, m.get("Shuffle Write Metrics") or {}
        launch, finish = info.get("Launch Time") or 0, info.get("Finish Time") or 0
        t = {
            "ok": reason == "Success" and not info.get("Failed") and not info.get("Killed"),
            "exec": info.get("Executor ID"), "dur": max(0, finish - launch) if finish else 0,
            "run": m.get("Executor Run Time", 0) or 0, "cpu_ns": m.get("Executor CPU Time", 0) or 0,
            "gc": m.get("JVM GC Time", 0) or 0, "result": m.get("Result Size", 0) or 0,
            "mem_spill": m.get("Memory Bytes Spilled", 0) or 0, "disk_spill": m.get("Disk Bytes Spilled", 0) or 0,
            "peak_exec": m.get("Peak Execution Memory", 0) or 0,
            "input": (m.get("Input Metrics") or {}).get("Bytes Read", 0) or 0,
            "output": (m.get("Output Metrics") or {}).get("Bytes Written", 0) or 0,
            "sr": (sr.get("Remote Bytes Read", 0) or 0) + (sr.get("Local Bytes Read", 0) or 0),
            "fetch_wait": sr.get("Fetch Wait Time", 0) or 0,
            "sw": sw.get("Shuffle Bytes Written", 0) or 0,
            "speculative": bool(info.get("Speculative")),
        }
        self.max_result_size = max(self.max_result_size, t["result"])
        heap = (e.get("Task Executor Metrics") or {}).get("JVMHeapMemory") or 0
        self.peak_heap = max(self.peak_heap, heap)
        self.tasks[(e.get("Stage ID"), e.get("Stage Attempt ID", 0))].append(t)


def load_event_log(files):
    app = App()
    bad = 0
    for path in files:
        with open_text(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    app.feed(json.loads(line))
                except (ValueError, TypeError, AttributeError):
                    bad += 1
    app.bad_lines = bad
    return app


# ----------------------------------------------------------------------------------------------- text logs

TEXT_PATTERNS = [  # id, regex, meaning
    ("oom_heap", r"OutOfMemoryError: Java heap space", "Executor/driver JVM ran out of heap"),
    ("oom_gc", r"OutOfMemoryError: GC overhead limit exceeded", "JVM spent almost all its time in GC"),
    ("oom_direct", r"OutOfMemoryError: Direct buffer memory", "Off-heap (direct) memory exhausted (often shuffle/netty)"),
    ("oom_meta", r"OutOfMemoryError: Metaspace", "Metaspace exhausted"),
    ("container_killed", r"(Container killed by YARN for exceeding (physical|virtual) memory limits|"
                         r"Container .* is running beyond physical memory limits|OOMKilled|exit code[: ]+137|"
                         r"exitCode[=: ]+137|Consider boosting spark\.(yarn\.)?executor\.memoryOverhead)",
     "Container killed for using more memory than heap + overhead (off-heap / Python / native memory)"),
    ("fetch_failed", r"(FetchFailedException|MetadataFetchFailedException|Missing an output location for shuffle)",
     "Shuffle fetch failures (lost executors or overloaded shuffle service)"),
    ("max_result", r"bigger than spark\.driver\.maxResultSize", "Results collected to the driver exceed maxResultSize"),
    ("broadcast_timeout", r"(Could not execute broadcast in \d+ secs|TimeoutException.*[Bb]roadcast|"
                          r"Futures timed out after \[\d+ seconds\].*broadcast)", "Broadcast join timed out"),
    ("broadcast_oom", r"(Not enough memory to build and broadcast the table|Cannot broadcast the table that is larger than)",
     "Broadcast side too large for the driver/executor"),
    ("spill", r"(Spilling (in-memory map|sort data|data)|spilling .* to disk|Task \d+ force spilling)",
     "Shuffle/sort data spilled to disk"),
    ("lost_executor", r"(Lost executor|ExecutorLostFailure|Executor heartbeat timed out)", "Executors were lost"),
    ("disk_full", r"No space left on device", "Local disk full (shuffle/spill directories)"),
    ("open_files", r"Too many open files", "OS file-handle limit hit (many shuffle files)"),
    ("python_worker", r"(Python worker exited unexpectedly|PythonException|crashed.*python)", "Python worker crashed (often memory)"),
    ("stack_overflow", r"StackOverflowError", "Very long lineage / deep recursion"),
    ("task_failed_max", r"failed \d+ times; aborting job", "A task hit spark.task.maxFailures and the job aborted"),
]


def scan_text_logs(files):
    hits = {}
    for path in files:
        try:
            f = open_text(path)
        except (OSError, ValueError) as ex:
            hits.setdefault("_unreadable", {"count": 0, "example": str(ex), "meaning": "Could not read"})["count"] += 1
            continue
        with f:
            for line in f:
                for pid, rx, meaning in TEXT_PATTERNS:
                    if re.search(rx, line):
                        h = hits.setdefault(pid, {"count": 0, "example": line.strip()[:300], "meaning": meaning,
                                                  "file": os.path.basename(path)})
                        h["count"] += 1
    return hits


# ----------------------------------------------------------------------------------------------- metrics

def summarize(app):
    s = {"app_name": app.name, "app_id": app.app_id, "spark_version": app.spark_version,
         "duration_ms": (app.end or app.max_event_ts or 0) - (app.start or 0) if app.start else None,
         "python": app.python, "jobs": app.jobs, "jobs_failed": app.jobs_failed}
    all_tasks = [t for ts in app.tasks.values() for t in ts]
    ok = [t for t in all_tasks if t["ok"]]
    s["tasks"], s["tasks_failed"] = len(all_tasks), len(all_tasks) - len(ok)
    s["task_fail_reasons"] = dict(app.task_fail)
    run = sum(t["run"] for t in all_tasks)
    s["total_task_run_ms"] = run
    s["gc_ms"] = sum(t["gc"] for t in all_tasks)
    s["gc_pct"] = round(pct(s["gc_ms"], run), 1)
    s["disk_spill"] = sum(t["disk_spill"] for t in all_tasks)
    s["mem_spill"] = sum(t["mem_spill"] for t in all_tasks)
    s["input_bytes"] = sum(t["input"] for t in all_tasks)
    s["shuffle_read"] = sum(t["sr"] for t in all_tasks)
    s["shuffle_write"] = sum(t["sw"] for t in all_tasks)
    s["fetch_wait_pct"] = round(pct(sum(t["fetch_wait"] for t in all_tasks), run), 1)
    s["max_result_size"] = app.max_result_size
    s["peak_jvm_heap"] = app.peak_heap
    s["cpu_pct_of_run"] = round(pct(sum(t["cpu_ns"] for t in all_tasks) / 1e6, run), 1)

    # executors: cores, peak concurrent, utilisation
    exs = app.executors
    cores = Counter(x.get("cores", 0) for x in exs.values() if x.get("cores"))
    s["executors_total"] = len(exs)
    s["executor_cores"] = cores.most_common(1)[0][0] if cores else None
    end = app.end or app.max_event_ts
    events = []
    for x in exs.values():
        if x.get("added"):
            events.append((x["added"], x.get("cores", 0)))
            events.append((x.get("removed") or end or x["added"], -x.get("cores", 0)))
    cur = peak = 0
    for _, d in sorted(events):
        cur += d
        peak = max(peak, cur)
    s["peak_cores"] = peak
    slot_ms = sum(((x.get("removed") or end or 0) - x["added"]) * x.get("cores", 0) for x in exs.values()
                  if x.get("added") and (x.get("removed") or end))
    s["slot_utilisation_pct"] = round(pct(run, slot_ms), 1) if slot_ms else None
    s["executors_peak"] = max(1, round(peak / s["executor_cores"])) if s["executor_cores"] and peak else None
    removed = Counter()
    for x in exs.values():
        r = (x.get("reason") or "")
        if r:
            key = ("memory/killed" if re.search(r"137|143|memory|OOM|killed", r, re.I) else
                   "idle (dynamic allocation)" if re.search(r"idle", r, re.I) else
                   "lost/heartbeat" if re.search(r"heartbeat|lost|unreachable", r, re.I) else r[:60])
            removed[key] += 1
    s["executors_removed"] = dict(removed)

    # stages
    stages = []
    for key, ts in app.tasks.items():
        oks = [t for t in ts if t["ok"]] or ts
        durs = [t["dur"] for t in oks]
        srs = [t["sr"] for t in oks]
        info = app.stages.get(key, {})
        st = {
            "stage": key[0], "attempt": key[1], "name": (info.get("name") or "")[:80], "tasks": len(ts),
            "failed_tasks": sum(1 for t in ts if not t["ok"]),
            "wall_ms": (info.get("completed") or 0) - (info.get("submitted") or 0) if info.get("submitted") else max(durs or [0]),
            "task_ms_median": int(statistics.median(durs)) if durs else 0, "task_ms_p95": int(pctl(durs, 95)),
            "task_ms_max": max(durs or [0]),
            "input": sum(t["input"] for t in ts), "shuffle_read": sum(srs), "shuffle_write": sum(t["sw"] for t in ts),
            "sr_median": int(statistics.median(srs)) if srs else 0, "sr_max": max(srs or [0]),
            "disk_spill": sum(t["disk_spill"] for t in ts), "mem_spill": sum(t["mem_spill"] for t in ts),
            "gc_pct": round(pct(sum(t["gc"] for t in ts), sum(t["run"] for t in ts)), 1),
            "failure": info.get("failure"),
        }
        st["skew_ratio"] = round(st["task_ms_max"] / st["task_ms_median"], 1) if st["task_ms_median"] else 0
        st["sr_skew_ratio"] = round(st["sr_max"] / st["sr_median"], 1) if st["sr_median"] else 0
        st["avg_input_per_task"] = st["input"] // max(1, st["tasks"])
        st["avg_sr_per_task"] = st["shuffle_read"] // max(1, st["tasks"])
        stages.append(st)
    stages.sort(key=lambda x: -x["wall_ms"])
    s["stages"] = stages
    s["stages_failed"] = sum(1 for st in app.stages.values() if st.get("failure"))

    # plans
    joins = Counter()
    for p in app.plans:
        for j in ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin", "CartesianProduct"):
            joins[j] += len(re.findall(r"\b" + j + r"\b", p))
    s["joins"] = {k: v for k, v in joins.items() if v}
    s["python_udf"] = any(re.search(r"BatchEvalPython|PythonUDF", p) for p in app.plans)
    s["arrow_udf"] = any(re.search(r"ArrowEvalPython|MapInPandas|FlatMapGroupsInPandas", p) for p in app.plans)
    s["aqe_in_plan"] = any("AdaptiveSparkPlan" in p for p in app.plans)
    return s


# ----------------------------------------------------------------------------------------------- rules

class Advisor:
    def __init__(self, props, summary, text_hits, args):
        self.p, self.s, self.h, self.a = props, summary, text_hits, args
        self.findings = []
        self.rec = {}  # key -> (value, finding id)
        self.locked = set()  # keys fixed by the node-fitted executor shape; other rules may not change them

    def get(self, k, default=None):
        return self.p.get(k, default)

    def add(self, fid, severity, title, evidence, configs=None, advice=None):
        kept, held = {}, []
        for k, v in (configs or {}).items():
            if k in self.locked:
                held.append(f"{k}={self.rec[k][0]}")
                continue
            kept[k] = v
            self.rec[k] = (v, fid)
        if held:
            advice = ((advice + " ") if advice else "") + \
                f"(Kept the node-fitted {', '.join(held)} instead of raising it; if this persists, " \
                f"use fewer cores per executor.)"
        self.findings.append({"id": fid, "severity": severity, "title": title, "evidence": evidence,
                              "configs": kept, "advice": advice})

    # ---- sizing facts
    def facts(self):
        ver = version_tuple(self.s.get("spark_version") or self.get("spark.version") or "")
        self.ver = ver
        self.aqe_supported = ver >= (3, 0)
        aqe_default = ver >= (3, 2)
        self.aqe_on = truthy(self.get("spark.sql.adaptive.enabled", str(aqe_default).lower())) and self.aqe_supported
        self.exec_cores = int(self.get("spark.executor.cores") or self.s.get("executor_cores") or 1)
        self.exec_mem = parse_size(self.get("spark.executor.memory", "1g"))
        ov = self.get("spark.executor.memoryOverhead") or self.get("spark.yarn.executor.memoryOverhead")
        self.overhead = parse_size(ov) if ov else max(384 * MB, int(0.10 * self.exec_mem))
        self.overhead_explicit = bool(ov)
        self.driver_mem = parse_size(self.get("spark.driver.memory", "1g"))
        self.shuffle_partitions = int(self.get("spark.sql.shuffle.partitions", 200))
        self.total_cores = self.s.get("peak_cores") or (self.exec_cores * int(self.get("spark.executor.instances", 2)))
        self.dyn = truthy(self.get("spark.dynamicAllocation.enabled", "false"))
        self.python = self.s.get("python") or "python_worker" in self.h
        self.planned_cores = None

    # ---- rules
    def run(self):
        self.facts()
        s, h = self.s, self.h
        stages = s.get("stages", [])
        target = self.a.target_partition_mb * MB

        # 1. executor shape (first, so later rules respect the node budget)
        self.size_executors()
        if self.planned_cores:
            self.total_cores = self.planned_cores

        # 2. memory kills / OOM
        killed = h.get("container_killed", {}).get("count", 0) + s.get("executors_removed", {}).get("memory/killed", 0)
        oom = sum(h.get(k, {}).get("count", 0) for k in ("oom_heap", "oom_gc"))
        if killed:
            frac = 0.20 if self.python else 0.15
            new_ov = max(self.overhead * 2, int(frac * self.exec_mem), 1 * GB)
            cfg = {"spark.executor.memoryOverhead": round_gb(new_ov)}
            if self.python:
                cfg["spark.executor.pyspark.memory"] = round_gb(max(1 * GB, self.exec_mem // 4))
            self.add("memory-overhead", "high", "Containers were killed for exceeding their memory limit",
                     f"{killed} container-kill / memory-limit signals. Heap is not the problem here: off-heap "
                     f"(netty shuffle buffers, Python workers, native libs) exceeded the container's overhead"
                     + (f" of {fmt_size(self.overhead)}{' (default 10%)' if not self.overhead_explicit else ''}."
                        if self.p else " (current value unknown: no event log given, sized from defaults)."),
                     cfg, "If kills continue, also lower spark.executor.cores so fewer tasks share one container.")
        if oom or h.get("oom_direct"):
            cfg = {"spark.executor.memory": round_gb(int(self.exec_mem * 1.5))}
            if h.get("oom_direct"):
                cfg["spark.executor.memoryOverhead"] = round_gb(max(self.overhead * 2, 1 * GB))
            self.add("executor-oom", "high", "Executors ran out of memory (OutOfMemoryError)",
                     f"{oom} heap/GC OOM lines, {h.get('oom_direct', {}).get('count', 0)} direct-memory OOM lines.",
                     cfg, "Usually caused by skewed partitions, huge partitions or wide rows: fix partition sizing/skew "
                          "first; more memory is the fallback.")

        # 3. shuffle partition sizing
        big = max(stages, key=lambda st: st["shuffle_read"], default=None)
        if big and big["shuffle_read"] > 0:
            ideal = max(self.total_cores or 1, math.ceil(big["shuffle_read"] / target))
            if self.total_cores:
                ideal = math.ceil(ideal / self.total_cores) * self.total_cores
            per_part = big["shuffle_read"] / max(1, big["tasks"])
            if per_part > 2 * target or per_part < target / 8 and big["tasks"] > 2 * (self.total_cores or 1):
                direction = "too large" if per_part > 2 * target else "too small"
                cfg = {"spark.sql.shuffle.partitions": str(ideal)}
                if self.aqe_supported:
                    cfg.update({"spark.sql.adaptive.enabled": "true",
                                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                                "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{self.a.target_partition_mb}m"})
                    if direction == "too small":
                        cfg["spark.sql.shuffle.partitions"] = str(max(ideal, self.shuffle_partitions))
                        cfg["spark.sql.adaptive.coalescePartitions.initialPartitionNum"] = str(max(ideal, self.shuffle_partitions))
                self.add("shuffle-partitions", "high" if direction == "too large" else "medium",
                         f"Shuffle partitions are {direction}",
                         f"Largest shuffle (stage {big['stage']}) read {fmt_size(big['shuffle_read'])} over "
                         f"{big['tasks']} tasks = {fmt_size(per_part)} per partition (target ~{self.a.target_partition_mb} MB). "
                         f"Current spark.sql.shuffle.partitions = {self.shuffle_partitions}, ~{self.total_cores} cores.",
                         cfg, "With AQE on, a generous partition count is safe: AQE coalesces small partitions at runtime.")
        if self.aqe_supported and not self.aqe_on:
            self.add("aqe", "high", "Adaptive Query Execution is off",
                     f"Spark {s.get('spark_version')} supports AQE but spark.sql.adaptive.enabled is false.",
                     {"spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled": "true",
                      "spark.sql.adaptive.skewJoin.enabled": "true"})

        # 4. skew
        skewed = [st for st in stages if st["tasks"] >= 20 and st["skew_ratio"] >= 4 and st["task_ms_max"] >= 30000]
        if skewed:
            w = skewed[0]
            cfg = {}
            if self.aqe_supported:
                cfg = {"spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.skewJoin.enabled": "true",
                       "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
                       "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "128m"}
            self.add("skew", "high", f"Data skew in {len(skewed)} stage(s)",
                     f"Stage {w['stage']}: slowest task {fmt_ms(w['task_ms_max'])} vs median {fmt_ms(w['task_ms_median'])} "
                     f"({w['skew_ratio']}x); shuffle read per task max {fmt_size(w['sr_max'])} vs median "
                     f"{fmt_size(w['sr_median'])}. Name: {w['name']}",
                     cfg, "AQE only splits skewed partitions of sort-merge joins. For skewed groupBy/window keys, "
                          "salt the hot key (add a random suffix, aggregate twice) or filter/handle null keys separately.")

        # 5. spill
        if s["disk_spill"] > 0:
            spill_stages = sorted((st for st in stages if st["disk_spill"]), key=lambda x: -x["disk_spill"])
            w = spill_stages[0]
            sev = "high" if s["disk_spill"] > 10 * GB or s["disk_spill"] > 0.2 * max(1, s["shuffle_read"]) else "medium"
            cfg = {}
            if w["avg_sr_per_task"] > target or w["avg_input_per_task"] > 2 * target:
                pass  # partition sizing rule above covers it
            else:
                cfg["spark.executor.memory"] = round_gb(int(self.exec_mem * 1.5))
            self.add("spill", sev, "Tasks spilled to disk",
                     f"{fmt_size(s['disk_spill'])} spilled to disk ({fmt_size(s['mem_spill'])} in memory form) in "
                     f"{len(spill_stages)} stage(s); worst is stage {w['stage']} ({fmt_size(w['disk_spill'])}).",
                     cfg, "Spill means a task's partition did not fit in its share of execution memory "
                          "(executor memory × spark.memory.fraction ÷ cores). Smaller partitions or fewer cores per "
                          "executor fix it without more memory.")

        # 6. GC
        if s["gc_pct"] >= 10 and s["total_task_run_ms"] > 60000:
            cfg = {"spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 "
                                                      "-XX:ConcGCThreads=4"}
            if not s.get("disk_spill"):
                cfg["spark.executor.memory"] = round_gb(int(self.exec_mem * 1.25))
            self.add("gc", "high" if s["gc_pct"] >= 20 else "medium", "High garbage-collection time",
                     f"GC took {s['gc_pct']}% of task time (healthy: under 5-10%).", cfg,
                     "Also check for caching large deserialized objects (prefer MEMORY_AND_DISK_SER / DataFrame cache), "
                     "huge collect()s and UDFs creating many objects.")

        # 7. utilisation / over-provisioning
        util = s.get("slot_utilisation_pct")
        if util is not None and util < 40 and (s.get("duration_ms") or 0) > 5 * 60000:
            cfg = {"spark.dynamicAllocation.enabled": "true",
                   "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                   "spark.dynamicAllocation.executorIdleTimeout": "60s"}
            planned = self.rec.get("spark.executor.instances", (None,))[0]
            if planned or s.get("executors_peak"):
                cfg["spark.dynamicAllocation.maxExecutors"] = str(planned or s["executors_peak"])
                cfg["spark.dynamicAllocation.minExecutors"] = "1"
            self.add("utilisation", "medium", "Executors sat idle most of the time",
                     f"Only {util}% of executor core-time ran tasks (peak {s.get('peak_cores')} cores). "
                     f"You paid for idle cores: driver-side work, small stages or long tails.",
                     cfg if not self.dyn else {"spark.dynamicAllocation.executorIdleTimeout": "60s"},
                     "Look for driver-only phases (collect, toPandas, single-threaded Python) and stages with very few tasks.")

        # 8. too few tasks for the cluster
        low_par = [st for st in stages if st["wall_ms"] > 60000 and 0 < st["tasks"] < (self.total_cores or 0) / 2]
        if low_par:
            w = low_par[0]
            cfg = {"spark.default.parallelism": str(2 * (self.total_cores or 1))}
            self.add("parallelism", "medium", "Long stages used only part of the cluster",
                     f"Stage {w['stage']} ran {fmt_ms(w['wall_ms'])} with {w['tasks']} tasks on ~{self.total_cores} cores.",
                     cfg, "If it reads files, check spark.sql.files.maxPartitionBytes and whether input is a few big "
                          "unsplittable files (gzip). Repartition right after reading if needed.")

        # 9. small input splits
        scans = [st for st in stages if st["input"] > 0 and st["tasks"] > 4 * (self.total_cores or 1)]
        small = [st for st in scans if st["avg_input_per_task"] < 16 * MB]
        if small:
            w = max(small, key=lambda x: x["tasks"])
            self.add("small-files", "medium", "Input split into many tiny tasks",
                     f"Stage {w['stage']} read {fmt_size(w['input'])} with {w['tasks']} tasks "
                     f"(~{fmt_size(w['avg_input_per_task'])} each).",
                     {"spark.sql.files.maxPartitionBytes": "256m", "spark.sql.files.openCostInBytes": "8m"},
                     "Too many small files: compact them upstream (e.g. Delta OPTIMIZE / Iceberg rewrite_data_files).")

        # 10. shuffle fetch health
        if s["fetch_wait_pct"] >= 15 or h.get("fetch_failed"):
            cfg = {"spark.shuffle.io.maxRetries": "10", "spark.shuffle.io.retryWait": "30s",
                   "spark.reducer.maxSizeInFlight": "96m"}
            self.add("shuffle-fetch", "high" if h.get("fetch_failed") else "medium", "Slow or failing shuffle fetches",
                     f"Fetch wait = {s['fetch_wait_pct']}% of task time; {h.get('fetch_failed', {}).get('count', 0)} "
                     f"fetch-failure lines.", cfg,
                     "Fetch failures usually follow lost executors (memory kills, spot/preemptible nodes). Fix the "
                     "kills first; on YARN enable the external shuffle service.")

        # 11. driver
        if h.get("max_result") or s.get("max_result_size", 0) > 512 * MB:
            self.add("driver-results", "high", "Large results collected to the driver",
                     f"Biggest single task result {fmt_size(s.get('max_result_size'))}; "
                     f"{h.get('max_result', {}).get('count', 0)} maxResultSize errors.",
                     {"spark.driver.maxResultSize": "4g", "spark.driver.memory": round_gb(max(self.driver_mem * 2, 4 * GB))},
                     "Avoid collect()/toPandas() on big data: write results out or aggregate first.")
        if h.get("broadcast_timeout") or h.get("broadcast_oom"):
            self.add("broadcast", "high", "Broadcast join problems",
                     f"{h.get('broadcast_timeout', {}).get('count', 0)} broadcast timeouts, "
                     f"{h.get('broadcast_oom', {}).get('count', 0)} broadcast-too-large errors.",
                     {"spark.sql.broadcastTimeout": "900", "spark.sql.autoBroadcastJoinThreshold": "32m"},
                     "If a broadcast hint forces a big table, remove the hint; keep the auto threshold modest.")
        if s.get("joins", {}).get("BroadcastNestedLoopJoin") or s.get("joins", {}).get("CartesianProduct"):
            self.add("bad-join", "high", "Nested-loop / cartesian join in the plan",
                     f"Plans contain {s['joins']}.", None,
                     "Usually a join with no equality condition (or an OR/non-equi condition). Rewrite the join keys; "
                     "no config fixes this.")

        # 12. python
        if s.get("python_udf"):
            self.add("python-udf", "medium", "Row-at-a-time Python UDFs",
                     "Physical plan contains BatchEvalPython (classic Python UDF).",
                     {"spark.sql.execution.arrow.pyspark.enabled": "true"},
                     "Replace with built-in functions or vectorised pandas_udf (Arrow) — typically several times faster.")

        # 13. serializer for RDD-heavy apps
        if "kryo" not in str(self.get("spark.serializer", "")).lower() and not s.get("joins") and s.get("tasks", 0) > 0 \
                and s.get("shuffle_read", 0) > 1 * GB:
            self.add("kryo", "low", "Java serialization for RDD shuffles",
                     "No SQL plans found but large shuffles, and spark.serializer is the Java default.",
                     {"spark.serializer": "org.apache.spark.serializer.KryoSerializer"})

        # 14. failures summary
        if s.get("tasks_failed") or s.get("stages_failed") or h.get("lost_executor") or h.get("disk_full"):
            reasons = f" {s['task_fail_reasons']}" if s.get("task_fail_reasons") else ""
            ev = f"{s.get('tasks_failed', 0)} failed tasks{reasons}, {s.get('stages_failed', 0)} failed stages, " \
                 f"executors removed: {s.get('executors_removed') or 'none'}."
            if h.get("disk_full"):
                ev += " Local disk filled up (No space left on device)."
            self.add("failures", "medium", "Task / executor failures", ev, None,
                     "Retries hide problems and cost time; fix the root cause above (memory kills, skew, disk).")
        return self

    def size_executors(self):
        a = self.a
        if a.node_cores and a.node_mem_gb:
            cores = 5 if a.node_cores >= 16 else 4 if a.node_cores >= 8 else max(1, a.node_cores - 1)
            per_node = max(1, (a.node_cores - 1) // cores)
            usable = (a.node_mem_gb - 1) * GB / per_node
            killed = bool(self.h.get("container_killed")) or "memory/killed" in (self.s.get("executors_removed") or {})
            ov_frac = 0.15 if killed else 0.10
            py_frac = 0.15 if self.python else 0.0
            heap_gb = max(1, int(usable * (1 - ov_frac - py_frac) / GB))
            cfg = {"spark.executor.cores": str(cores), "spark.executor.memory": f"{heap_gb}g"}
            rest = usable - heap_gb * GB
            if self.python:
                cfg["spark.executor.pyspark.memory"] = round_gb(int(rest * py_frac / (ov_frac + py_frac)))
                rest -= parse_size(cfg["spark.executor.pyspark.memory"])
            cfg["spark.executor.memoryOverhead"] = f"{max(384, int(rest / MB))}m"
            if a.nodes:
                cfg["spark.executor.instances"] = str(max(1, per_node * a.nodes - 1))
            self.planned_cores = cores * int(cfg.get("spark.executor.instances", 0) or 0) or None
            self.add("executor-shape", "medium", "Executor size for your nodes",
                     f"{a.node_cores} cores / {a.node_mem_gb} GB per node: {per_node} executor(s) per node × {cores} cores, "
                     f"leaving 1 core + 1 GB for the OS/node manager"
                     f"{f' and 1 executor slot for the driver/AM across {a.nodes} nodes' if a.nodes else ''}.",
                     cfg, "Rule of thumb: 4-5 cores per executor keeps HDFS/S3 throughput high and GC pauses short. "
                          "Heap, overhead" + (" and Python memory" if self.python else "") +
                          " are carved from each executor's share of the node so containers pack exactly.")
            self.locked |= {k for k in cfg if k != "spark.executor.instances"}
        elif self.exec_cores > 5:
            cfg = {"spark.executor.cores": "5",
                   "spark.executor.memory": round_gb(int(self.exec_mem * 5 / self.exec_cores))}
            inst = self.get("spark.executor.instances") or self.s.get("executors_peak")
            if inst:
                cfg["spark.executor.instances"] = str(math.ceil(int(inst) * self.exec_cores / 5))
            self.add("executor-shape", "medium", "Very fat executors",
                     f"spark.executor.cores = {self.exec_cores}. More than ~5 concurrent tasks per JVM tends to cause "
                     f"long GC pauses and I/O contention.", cfg,
                     "Same memory per core and the same total cores, split into more, smaller executors. "
                     "Pass --node-cores/--node-mem-gb/--nodes for an exact fit to your machines.")
        elif self.exec_cores == 1 and (self.s.get("executors_total") or 0) > 20:
            self.add("executor-shape", "low", "Very thin executors",
                     "1 core per executor: broadcast variables and caches are duplicated in every JVM.",
                     {"spark.executor.cores": "4", "spark.executor.memory": round_gb(self.exec_mem * 4)})

    # ---- output
    def recommended(self):
        rows = []
        for k, (v, fid) in sorted(self.rec.items()):
            cur = self.p.get(k)
            if cur is not None and str(cur).strip() == str(v).strip():
                continue
            rows.append({"key": k, "current": cur, "recommended": v, "because": fid})
        return rows


# ----------------------------------------------------------------------------------------------- report

SEV_ORDER = {"high": 0, "medium": 1, "low": 2}


def report_md(summary, findings, recs, text_hits, inputs):
    s = summary or {}
    out = ["# Spark optimisation report", ""]
    if s:
        out += [f"**App:** {s.get('app_name') or '?'} (`{s.get('app_id') or '?'}`) · Spark {s.get('spark_version') or '?'} · "
                f"runtime {fmt_ms(s.get('duration_ms'))} · {s.get('jobs')} jobs ({s.get('jobs_failed')} failed) · "
                f"{s.get('tasks')} tasks ({s.get('tasks_failed')} failed)", "",
                "| Metric | Value |", "|---|---|",
                f"| Input read | {fmt_size(s.get('input_bytes'))} |",
                f"| Shuffle read / write | {fmt_size(s.get('shuffle_read'))} / {fmt_size(s.get('shuffle_write'))} |",
                f"| Disk spill | {fmt_size(s.get('disk_spill'))} |",
                f"| GC time | {s.get('gc_pct')}% of task time |",
                f"| Shuffle fetch wait | {s.get('fetch_wait_pct')}% of task time |",
                f"| Executor core utilisation | {s.get('slot_utilisation_pct', '-')}% |",
                f"| Executors (peak cores) | {s.get('executors_total')} ({s.get('peak_cores')}) × {s.get('executor_cores')} cores |",
                f"| Joins in plans | {s.get('joins') or '-'} |", ""]
        top = s.get("stages", [])[:5]
        if top:
            out += ["**Slowest stages**", "", "| Stage | Wall | Tasks | Median / max task | Shuffle read | Spill | GC |",
                    "|---|---|---|---|---|---|---|"]
            for st in top:
                out.append(f"| {st['stage']} {st['name'][:40]} | {fmt_ms(st['wall_ms'])} | {st['tasks']} | "
                           f"{fmt_ms(st['task_ms_median'])} / {fmt_ms(st['task_ms_max'])} | {fmt_size(st['shuffle_read'])} | "
                           f"{fmt_size(st['disk_spill'])} | {st['gc_pct']}% |")
            out.append("")
    if text_hits:
        out += ["**Log signals**", "", "| Signal | Count | Example |", "|---|---|---|"]
        for k, v in sorted(text_hits.items(), key=lambda kv: -kv[1]["count"]):
            out.append(f"| {v['meaning']} | {v['count']} | `{v['example'][:120]}` |")
        out.append("")
    out += ["## Findings", ""]
    if not findings:
        out.append("No problems detected — the job looks healthy for the data it processed.")
    for f in sorted(findings, key=lambda f: SEV_ORDER[f["severity"]]):
        out.append(f"- **[{f['severity'].upper()}] {f['title']}** ({f['id']}): {f['evidence']}")
        if f.get("advice"):
            out.append(f"  - {f['advice']}")
    out += ["", "## Recommended config", ""]
    if recs:
        out += ["| Config | Current | Recommended | Why |", "|---|---|---|---|"]
        out += [f"| `{r['key']}` | {r['current'] if r['current'] is not None else '(default)'} | `{r['recommended']}` | {r['because']} |"
                for r in recs]
        out += ["", "```bash", "spark-submit \\"]
        out += [f"  --conf {r['key']}={json.dumps(str(r['recommended'])) if ' ' in str(r['recommended']) else r['recommended']} \\"
                for r in recs]
        out += ["  your_job.py", "```"]
    else:
        out.append("No config changes recommended.")
    out += ["", f"_Analysed: {', '.join(os.path.basename(i) for i in inputs)}_"]
    return "\n".join(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--node-cores", type=int, help="cores per worker node (for executor sizing)")
    ap.add_argument("--node-mem-gb", type=float, help="memory per worker node in GB available to YARN/K8s")
    ap.add_argument("--nodes", type=int, help="number of worker nodes")
    ap.add_argument("--target-partition-mb", type=int, default=128, help="target shuffle partition size (default 128)")
    ap.add_argument("--json", action="store_true", help="print JSON instead of markdown")
    ap.add_argument("--out", help="also write the report to this file")
    a = ap.parse_args(argv)

    groups, texts = expand_inputs(a.paths)
    if len(groups) > 1:
        print(f"note: {len(groups)} event logs found; analysing each separately", file=sys.stderr)
    text_hits = scan_text_logs(texts) if texts else {}
    results = []
    for files in groups or [[]]:
        app = load_event_log(files) if files else None
        summary = summarize(app) if app else {}
        props = app.props if app else {}
        adv = Advisor(props, summary or {"stages": [], "disk_spill": 0, "mem_spill": 0, "gc_pct": 0,
                                          "total_task_run_ms": 0, "fetch_wait_pct": 0, "shuffle_read": 0},
                      text_hits, a).run()
        recs = adv.recommended()
        results.append({"inputs": files + texts, "summary": summary, "findings": adv.findings,
                        "recommended": recs, "log_signals": text_hits,
                        "current_config": {k: v for k, v in props.items() if k.startswith("spark.") and
                                           not re.search(r"password|secret|token|key", k, re.I)}})
    if a.json:
        out = json.dumps(results if len(results) > 1 else results[0], indent=2, default=str)
    else:
        out = "\n\n---\n\n".join(report_md(r["summary"], r["findings"], r["recommended"], r["log_signals"], r["inputs"])
                                 for r in results)
    if a.out:
        with open(a.out, "w") as f:
            f.write(out + "\n")
    print(out)


if __name__ == "__main__":
    main()
