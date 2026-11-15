"""Tests for the Spark log analyzer (stdlib unittest): python3 -m unittest discover -s tests"""
import gzip
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "spark-optimizer", "scripts"))
sys.path.insert(0, os.path.join(ROOT, "examples"))
import analyze_spark_log as A  # noqa: E402
import make_sample_eventlog  # noqa: E402

GB = 1024 ** 3


def run(*args):
    buf = io.StringIO()
    with redirect_stdout(buf):
        A.main(list(args))
    return buf.getvalue()


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        make_sample_eventlog.build(cls.dir)
        cls.ev = os.path.join(cls.dir, "sample-eventlog.json")
        cls.err = os.path.join(cls.dir, "sample-executor-stderr.log")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir)

    def result(self, *extra):
        return json.loads(run(self.ev, self.err, "--json", *extra))


class TestHelpers(unittest.TestCase):
    def test_parse_size(self):
        self.assertEqual(A.parse_size("4g"), 4 * GB)
        self.assertEqual(A.parse_size("512m"), 512 * 1024 ** 2)
        self.assertEqual(A.parse_size("2048"), 2048 * 1024 ** 2)  # Spark memory default unit is MiB
        self.assertEqual(A.parse_size("1.5gb"), int(1.5 * GB))

    def test_round_gb(self):
        self.assertEqual(A.round_gb(int(1.2 * GB)), "1536m")
        self.assertEqual(A.round_gb(int(5.1 * GB)), "6g")

    def test_version(self):
        self.assertTrue(A.version_tuple("3.5.1") >= (3, 2))
        self.assertTrue(A.version_tuple("2.4.8") < (3, 0))


class TestFindings(Base):
    def test_detects_core_problems(self):
        ids = {f["id"] for f in self.result()["findings"]}
        for expected in ("skew", "spill", "aqe", "shuffle-partitions", "memory-overhead", "gc", "python-udf",
                         "executor-shape"):
            self.assertIn(expected, ids)

    def test_metrics(self):
        s = self.result()["summary"]
        self.assertEqual(s["spark_version"], "3.5.1")
        self.assertEqual(s["executor_cores"], 8)
        self.assertGreater(s["disk_spill"], 0)
        top = s["stages"][0]
        self.assertEqual(top["stage"], 1)
        self.assertGreater(top["skew_ratio"], 10)

    def test_partitions_sized_to_target(self):
        recs = {r["key"]: r["recommended"] for r in self.result()["recommended"]}
        n = int(recs["spark.sql.shuffle.partitions"])
        self.assertGreaterEqual(n, 400)  # ~54 GB / 128 MB
        self.assertEqual(recs["spark.sql.adaptive.enabled"], "true")

    def test_node_fit_adds_up(self):
        r = self.result("--node-cores", "16", "--node-mem-gb", "64", "--nodes", "6")
        recs = {x["key"]: x["recommended"] for x in r["recommended"]}
        self.assertEqual(recs["spark.executor.cores"], "5")
        self.assertEqual(recs["spark.executor.instances"], "17")
        per_exec = sum(A.parse_size(recs[k]) for k in ("spark.executor.memory", "spark.executor.memoryOverhead",
                                                      "spark.executor.pyspark.memory"))
        self.assertLessEqual(per_exec, (64 - 1) * GB / 3)
        self.assertEqual(recs["spark.dynamicAllocation.maxExecutors"], "17")
        self.assertEqual(int(recs["spark.sql.shuffle.partitions"]) % 85, 0)  # multiple of planned cores

    def test_text_only(self):
        r = json.loads(run(self.err, "--json"))
        self.assertEqual(r["summary"], {})
        ids = {f["id"] for f in r["findings"]}
        self.assertIn("memory-overhead", ids)
        self.assertIn("shuffle-fetch", ids)

    def test_gzip_and_rolling_dir(self):
        roll = os.path.join(self.dir, "eventlog_v2_app")
        os.makedirs(roll, exist_ok=True)
        with open(self.ev) as f:
            lines = f.read().splitlines(True)
        half = len(lines) // 2
        with gzip.open(os.path.join(roll, "events_1_app.gz"), "wt") as f:
            f.writelines(lines[:half])
        with open(os.path.join(roll, "events_2_app"), "w") as f:
            f.writelines(lines[half:])
        r = json.loads(run(roll, "--json"))
        self.assertEqual(r["summary"]["tasks"], 680)

    def test_secrets_not_echoed(self):
        with open(self.ev) as f:
            lines = f.readlines()
        env = json.loads(lines[1])
        env["Spark Properties"]["spark.hadoop.fs.s3a.secret.key"] = "SUPERSECRET"
        lines[1] = json.dumps(env) + "\n"
        p = os.path.join(self.dir, "with-secret.json")
        with open(p, "w") as f:
            f.writelines(lines)
        self.assertNotIn("SUPERSECRET", run(p, "--json"))
        self.assertNotIn("SUPERSECRET", run(p))

    def test_markdown_report(self):
        md = run(self.ev, self.err)
        self.assertIn("## Recommended config", md)
        self.assertIn("spark-submit", md)


def write_kill_log(path, heap="16g", peak_gb=9.5, master="k8s://https://k8s.example:443", oom=False):
    """The 'Spark didn't run out of heap' scenario: 16g heap, 5 cores, default overhead, exit-137 kill."""
    ev = [{"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
          {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {
              "spark.executor.memory": heap, "spark.executor.cores": "5", "spark.executor.instances": "4",
              "spark.master": master}},
          {"Event": "SparkListenerApplicationStart", "App Name": "orders_enrich", "App ID": "spark-a1", "Timestamp": 0}]
    for i in range(4):
        ev.append({"Event": "SparkListenerExecutorAdded", "Timestamp": 1000, "Executor ID": str(i),
                   "Executor Info": {"Total Cores": 5}})
    for k in range(40):
        ev.append({"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Stage Attempt ID": 0,
                   "Task End Reason": {"Reason": "Success"},
                   "Task Info": {"Task ID": k, "Executor ID": str(k % 4), "Launch Time": 2000, "Finish Time": 32000},
                   "Task Metrics": {"Executor Run Time": 30000, "JVM GC Time": 900},
                   "Task Executor Metrics": {"JVMHeapMemory": int(peak_gb * GB)}})
    ev.append({"Event": "SparkListenerExecutorRemoved", "Timestamp": 30000, "Executor ID": "2",
               "Removed Reason": "The executor with id 2 exited with exit code 137(SIGKILL, possible container OOM)."})
    ev.append({"Event": "SparkListenerApplicationEnd", "Timestamp": 40000})
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(e) for e in ev) + "\n")


class TestHeapVsContainerKill(unittest.TestCase):
    """Heap OOM and container kills are different problems with different knobs."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def path(self, name, text=None):
        p = os.path.join(self.dir, name)
        if text is not None:
            with open(p, "w") as f:
                f.write(text)
        return p

    def recs(self, r):
        return {x["key"]: x["recommended"] for x in r["recommended"]}

    def test_post_example_shifts_heap_to_overhead(self):
        # Before 16g heap + 1.6g default overhead (17.6g container) -> after 12g + 4g = 16g, 2.5x off-heap room
        ev = self.path("ev.json")
        write_kill_log(ev)
        r = json.loads(run(ev, "--json"))
        recs = self.recs(r)
        self.assertEqual(recs["spark.executor.memory"], "12g")
        self.assertEqual(recs["spark.executor.memoryOverhead"], "4g")
        mm = r["memory_model"]
        self.assertAlmostEqual(mm["container"] / GB, 17.6, places=1)
        self.assertAlmostEqual(mm["outside_heap"] / GB, 1.6, places=1)
        f = next(x for x in r["findings"] if x["id"] == "memory-overhead")
        self.assertIn("2.5x the off-heap room", f["evidence"])
        self.assertIn("smaller container", f["evidence"])
        self.assertIn("memoryOverheadFactor", f["advice"])  # Kubernetes note

    def test_unified_memory_matches_formula(self):
        ev = self.path("ev.json")
        write_kill_log(ev, heap="12g")
        mm = json.loads(run(ev, "--json"))["memory_model"]
        self.assertAlmostEqual(mm["unified"] / GB, (12 - 300 / 1024) * 0.6, places=2)  # ~7 GB
        self.assertAlmostEqual(mm["unified_per_task"] / GB, (12 - 300 / 1024) * 0.6 / 5, places=2)

    def test_no_shrink_when_heap_is_nearly_full(self):
        ev = self.path("ev.json")
        write_kill_log(ev, peak_gb=15.0, master="yarn")
        r = json.loads(run(ev, "--json"))
        recs = self.recs(r)
        self.assertNotIn("spark.executor.memory", recs)
        self.assertEqual(recs["spark.executor.memoryOverhead"], "4g")
        f = next(x for x in r["findings"] if x["id"] == "memory-overhead")
        self.assertIn("no room to shrink", f["evidence"])
        self.assertNotIn("memoryOverheadFactor", f["advice"])  # not Kubernetes

    def test_never_shrinks_heap_when_heap_also_ooms(self):
        ev = self.path("ev.json")
        write_kill_log(ev)
        log = self.path("exec.log", "java.lang.OutOfMemoryError: Java heap space\n")
        recs = self.recs(json.loads(run(ev, log, "--json")))
        self.assertEqual(recs["spark.executor.memory"], "24g")  # heap OOM wins: 1.5x, never shrink

    def test_text_only_kill_gives_relative_advice(self):
        log = self.path("driver.log", "WARN Executor 2 lost, reason: OOMKilled, exit code 137\n")
        r = json.loads(run(log, "--json"))
        f = next(x for x in r["findings"] if x["id"] == "memory-overhead")
        self.assertEqual(f["configs"], {})
        self.assertIn("15-25%", f["advice"])
        self.assertNotIn("spark.executor.memory", self.recs(r))

    def test_text_only_heap_oom_no_absolute_value(self):
        log = self.path("exec.log", "java.lang.OutOfMemoryError: Java heap space\n")
        r = json.loads(run(log, "--json"))
        f = next(x for x in r["findings"] if x["id"] == "executor-oom")
        self.assertEqual(f["configs"], {})
        self.assertIn("unknown", f["evidence"])
        self.assertIn("off-heap", f["evidence"])


if __name__ == "__main__":
    unittest.main()
