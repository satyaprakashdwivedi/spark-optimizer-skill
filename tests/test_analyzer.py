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


if __name__ == "__main__":
    unittest.main()
