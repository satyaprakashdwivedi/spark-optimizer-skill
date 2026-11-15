#!/usr/bin/env python3
"""Generate a small synthetic Spark 3.5 event log with typical problems (skewed join stage, disk spill,
high GC, too-large shuffle partitions, AQE off, a Python UDF) plus a matching executor stderr log.

    python3 examples/make_sample_eventlog.py examples/
"""
import json
import os
import random
import sys

MB = 1024 ** 2


def build(out_dir):
    random.seed(7)
    t0 = 1_700_000_000_000
    ev = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
        {"Event": "SparkListenerEnvironmentUpdate", "JVM Information": {}, "Spark Properties": {
            "spark.app.name": "daily_sales_join", "spark.executor.memory": "8g", "spark.executor.cores": "8",
            "spark.executor.instances": "10", "spark.driver.memory": "4g", "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "false", "spark.submit.deployMode": "cluster",
            "spark.pyspark.python": "python3"}},
        {"Event": "SparkListenerApplicationStart", "App Name": "daily_sales_join", "App ID": "application_1700000000000_0042",
         "Timestamp": t0},
    ]
    for i in range(10):
        ev.append({"Event": "SparkListenerExecutorAdded", "Timestamp": t0 + 5000, "Executor ID": str(i + 1),
                   "Executor Info": {"Host": f"worker-{i % 5}", "Total Cores": 8}})
    ev.append({"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart", "executionId": 0,
               "description": "save", "time": t0 + 6000,
               "physicalPlanDescription": "== Physical Plan ==\n*(5) SortMergeJoin [customer_id], [customer_id], Inner\n"
                                          ":- Exchange hashpartitioning(customer_id, 200)\n"
                                          "+- BatchEvalPython [clean_name(name)]\n+- FileScan parquet sales"})
    tid = 0

    def stage(sid, name, n, start, dur_fn, metrics_fn):
        nonlocal tid
        end = start
        for k in range(n):
            d = dur_fn(k)
            launch = start + random.randint(0, 2000)
            m = metrics_fn(k, d)
            ev.append({"Event": "SparkListenerTaskEnd", "Stage ID": sid, "Stage Attempt ID": 0,
                       "Task End Reason": {"Reason": "Success"},
                       "Task Info": {"Task ID": tid, "Executor ID": str(k % 10 + 1), "Launch Time": launch,
                                     "Finish Time": launch + d, "Failed": False, "Killed": False},
                       "Task Metrics": m, "Task Executor Metrics": {"JVMHeapMemory": int(7.2 * 1024 * MB)}})
            tid += 1
            end = max(end, launch + d)
        ev.append({"Event": "SparkListenerStageCompleted", "Stage Info": {
            "Stage ID": sid, "Stage Attempt ID": 0, "Stage Name": name, "Number of Tasks": n,
            "Submission Time": start, "Completion Time": end}})
        return end

    # stage 0: scan 60 GB
    end0 = stage(0, "scan parquet sales", 480, t0 + 7000, lambda k: random.randint(20000, 40000),
                 lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 700_000, "JVM GC Time": int(d * 0.06),
                               "Input Metrics": {"Bytes Read": 128 * MB},
                               "Shuffle Write Metrics": {"Shuffle Bytes Written": 100 * MB}})
    # stage 1: skewed sort-merge join, 200 partitions of ~240 MB, one hot key partition
    def dur1(k):
        return 900_000 if k == 17 else random.randint(40000, 70000)

    def m1(k, d):
        sr = 9 * 1024 * MB if k == 17 else 230 * MB
        return {"Executor Run Time": d, "Executor CPU Time": d * 500_000, "JVM GC Time": int(d * 0.22),
                "Memory Bytes Spilled": sr * 3, "Disk Bytes Spilled": sr // 2, "Peak Execution Memory": 600 * MB,
                "Shuffle Read Metrics": {"Remote Bytes Read": int(sr * 0.8), "Local Bytes Read": int(sr * 0.2),
                                         "Fetch Wait Time": int(d * 0.05)},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 20 * MB}}
    end1 = stage(1, "save at sales_job.py:88", 200, end0 + 500, dur1, m1)
    ev.append({"Event": "SparkListenerJobEnd", "Job ID": 0, "Completion Time": end1, "Job Result": {"Result": "JobSucceeded"}})
    ev.append({"Event": "SparkListenerExecutorRemoved", "Timestamp": end1 - 300000, "Executor ID": "4",
               "Removed Reason": "Container killed by YARN for exceeding physical memory limits. 9.1 GB of 8.8 GB used."})
    ev.append({"Event": "SparkListenerApplicationEnd", "Timestamp": end1 + 2000})

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "sample-eventlog.json"), "w") as f:
        f.write("\n".join(json.dumps(e) for e in ev) + "\n")
    with open(os.path.join(out_dir, "sample-executor-stderr.log"), "w") as f:
        f.write("""23/11/14 10:02:11 INFO UnsafeExternalSorter: Thread 71 spilling sort data of 1824.0 MiB to disk (3 times so far)
23/11/14 10:09:40 WARN YarnAllocator: Container killed by YARN for exceeding physical memory limits. 9.1 GB of 8.8 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
23/11/14 10:09:41 ERROR YarnClusterScheduler: Lost executor 4 on worker-3: Container killed by YARN for exceeding physical memory limits.
23/11/14 10:09:55 WARN TaskSetManager: Lost task 17.0 in stage 1.0 (TID 497): FetchFailed(BlockManagerId(4, worker-3, 7337), shuffleId=0) org.apache.spark.shuffle.FetchFailedException: Failed to connect to worker-3
""")


if __name__ == "__main__":
    build(sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__)))
