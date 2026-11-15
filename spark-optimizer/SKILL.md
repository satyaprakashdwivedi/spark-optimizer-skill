---
name: spark-optimizer
description: Analyse Apache Spark logs and recommend the best Spark configuration. Use when the user shares or points to a Spark event log, History Server download, eventlog_v2 folder, or driver/executor stderr (YARN, Kubernetes, EMR, Databricks, Dataproc). Also use when they ask to tune or speed up a Spark job, or to fix OOM, "Container killed by YARN", spill, skew, slow stages, GC or shuffle fetch failures, or to size executors, memory, cores and spark.sql.shuffle.partitions.
---

# Spark optimizer

Turn Spark logs into a measured diagnosis and a concrete, conflict-free config. The bundled analyzer does the
measuring. Your job is to check its findings against the user's context and to explain the trade-offs.

## 1. Collect inputs

Ask only for what's missing. One message with all the questions is fine.

- **Logs (required).** Any of:
  - **Spark event log:** the most useful input. Get it from the History Server → app → *Download*, or from the
    `spark.eventLog.dir` path, e.g. `hdfs:///spark-logs/application_…`, an S3 or DBFS path, or an `eventlog_v2_*`
    folder for rolling logs.
  - **Driver or executor stderr:** from `yarn logs -applicationId <id>`, `kubectl logs`, or the Databricks/EMR log
    folders.
  - A folder that contains either kind.
  - `.gz` files are read directly. Files compressed with lz4, zstd or snappy need decompressing first
    (`zstd -d file.zstd`, `lz4 -d file.lz4`).
- **Cluster shape (strongly recommended):** cores and memory per worker node, and the number of nodes. Without
  them the analyzer can only adjust executors relative to the current settings.
- **Context:** the SLA or goal (fastest, cheapest, or just stop failing), whether the cluster is shared, and the
  platform (YARN, K8s, Databricks, EMR).

## 2. Run the analyzer

```bash
python3 <skill-dir>/scripts/analyze_spark_log.py <logs...> \
    [--node-cores 16 --node-mem-gb 64 --nodes 6] [--target-partition-mb 128] [--json] [--out report.md]
```

- `<skill-dir>` is the folder this SKILL.md lives in. The script needs only the Python 3.9+ standard library.
- Pass the event log and the stderr logs together. They complement each other: the event log gives the metrics,
  and stderr gives the reasons (kills, OOM, fetch failures).
- Use `--json` when you need the raw numbers (per-stage metrics, current config, findings).
- Several apps can be passed at once. Each event log gets its own report.

## 3. Review before recommending

Read the report and sanity-check it. Don't just paste it.

1. **Root cause first, by order of fix.** Fix things in this order:
   1. Memory kills and OOM.
   2. Skew.
   3. Partition sizing and spill.
   4. GC.
   5. Utilisation.
   6. Small files.

   Many symptoms share one cause. For example, one skewed partition causes the spill, the long tail and the
   container kill.
2. **Code beats config.** Call out problems no config can fix:
   - cartesian or nested-loop joins;
   - row-at-a-time Python UDFs;
   - `collect()` or `toPandas()` on big data;
   - skewed `groupBy` keys (they need salting);
   - many tiny input files (they need compaction).
3. **Check the numbers add up:**
   - per-executor heap + `memoryOverhead` + `pyspark.memory` ≤ the node memory ÷ executors per node;
   - total executor cores ≤ the cluster's cores, minus 1 core per node;
   - `maxExecutors` ≥ `spark.executor.instances`.
4. **Check the platform's defaults:**
   - AQE is on by default from Spark 3.2.
   - Databricks manages executor sizing. There, focus on SQL and AQE settings, and on instance types.
   - On K8s, overhead is `spark.kubernetes.memoryOverheadFactor` (0.1 by default; 0.4 for non-JVM jobs).
   - On EMR, `maximizeResourceAllocation` may already set executor size.
5. **Spark version:**
   - On Spark 2.x, drop the AQE settings and use `spark.sql.shuffle.partitions` sizing only.
   - Spark 3.0–3.1 needs AQE switched on explicitly.

See `references/tuning-rules.md` for the thresholds, formulas and why each rule exists.

## 4. Answer format

1. **Summary:** 2-3 lines. Runtime, the biggest problem, and the expected gain, phrased as an estimate.
2. **Findings:** ranked, each with its evidence from the logs (stage, numbers).
3. **Recommended config:**
   - a `spark-submit --conf` block, plus the same settings as a `spark-defaults.conf` snippet;
   - only keys that change, each with a one-line reason.
4. **Code changes:** if any are needed (salting, broadcast hints, UDF rewrites, compaction).
5. **How to verify:** what to compare in the next run's event log, such as stage time, spill, GC % and
   max/median task time.

Be honest about uncertainty:
- Text logs alone give signals, not metrics. Say so, and ask for the event log.
- Never invent numbers that are not in the analyzer output.
