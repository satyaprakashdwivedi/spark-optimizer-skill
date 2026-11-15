# Spark optimisation report

**App:** daily_sales_join (`application_1700000000000_0042`) · Spark 3.5.1 · runtime 15.9 min · 1 jobs (0 failed) · 680 tasks (0 failed)

| Metric | Value |
|---|---|
| Input read | 60.0 GB |
| Shuffle read / write | 53.7 GB / 50.8 GB |
| Disk spill | 26.8 GB |
| GC time | 13.2% of task time |
| Shuffle fetch wait | 2.3% of task time |
| Executor core utilisation | 35.6% |
| Executors (peak cores) | 10 (80) × 8 cores |
| Joins in plans | {'SortMergeJoin': 1} |

**Slowest stages**

| Stage | Wall | Tasks | Median / max task | Shuffle read | Spill | GC |
|---|---|---|---|---|---|---|
| 1 save at sales_job.py:88 | 15.0 min | 200 | 54.7s / 15.0 min | 53.7 GB | 26.8 GB | 22.0% |
| 0 scan parquet sales | 41.8s | 480 | 29.6s / 40.0s | 0 B | 0 B | 6.0% |

**Executor memory (per executor)**

| | |
|---|---|
| Heap (`spark.executor.memory`) | 8.0 GB |
| Overhead | 819.2 MB (default: max(384MB, 10%)) |
| Container total | 8.8 GB (819.2 MB outside the heap) |
| Unified memory = (heap - 300MB) x fraction | 4.6 GB, ~591.9 MB per task (8 cores) |
| Measured heap peak | 7.2 GB (90.0% of heap) |

**Log signals**

| Signal | Count | Example |
|---|---|---|
| Container killed for using more memory than heap + overhead (off-heap / Python / native memory) | 2 | `23/11/14 10:09:40 WARN YarnAllocator: Container killed by YARN for exceeding physical memory limits. 9.1 GB of 8.8 GB ph` |
| Shuffle/sort data spilled to disk | 1 | `23/11/14 10:02:11 INFO UnsafeExternalSorter: Thread 71 spilling sort data of 1824.0 MiB to disk (3 times so far)` |
| Executors were lost | 1 | `23/11/14 10:09:41 ERROR YarnClusterScheduler: Lost executor 4 on worker-3: Container killed by YARN for exceeding physic` |
| Shuffle fetch failures (lost executors or overloaded shuffle service) | 1 | `23/11/14 10:09:55 WARN TaskSetManager: Lost task 17.0 in stage 1.0 (TID 497): FetchFailed(BlockManagerId(4, worker-3, 73` |

## Findings

- **[HIGH] Containers were killed for exceeding their memory limit** (memory-overhead): 3 container-kill signals ("exceeding memory limits" / exit code 137). The heap was not the problem: off-heap memory (Netty shuffle buffers, Python workers, native libraries) went past the container limit. Before: heap 8.0 GB + overhead 819.2 MB (default) = container ~8.8 GB, only 819.2 MB outside the heap. After: overhead 2.0 GB on top of the same heap (measured heap peak 7.2 GB = 90.0% leaves no room to shrink it).
  - Do not fix this by raising spark.executor.memory: with the default overhead, +8g heap buys only +0.8g off-heap. If tasks then spill, add shuffle partitions rather than heap. If kills continue, lower spark.executor.cores so fewer tasks share one container. (Kept the node-fitted spark.executor.memoryOverhead=3072m, spark.executor.pyspark.memory=4g instead of raising it; if this persists, use fewer cores per executor.)
- **[HIGH] Shuffle partitions are too large** (shuffle-partitions): Largest shuffle (stage 1) read 53.7 GB over 200 tasks = 274.9 MB per partition (target ~128 MB). Current spark.sql.shuffle.partitions = 200, ~85 cores.
  - With AQE on, a generous partition count is safe: AQE coalesces small partitions at runtime.
- **[HIGH] Adaptive Query Execution is off** (aqe): Spark 3.5.1 supports AQE but spark.sql.adaptive.enabled is false.
- **[HIGH] Data skew in 1 stage(s)** (skew): Stage 1: slowest task 15.0 min vs median 54.7s (16.5x); shuffle read per task max 9.0 GB vs median 230.0 MB. Name: save at sales_job.py:88
  - AQE only splits skewed partitions of sort-merge joins. For skewed groupBy/window keys, salt the hot key (add a random suffix, aggregate twice) or filter/handle null keys separately.
- **[HIGH] Tasks spilled to disk** (spill): 26.8 GB spilled to disk (161.1 GB in memory form) in 1 stage(s); worst is stage 1 (26.8 GB).
  - Spill means a task's partition did not fit in its share of execution memory (executor memory × spark.memory.fraction ÷ cores). Smaller partitions or fewer cores per executor fix it without more memory.
- **[HIGH] Slow or failing shuffle fetches** (shuffle-fetch): Fetch wait = 2.3% of task time; 1 fetch-failure lines.
  - Fetch failures usually follow lost executors (memory kills, spot/preemptible nodes). Fix the kills first; on YARN enable the external shuffle service.
- **[MEDIUM] Executor size for your nodes** (executor-shape): 16 cores / 64.0 GB per node: 3 executor(s) per node × 5 cores, leaving 1 core + 1 GB for the OS/node manager and 1 executor slot for the driver/AM across 6 nodes.
  - Rule of thumb: 4-5 cores per executor keeps HDFS/S3 throughput high and GC pauses short. Heap, overhead and Python memory are carved from each executor's share of the node so containers pack exactly.
- **[MEDIUM] High garbage-collection time** (gc): GC took 13.2% of task time (healthy: under 5-10%).
  - Also check for caching large deserialized objects (prefer MEMORY_AND_DISK_SER / DataFrame cache), huge collect()s and UDFs creating many objects.
- **[MEDIUM] Executors sat idle most of the time** (utilisation): Only 35.6% of executor core-time ran tasks (peak 80 cores). You paid for idle cores: driver-side work, small stages or long tails.
  - Look for driver-only phases (collect, toPandas, single-threaded Python) and stages with very few tasks.
- **[MEDIUM] Row-at-a-time Python UDFs** (python-udf): Physical plan contains BatchEvalPython (classic Python UDF).
  - Replace with built-in functions or vectorised pandas_udf (Arrow) — typically several times faster.
- **[MEDIUM] Task / executor failures** (failures): 0 failed tasks, 0 failed stages, executors removed: {'memory/killed': 1}.
  - Retries hide problems and cost time; fix the root cause above (memory kills, skew, disk).

## Recommended config

| Config | Current | Recommended | Why |
|---|---|---|---|
| `spark.dynamicAllocation.enabled` | (default) | `true` | utilisation |
| `spark.dynamicAllocation.executorIdleTimeout` | (default) | `60s` | utilisation |
| `spark.dynamicAllocation.maxExecutors` | (default) | `17` | utilisation |
| `spark.dynamicAllocation.minExecutors` | (default) | `1` | utilisation |
| `spark.dynamicAllocation.shuffleTracking.enabled` | (default) | `true` | utilisation |
| `spark.executor.cores` | 8 | `5` | executor-shape |
| `spark.executor.extraJavaOptions` | (default) | `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=4` | gc |
| `spark.executor.instances` | 10 | `17` | executor-shape |
| `spark.executor.memory` | 8g | `14g` | executor-shape |
| `spark.executor.memoryOverhead` | (default) | `3072m` | executor-shape |
| `spark.executor.pyspark.memory` | (default) | `4g` | executor-shape |
| `spark.reducer.maxSizeInFlight` | (default) | `96m` | shuffle-fetch |
| `spark.shuffle.io.maxRetries` | (default) | `10` | shuffle-fetch |
| `spark.shuffle.io.retryWait` | (default) | `30s` | shuffle-fetch |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | (default) | `128m` | shuffle-partitions |
| `spark.sql.adaptive.coalescePartitions.enabled` | (default) | `true` | aqe |
| `spark.sql.adaptive.enabled` | false | `true` | skew |
| `spark.sql.adaptive.skewJoin.enabled` | (default) | `true` | skew |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | (default) | `3` | skew |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | (default) | `128m` | skew |
| `spark.sql.execution.arrow.pyspark.enabled` | (default) | `true` | python-udf |
| `spark.sql.shuffle.partitions` | 200 | `510` | shuffle-partitions |

```bash
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=60s \
  --conf spark.dynamicAllocation.maxExecutors=17 \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.executor.cores=5 \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=4" \
  --conf spark.executor.instances=17 \
  --conf spark.executor.memory=14g \
  --conf spark.executor.memoryOverhead=3072m \
  --conf spark.executor.pyspark.memory=4g \
  --conf spark.reducer.maxSizeInFlight=96m \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.shuffle.io.retryWait=30s \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128m \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=3 \
  --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=128m \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.sql.shuffle.partitions=510 \
  your_job.py
```

_Analysed: sample-eventlog.json, sample-executor-stderr.log_
