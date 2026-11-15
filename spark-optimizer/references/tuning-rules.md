# Spark tuning rules used by the analyzer

Each rule lists its trigger, why it matters and what to change. The thresholds are starting points: adjust them to the
workload.

## Executor shape
- **Trigger:** `--node-cores/--node-mem-gb` given, `spark.executor.cores` > 5, or 1-core executors in a large app.
- **Why:**
  - At more than about 5 concurrent tasks per JVM, GC pauses and HDFS/S3 client contention grow.
  - With 1-core executors, every JVM duplicates its broadcasts and caches.
- **Formula:**
  1. Cores per executor: 5 on nodes with 16 or more cores, otherwise 4.
  2. Executors per node: (node cores − 1) ÷ cores per executor.
  3. Per-executor budget: (node memory − 1 GB) ÷ executors per node.
  4. Split the budget into heap, `memoryOverhead` (10%, or 15% after memory kills) and `pyspark.memory` (15% for
     PySpark).
  5. Instances: executors per node × nodes − 1 (one slot is left for the driver or YARN AM).
- **With node info:** the analyzer locks these values, so other rules can't push the container past the node.

## Memory overhead / container kills
- **Trigger:** "Container killed by YARN for exceeding memory limits", exit code 137, OOMKilled, or executors
  removed for memory.
- **Why:** the container limit is heap + overhead (+ pyspark memory). Kills come from off-heap use, not heap:
  - netty shuffle buffers;
  - Python workers;
  - Arrow;
  - native libraries.
- **Fix:**
  - Raise `spark.executor.memoryOverhead` to at least 15% of heap, and at least 1 GB.
  - Set `spark.executor.pyspark.memory` for PySpark.
  - If kills persist, use fewer cores per executor.

## OutOfMemoryError (heap)
- **Why:** usually one huge partition (skew, too few partitions), wide rows, or large broadcast/collect.
- **Fix:** partition sizing and skew first. Then raise `spark.executor.memory` by about 1.5×.

## Shuffle partitions
- **Target:** about 128 MB of shuffle data per partition (`--target-partition-mb`).
- **Formula:**
  - partitions = largest stage shuffle read ÷ target, rounded up to a multiple of total cores;
  - never fewer than the total cores.
- **Trigger:** more than 2× the target per partition (too large, which spills), or less than 1/8 of the target with
  far more tasks than cores (too small: scheduling overhead and tiny files).
- **AQE (Spark 3.0+):**
  - Turn on `spark.sql.adaptive.enabled` and `coalescePartitions`, and set `advisoryPartitionSizeInBytes`.
  - With AQE on, a high starting partition count is safe because AQE merges small partitions at runtime.

## Skew
- **Trigger:** a stage with 20 or more tasks where the slowest task is at least 4× the median and at least 30 s.
  Shuffle read per task (max vs median) confirms it's a data problem, not a slow node.
- **Fix:**
  - **Sort-merge joins:** AQE skew join (`skewJoin.enabled`, `skewedPartitionFactor`,
    `skewedPartitionThresholdInBytes`).
  - **groupBy or window skew:** salt the hot key and aggregate in two passes.
  - **Null keys:** filter them out, or handle them separately.
  - **Small side under ~100 MB:** broadcast it.

## Spill
- **Trigger:** disk spill above 0. It is high when above 10 GB or above 20% of shuffle read.
- **Why:** a task's partition did not fit in its execution memory share, which is roughly heap × 0.6 ÷ cores.
- **Fix:**
  - Smaller partitions, or fewer cores per executor.
  - Raise memory only when partitions are already reasonable.

## Garbage collection
- **Trigger:** GC above 10% of task time (high above 20%).
- **Fix:**
  - G1GC with an earlier `InitiatingHeapOccupancyPercent`.
  - Fewer cores per executor.
  - Cache in serialized form.
  - Avoid object-heavy UDFs.
  - More memory only when nothing is spilling.

## Utilisation
- **Trigger:** task time is under 40% of the executor core time paid for, on jobs longer than 5 minutes.
- **Fix:**
  - Turn on dynamic allocation, with `shuffleTracking` (no external shuffle service needed, Spark 3.0+) and a 60 s
    idle timeout.
  - Look for driver-only phases and long tails.

## Parallelism
- **Trigger:** a stage longer than 60 s using fewer tasks than half the cores.
- **Fix:**
  - `spark.default.parallelism` = 2 × cores.
  - Repartition after reading unsplittable inputs (gzip).
  - Tune `spark.sql.files.maxPartitionBytes`.

## Small files
- **Trigger:** input stages with more than 4× as many tasks as cores, at under 16 MB per task.
- **Fix:**
  - `spark.sql.files.maxPartitionBytes` = 256m and `openCostInBytes` = 8m.
  - Compact upstream (Delta OPTIMIZE, Iceberg `rewrite_data_files`, Hive concatenation).

## Shuffle fetch
- **Trigger:** fetch wait above 15% of task time, or FetchFailedException.
- **Fix:**
  - Fix lost executors first.
  - Raise `spark.shuffle.io.maxRetries` and `retryWait`.
  - Turn on the external shuffle service on YARN.
  - On spot instances, enable decommissioning (`spark.decommission.enabled`, Spark 3.1+).

## Driver
- **maxResultSize errors, or task results above 512 MB:** raise `spark.driver.maxResultSize` and the driver memory,
  but mainly stop collecting big data.
- **Broadcast timeouts:** raise `spark.sql.broadcastTimeout` and keep `autoBroadcastJoinThreshold` modest (around
  32m); remove any forced hints on big tables.

## Plans
- **BroadcastNestedLoopJoin or CartesianProduct:** the join has no equi-condition. Rewrite it.
- **BatchEvalPython:** a row-at-a-time Python UDF. Use built-in functions or `pandas_udf` with Arrow.

## Serialization
- **RDD-heavy jobs with large shuffles** that still use the Java serializer: use `KryoSerializer`. DataFrames
  already use an efficient binary format (Tungsten).
