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

## Heap OOM vs container kill: read the kill message first
Two failures that look alike and need different settings.

| Message | What ran out | Setting to change |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` / `GC overhead limit exceeded` | JVM heap: a task holds too much | partitions (more), cores per executor (fewer), then `spark.executor.memory` |
| "Container killed… exceeding memory limits", exit code 137, `OOMKilled` | off-heap: Netty shuffle buffers, Python workers, Arrow, native libraries | `spark.executor.memoryOverhead` (+ `spark.executor.pyspark.memory`) |

**The memory model of one executor:**
- container = heap + overhead (+ `pyspark.memory` + `offHeap.size`);
- default overhead = max(384MB, `memoryOverheadFactor` × heap), where the factor is 0.10;
- unified memory (execution + storage) = (heap − 300MB) × `spark.memory.fraction` (default 0.6), shared by the
  executor's concurrent tasks.

**Why "just bump executor memory" is wrong for kills:** with the default factor, +8g of heap buys only +0.8g of
off-heap room. You pay for 8g you didn't need to get 0.8g you did.

**Container-kill rule:**
1. New overhead = max(2 × current, 15% of heap (20% for PySpark), 1 GB), rounded to a clean value.
2. If the event log has a measured heap peak, there's no heap OOM and GC is under 10%, shrink the heap to
   peak × 1.25 (rounded up to a whole GB), as long as heap + new overhead fits inside the current container.
3. Otherwise keep the heap and add the overhead on top. The container grows; if kills persist, lower
   `spark.executor.cores`.

Worked example (5 cores):

| | Heap | Overhead | Container | Outside heap | Unified per task |
|---|---|---|---|---|---|
| Before | 16g | 1.6g (default) | ~17.6g | 1.6g | ~1.9g |
| After (heap peak 9.5g) | 12g | 4g | 16g | 4g (2.5×) | ~1.4g |

If tasks then spill, add shuffle partitions rather than heap.

**Kubernetes:** an explicit `spark.executor.memoryOverhead` overrides `spark.kubernetes.memoryOverheadFactor` and
`spark.executor.memoryOverheadFactor`. The pod is killed with OOMKilled / exit 137.

**Heap OOM rule:**
- Usually one huge partition (skew, too few partitions), wide rows, or a large broadcast or collect.
- Fix partition sizing and skew first, then raise `spark.executor.memory` by about 1.5×.
- The heap is never shrunk when a heap OOM is present.
- With text logs only (current memory unknown), the analyzer gives relative advice instead of inventing an absolute
  value.

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
