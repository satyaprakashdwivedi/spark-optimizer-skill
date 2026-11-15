# Spark Optimizer: a Claude skill

A [Claude skill](https://docs.claude.com/en/docs/agents-and-tools/agent-skills/overview) that reads **Apache Spark logs** and recommends the
**best Spark configuration** for the job. It works out what slowed the job down or made it fail, then gives you a ready-to-paste
`spark-submit` config whose settings don't conflict with each other.

## What it does
Give Claude your Spark event log and/or your driver and executor logs, and ask it to "tune this Spark job". The skill then:

1. **Measures the run** with a bundled analyzer (`scripts/analyze_spark_log.py`). It needs only the Python standard library, so
   there's nothing to install. It reports:
   - per-stage task time: median, p95 and max, with a skew ratio;
   - shuffle read and write;
   - spill to memory and disk;
   - GC time;
   - fetch wait;
   - input split size;
   - executor core utilisation;
   - failed tasks and why executors were removed;
   - join types and Python UDFs in the physical plans;
   - your current Spark config.
2. **Scans text logs** for known failure signatures:
   - OOM (heap, GC overhead, direct memory, metaspace);
   - *Container killed by YARN* / exit code 137 / OOMKilled;
   - FetchFailed;
   - maxResultSize;
   - broadcast timeouts;
   - spill;
   - lost executors;
   - *No space left on device*;
   - Python worker crashes.
3. **Applies tuning rules** and produces findings ranked by severity, each with evidence from the logs:
   - executor shape;
   - memory overhead;
   - shuffle partition sizing;
   - AQE;
   - skew joins;
   - spill;
   - G1GC;
   - dynamic allocation;
   - parallelism;
   - small files;
   - shuffle fetch retries;
   - driver result size and broadcasts;
   - non-equi joins;
   - Arrow for Python;
   - Kryo.
4. **Sizes executors to your nodes** when you give the node cores, memory and count. Heap, `memoryOverhead` and `pyspark.memory`
   are carved from each executor's share of the node, so the recommendations always fit together and containers pack exactly.
5. **Claude reviews the result.** It reasons about root causes, separates fixes that need code changes from fixes that only need
   config, adjusts for your platform (YARN, Kubernetes, EMR, Databricks) and Spark version, and tells you how to verify the
   improvement on the next run.

### Example
From a synthetic job ([`examples/`](examples)) with a skewed sort-merge join, 54 GB of shuffle over 200 partitions, AQE off,
a Python UDF and a YARN memory kill:

| Finding | Evidence |
|---|---|
| Containers killed for memory | 3 kill signals; overhead was the default 10% |
| Shuffle partitions too large | 53.7 GB over 200 tasks = 275 MB per partition (target 128 MB) |
| AQE off | Spark 3.5.1 supports it |
| Data skew | slowest task 15 min vs median 55 s (16.5×); 9 GB vs 230 MB shuffle read |
| Disk spill | 26.8 GB |
| High GC | 13% of task time |

The recommended config for 6 nodes with 16 cores and 64 GB each:
- **Executors:** 17 executors, each with 5 cores, 14g heap, 3g overhead and 4g Python memory;
- **Shuffle partitions:** 510;
- **AQE:** on, with skew-join handling;
- **Dynamic allocation:** on, capped at 17 executors;
- **Python:** Arrow enabled;
- **GC:** G1GC.

Full report: [`examples/sample-report.md`](examples/sample-report.md).

## Install

**Claude Code**: personal skill, available in every project:
```bash
git clone https://github.com/satyaprakashdwivedi/spark-optimizer-skill.git
cp -r spark-optimizer-skill/spark-optimizer ~/.claude/skills/
```
To make it available in one repo only, copy it into `<repo>/.claude/skills/` instead.

**Claude.ai / Claude Desktop**:
1. Zip the `spark-optimizer` folder.
2. Upload it under *Settings → Capabilities → Skills*.

**Claude API / Agent SDK**: upload the folder as a custom skill (see the Agent Skills docs).

## Use
In Claude:
> Here's the event log for last night's job: `~/Downloads/application_1700000000000_0042`. Our nodes have 16 cores and 64 GB, and there are 6 of them. Make it faster.

Or run the analyzer directly:
```bash
python3 spark-optimizer/scripts/analyze_spark_log.py <event-log or folder> [stderr logs...] \
    --node-cores 16 --node-mem-gb 64 --nodes 6        # optional: exact executor sizing
    [--target-partition-mb 128] [--json] [--out report.md]
```

### Getting the logs
| Source | How |
|---|---|
| Spark History Server | open the app, then click **Download** to get the event log |
| `spark.eventLog.dir` | e.g. `hdfs:///spark-logs/`, `s3://…/spark-events/`, `dbfs:/…`. Rolling logs live in an `eventlog_v2_<appId>/` folder, which you pass as a whole |
| YARN | `yarn logs -applicationId <appId> > app.log` |
| Kubernetes | `kubectl logs <driver-pod>`, plus executor pods |
| EMR / Dataproc / Databricks | the cluster's log folder (stderr and the event log) |

The analyzer reads `.gz` files directly. Decompress `.zstd` or `.lz4` files first (`zstd -d`, `lz4 -d`).

## Layout
```
spark-optimizer/            the skill (copy this folder)
  SKILL.md                  instructions Claude follows
  scripts/analyze_spark_log.py
  references/tuning-rules.md  thresholds, formulas and the reasoning behind each rule
examples/                   synthetic event log + stderr + generated report
tests/                      unittest suite (python3 -m unittest discover -s tests)
```

## Privacy
Everything runs locally. When the analyzer prints your current config, it leaves out any setting whose name contains
`password`, `secret`, `token` or `key`.

## Limits
- The thresholds are good starting points, not laws. Validate each change on a real run.
- Text logs alone give signals, not metrics. Use the event log for proper sizing.
- On Databricks, executor sizing is managed for you, so focus on the SQL, AQE and cluster or instance-type recommendations.

## License
MIT
