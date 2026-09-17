---
layout: page
title: Micro Benchmarks for Velox Backend
nav_order: 3
parent: Developer Overview
---

# Generate Micro Benchmarks for Velox Backend

**This document explains how to use the existing micro benchmark template in Gluten Cpp.**

A micro benchmark for Velox backend is provided in Gluten Cpp to simulate the execution of a first
or middle stage in Spark. It serves as a more convenient alternative to debug in Gluten Cpp
comparing with directly debugging in a Spark job. Developers can use it to create their own
workloads, debug in native process, profile the hotspot and do optimizations.

To simulate a first stage, you need to dump the Substrait plan and input split info into two JSON
files. The input URIs of the splits should be existing file locations, which can be either local or
HDFS paths.

To simulate a middle stage, in addition to the JSON file, you also need to save the input data of
this stage into Parquet files. The benchmark will load the data into Arrow format, then add
Arrow2Velox to feed the data into Velox pipeline to reproduce the reducer stage. Shuffle exchange is
not included.

Please refer to the sections below to learn how to dump the Substrait plan and create the input data
files.

## Direct columnar expression checks

`NativeExpressionEvalHelper` in the `backends-velox` test sources extends Spark's
`ExpressionEvalHelper` with a columnar native path:

```scala
evaluateWithNative(expression, inputBatch, inputAttributes)
checkEvaluationWithNative(expression, expectedValues, inputBatch, inputAttributes)
prepareNativeExpression(Seq(expression), inputAttributes)
```

Preparation accepts multiple input attributes but exactly one output expression;
empty or multiple output sequences fail before native allocation.
The input is an already-offloaded `ColumnarBatch`, not an `InternalRow`.
`inputAttributes` supplies names, types and nullability because native batch handles
do not expose Spark column metadata. The evaluator borrows the input and returns a
single-column native `ColumnarBatch` with the same row count, including empty input.
The caller closes the output. The check helper compares every result with the
corresponding expected Catalyst value using `VeloxInputBatch` and Spark's
`checkResult`, then closes both the read view and the output. No Arrow conversion
is needed to inspect results. Encoded results use a batch-owned flat read view
when needed for comparison; this materialization is outside expression evaluation.

Initialize the native backend with `MockVeloxBackend.initialize()`, then create inputs, evaluate and consume
outputs inside the same `TaskResources.runUnsafe` scope. This provides native
memory/resource management without creating a `SparkContext`, `SparkSession`,
DataFrame or Spark job. The input remains caller-owned and must stay open until
evaluation finishes. The output remains valid after the evaluator is closed, within
the enclosing resource scope. Unsupported expressions fail instead of using Spark fallback.
Inputs may come from a different named Gluten runtime within that scope; compiled
evaluator handles remain bound to the runtime that compiled them.

Generate a string input directly with
`org.apache.gluten.velox.vector.VeloxWritableColumnVector`: allocate the row count,
fill `putByteArray`/`putNull`, then call `finishStringColumn`. Assemble the column
owner handles with `VeloxOutputBatchJniWrapper.makeVeloxBatch`, using the same
attribute names as the expression schema, and wrap the result with
`ColumnarBatches.create`. The assembled batch owns shared references to the
vectors, so close the writable columns after assembly. No Arrow intermediate is
needed. Keep this allocation and data filling outside the evaluation loop.

The implementation serializes Substrait's standard `ExtendedExpression` and calls
Gluten's `VeloxExpressionEvaluatorJniWrapper`. Native compilation uses the existing
Substrait expression converter and Velox `ExprSet`; execution evaluates the input
vectors directly, with no ReadRel, Project operator, query task, scan or hash guard.
For repeated execution, prepare once, call `prepared.evaluate(inputBatch)` for each
batch, close every output, and finally close the prepared evaluator. The evaluator
is not thread-safe. Keep generation, Arrow-to-Velox conversion, compilation and
correctness assertions outside the timed loop. JNI and result ownership costs
remain part of this entry point; it is not a bare C++ function timer.

After building the Velox backend and its native libraries, run the focused suite:

```shell
./build/mvn -P 'java-17,backends-velox,spark-4.1,scala-2.13,delta,!scalatest-single-suite-from-surefire-test' \
  -pl gluten-velox-vector,backends-velox -Dtest=none -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.spark.sql.catalyst.expressions.NativeExpressionEvalHelperSuite \
  test
```

## Direct expression performance benchmarks

`RTrimExpressionBenchmark` and `LTrimExpressionBenchmark` use Spark's standard
`Benchmark.addCase` and `run`, with separate catalogs and input generators.
Vanilla evaluates stable `UnsafeRow` inputs with the requested expression's
`child.genCode` inside Spark's standard `Predicate.create` wrapper. The Boolean
control result is not the expression result. Both engines consume each computed
String's **byte length**, with **NULL=-1** and **empty=0**, into a signed 64-bit sum
reset and published once per full pass. This signature is not a content hash or
correctness proof. The JVM timed terminal passes only a primitive length to its
accumulator; it does not publish or return the String reference. Native keeps the
real `ExprSet` result vector and uses one additional JNI call per live batch to
consume `StringView` lengths before the existing output close. Encoded results
are read through native decoding, without payload copying or a new result vector.

Full content/reference checks and `UnsafeProjection` input encoding/oracle are
untimed. The original String expression is unchanged: it is not replaced with a
SQL length expression. This measures the expression plus result-property
consumption, not full String-production latency or pure trim time. Materialization
that the JIT can legally eliminate while computing this property is part of the
protocol; retained engine work and allocation still require separate profile
proof. Previous final-projection, reference-publication and handwritten-evaluator
results use different protocols and must not be combined as one performance run.

See the Scala entrypoints for Java17, offline full-reactor build/classpath setup,
case selection, engine order and reproducible invocation examples. Arguments are
`rows [native-batch-size [seed [case-name [engine [order]]]]]`, with defaults
10240, 20260912, both, vanilla-first. Engine selection only controls timed Spark
cases; every mode checks the actual JVM terminal and native results against the
untimed oracle. Spark owns warmup and iteration timing and prints every measured
iteration. No Python benchmark launcher or custom timing/fork framework is used.

### Aligned trim coverage

Each independent RTrim/LTrim catalog has **41 cases**, in the same coverage order:

| Coverage | Cases per function |
|---|---:|
| Byte widths10/12/13/64/256 × none/first/cluster/half-even/last/penultimate/all |35|
| 64-byte NULL50 and UTF8-half-even |2|
| 10/256-byte identity controls |2|
| Opposite-side preservation boundary and two-input custom trim |2|

Positions describe rows within the **actual native batch**, not the side of the
string being trimmed. `first` selects row0, `cluster` the first `min(16,count)`
rows, `half-even` even rows, and `last` the actual last row, including a partial
batch. `penultimate` swaps the complete last/penultimate rows and their identities;
a singleton stays unchanged. Selected strings have two target spaces. Input byte
widths, seeded eight-hex row identities, bodies, NULL positions and UTF8 rules
match across functions; RTrim places target spaces after the body, LTrim before it.
LTrim generates its data independently, using body-relative offsets rather than
reversing UTF8 or calling RTrim's generator.

Both standalone entrypoints accept `aligned` (all41), `representative` (the
corresponding12) or a case ID. Omitting the selector runs all41. The historical
RTrim12 IDs, byte fingerprints and representative ordering remain unchanged.
The old LTrim128-row groups, injected regular-grid empty strings and
`ltrim-pattern-lN` IDs are replaced by batch-local `ltrim-lN-pattern` definitions;
old measurements must not be combined with the aligned dataset. Empty/all-space
semantics remain covered in UTs. The opposite-boundary and custom cases retain
matching joint inputs and test preservation at the other string end.

### Three-process combined run

`TrimExpressionBenchmark` copies only result labels from the two independent
catalogs, with `rtrim/` and `ltrim/` namespaces, then calls the shared runner once.
It runs RTrim41 followed by LTrim41, releasing each case before the next and
shutting down native state once after all82. Qualified identity labels avoid
collisions; for example `ltrim/identity-l10` selects one case.

Run three sequential fresh JVMs using the commands in that Scala source:
Vanilla with A's JVM classpath, NativeA, then NativeB. Each uses the `aligned`
selector and only its `vanilla` or `native` timed engine. With4,000,000rows per
case, batch10240 and seed20260912 this is **3 processes ×82 cases =246 result
groups**, not246 processes or multiple independent forks. Use identical Java17,
heap/GC/thread settings and CPU affinity, no concurrent builds, and a distinct
output cwd per process with `SPARK_GENERATE_BENCHMARK_FILES=1`. Preserve complete
commands, source/diff/jar/native identities, preflight records, raw iterations,
standard results and exits; do not discard slow samples or subtract identity time.

The uniform native version contrast is A/Velox752d before both optimizations and
B/Veloxab79 after RTrim and unary LTrim optimization. The earlier LTrim comparison
used6bec as its baseline and remains a separate experiment. Uncommitted benchmark
changes require base-commit plus diff provenance, not a clean-artifact claim.
One process per engine measures within-process iteration variation only; it does
not provide cross-process confidence intervals or per-case cold-start isolation.

## Try the example

To run a micro benchmark, user should provide one file that contains the Substrait plan in JSON
format, and optional one or more input data files in parquet format. The commands below help to
generate example input files:

```shell
cd /path/to/gluten/
./dev/buildbundle-veloxbe.sh --build_tests=ON --build_benchmarks=ON

# Run test to generate input data files. If you are using spark 3.4, replace -Pspark-3.5 with -Pspark-3.4.
mvn test -Pspark-3.5 -Pbackends-velox -pl backends-velox -am \
-DtagsToInclude="org.apache.gluten.tags.GenerateExample" -Dtest=none -DfailIfNoTests=false -Dexec.skip
```

The generated example files are placed in gluten/backends-velox:

- stageId means Spark stage id in web ui.
- partitionedId means Spark partition id in web stage ui.
- vId means backends internal virtual id.

```shell
$ tree gluten/backends-velox/generated-native-benchmark/
gluten/backends-velox/generated-native-benchmark/
├── conf_12_10_3.ini
├── data_12_10_3_0.parquet
├── data_12_10_3_1.parquet
├── plan_12_10_3.json
```

Run micro benchmark with the generated files as input. You need to specify the **absolute** path to
the input files:

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan <path-to-gluten>/backends-velox/generated-native-benchmark/plan_{stageId}_{partitionId}_{vId}.json \
--data <path-to-gluten>/backends-velox/generated-native-benchmark/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet,\
<path-to-gluten>/backends-velox/generated-native-benchmark/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet \
--conf <path-to-gluten>/backends-velox/generated-native-benchmark/conf_{stageId}_{partitionId}_{vId}.ini \
--threads 1 --iterations 1 --noprint-result
```

The output should be like:

```shell
2022-11-18T16:49:56+08:00
Running ./generic_benchmark
Run on (192 X 3800 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x96)
  L1 Instruction 32 KiB (x96)
  L2 Unified 2048 KiB (x96)
  L3 Unified 99840 KiB (x2)
Load Average: 0.28, 1.17, 1.59
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
-- Project[expressions: (n3_0:BIGINT, ROW["n1_0"]), (n3_1:VARCHAR, ROW["n1_1"])] -> n3_0:BIGINT, n3_1:VARCHAR
   Output: 535 rows (65.81KB, 1 batches), Cpu time: 36.33us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 3, Threads: 1
      queuedWallNanos    sum: 2.00us, count: 2, min: 0ns, max: 2.00us
  -- HashJoin[RIGHT SEMI (FILTER) n0_0=n1_0] -> n1_0:BIGINT, n1_1:VARCHAR
     Output: 535 rows (65.81KB, 1 batches), Cpu time: 191.56us, Blocked wall time: 0ns, Peak memory: 2.00MB, Memory allocations: 8
     HashBuild: Input: 582 rows (16.45KB, 1 batches), Output: 0 rows (0B, 0 batches), Cpu time: 1.84us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 3, Threads: 1
        distinctKey0       sum: 583, count: 1, min: 583, max: 583
        queuedWallNanos    sum: 0ns, count: 1, min: 0ns, max: 0ns
        rangeKey0          sum: 59748, count: 1, min: 59748, max: 59748
     HashProbe: Input: 37897 rows (296.07KB, 1 batches), Output: 535 rows (65.81KB, 1 batches), Cpu time: 189.71us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 5, Threads: 1
        queuedWallNanos    sum: 0ns, count: 1, min: 0ns, max: 0ns
    -- ArrowStream[] -> n0_0:BIGINT
       Input: 0 rows (0B, 0 batches), Output: 37897 rows (296.07KB, 1 batches), Cpu time: 1.29ms, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1
    -- ArrowStream[] -> n1_0:BIGINT, n1_1:VARCHAR
       Input: 0 rows (0B, 0 batches), Output: 582 rows (16.45KB, 1 batches), Cpu time: 894.22us, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1

-----------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                   Time             CPU   Iterations UserCounters...
-----------------------------------------------------------------------------------------------------------------------------
InputFromBatchVector/iterations:1/process_time/real_time/threads:1   41304520 ns     23740340 ns            1 collect_batch_time=34.7812M elapsed_time=41.3113M

```

## Generate Substrait plan and input for any query

First, build Gluten with `--build_benchmarks=ON`.

```shell
cd /path/to/gluten/
./dev/buildbundle-veloxbe.sh --build_benchmarks=ON

# For debugging purpose, rebuild Gluten with build type `Debug`.
./dev/buildbundle-veloxbe.sh --build_benchmarks=ON --build_type=Debug
```

First, get the Stage Id from spark UI for the stage you want to simulate.
And then re-run the query with below configurations to dump the inputs to micro benchmark.

| Parameters                                  | Description                                                                                                                                                                                                                                                                                                                                 | Recommend Setting                                          |
|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| spark.gluten.sql.benchmark_task.taskId      | Comma-separated string to specify the Task IDs to dump. If it's set, `spark.gluten.sql.benchmark_task.stageId` and `spark.gluten.sql.benchmark_task.partitionId` will be ignored.                                                                                                                                                           | Comma-separated string of task IDs. Empty by default.      |
| spark.gluten.sql.benchmark_task.stageId     | Spark stage ID.                                                                                                                                                                                                                                                                                                                             | Target stage ID                                            |
| spark.gluten.sql.benchmark_task.partitionId | Comma-separated string to specify the Partition IDs in a stage to dump. Must be specified together with `spark.gluten.sql.benchmark_task.stageId`. Empty by default, meaning all partitions of this stage will be dumped. To identify the partition ID, navigate to the `Stage` tab in the Spark UI and locate it under the `Index` column. | Comma-separated string of partition IDs. Empty by default. |
| spark.gluten.saveDir                        | Directory to save the inputs to micro benchmark, should exist and be empty.                                                                                                                                                                                                                                                                 | /path/to/saveDir                                           |

Check the files in `spark.gluten.saveDir`. If the simulated stage is a first stage, you will get 3
or 4 types of dumped file:

- Configuration file: INI formatted, file name `conf_{stageId}_{partitionId}_{vId}.ini`. Contains the
  configurations to init Velox backend and runtime session.
- Plan file: JSON formatted, file name `plan_{stageId}_{partitionId}_{vId}.json`. Contains the substrait
  plan to the stage, without input file splits.
- Split file: JSON formatted, file name `split_{stageId}_{partitionId}_{vId}_{splitIdx}.json`. There can
  be more than one split file in a first stage task. Contains the substrait plan piece to the input
  file splits.
- Data file(optional): Parquet formatted, file
  name `data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet`. If the first stage contains one or
  more BHJ operators, there can be one or more input data files. The input data files of a first
  stage will be loaded as iterators to serve as the inputs for the pipeline:

```
"localFiles": {
  "items": [
    {
      "uriFile": "iterator:0"
    }
  ]
}
```

Run benchmark. By default, the result will be printed to stdout. You can use `--noprint-result` to
suppress this output.

Sample command:

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--conf /absolute_path/to/conf_{stageId}_{partitionId}_{vId}.ini \
--plan /absolute_path/to/plan_{stageId}_{partitionId}_{vId}.json \
--split /absolut_path/to/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json,/absolut_path/to/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json \
--threads 1 --noprint-result

# If the stage requires data files, use --data-file to specify the absolute path.
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--conf /absolute_path/to/conf_{stageId}_{partitionId}_{vId}.ini \
--plan /absolute_path/to/plan_{stageId}_{partitionId}_{vId}.json \
--split /absolut_path/to/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json,/absolut_path/to/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json \
--data /absolut_path/to/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet,/absolut_path/to/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet \
--threads 1 --noprint-result
```

If the simulated stage is a middle stage, which means pure shuffle stage, you will get 3 types of
dumped file:

- Configuration file: INI formatted, file name `conf_{stageId}_{partitionId}_{vId}.ini`. Contains the
  configurations to init Velox backend and runtime session.
- Plan file: JSON formatted, file name `plan_{stageId}_{partitionId}_{vId}.json`. Contains the substrait
  plan to the stage.
- Data file: Parquet formatted, file name `data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet`.
  There can be more than one input data file in a middle stage task. The input data files of a
  middle stage will be loaded as iterators to serve as the inputs for the pipeline:

```
"localFiles": {
  "items": [
    {
      "uriFile": "iterator:0"
    }
  ]
}
```

Sample command:

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--conf /absolute_path/to/conf_{stageId}_{partitionId}_{vId}.ini \
--plan /absolute_path/to/plan_{stageId}_{partitionId}_{vId}.json \
--data /absolut_path/to/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet,/absolut_path/to/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet \
--threads 1 --noprint-result
```

For some complex queries, stageId may cannot represent the Substrait plan input, please get the
taskId from spark UI, and get your target parquet from saveDir.

In this example, only one partition input with stage id 3 partition id 2, partitionId is 36, iterator length is 2.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan /absolute_path/to/plan_3_36_0.json \
--data /absolute_path/to/data_3_36_0_0.parquet,/absolute_path/to/data_3_36_0_1.parquet \
--threads 1 --noprint-result
```

## Save output to parquet for analysis

You can save the output to a parquet file via `--save-output <output>`

Note: 1. This option cannot be used together with `--with-shuffle`. 2. This option cannot be used
for write tasks. Please refer to section [Simulate write tasks](#simulate-write-tasks) for more
details.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan /absolute_path/to/plan_{stageId}_{partitionId}_{vId}.json \
--data /absolute_path/to/data_{stageId}_{partitionId}_{vId}_{iteratorIdx}.parquet
--threads 1 --noprint-result --save-output /absolute_path/to/result.parquet
```

## Add shuffle write process

You can add the shuffle write process at the end of the pipeline via `--with-shuffle`

Note: 1. This option cannot be used together with `--save-output`. 2. This option cannot be used
for write tasks. Please refer to section [Simulate write tasks](#simulate-write-tasks) for more
details.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan /absolute_path/to/plan_{stageId}_{partitionId}_{vId}.json \
--split /absolute_path/to/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json \
--threads 1 --noprint-result --with-shuffle
```

Developers can leverage the `--with-shuffle` option to benchmark the shuffle-write process by creating
a simple pipeline of `table scan + shuffle write` in Gluten. This can be done by dumping the micro benchmark
inputs from a first stage. The steps are demonstrated as below:

1. Start spark-shell or pyspark

We need to set `spark.gluten.sql.benchmark_task.stageId` and `spark.gluten.saveDir` to dump the inputs.
Normally, the stage id should be greater than 0. You can run the command in step 2 in advance to get the
right stage id in your case. We shall set `spark.default.parallelism` to 1 and `spark.sql.files.maxPartitionBytes`
large enough to make sure there will be only 1 task in the first stage.

```
# Start pyspark
./bin/pyspark --master local[*] \
--conf spark.gluten.sql.benchmark_task.stageId=1 \
--conf spark.gluten.saveDir=/path/to/saveDir \
--conf spark.default.parallelism=1 \
--conf spark.sql.files.maxPartitionBytes=10g
... # omit other spark & gluten config
```

2. Run the table-scan command to dump the plan for the first stage

If simulating single or round-robin partitioning, the first stage can only have the table scan operator.

```
>>> spark.read.format("parquet").load("file:///example.parquet").show()
```

If simulating hash partitioning, there will be a projection for generating the hash partitioning key.
Therefore we need to explicitly run the `repartition` to generate the `scan + project` pipeline for the first stage.
Note that using different number of shuffle partitions here doesn't change the generated pipeline.

```
>>> spark.read.format("parquet").load("file:///example.parquet").repartition(10, "key1", "key2").show()
```

Simuating range partitioning is not supported.

3. Run the micro benchmark with dumped inputs

General configurations for shuffle write:

- `--with-shuffle`: Add shuffle write process at the end of the pipeline
- `--shuffle-writer`: Specify shuffle writer type. Valid options are sort and hash. Default is hash.
- `--partitioning`: Specify partitioning type. Valid options are rr, hash and single. Defualt is rr.
                    The partitioning type should match the command in step 2.
- `--shuffle-partitions`: Specify number of shuffle partitions.
- `--compression`: By default, the compression codec for shuffle outputs is lz4. You can switch to other compression codecs
  or use hardware accelerators Valid options are: lz4, zstd, qat-gzip, qat-zstd and iaa-gzip. The compression levels are fixed (use default compression level 1).

  Note using QAT or IAA codec requires Gluten cpp is built with these features.
  Please check the corresponding section in [Velox document](../get-started/Velox.md) first for how to
  setup, build and enable these features in Gluten. For QAT support, please
  check [Intel® QuickAssist Technology (QAT) support](../get-started/Velox.md#intel-quickassist-technology-qat-support).
  For IAA support, please
  check [Intel® In-memory Analytics Accelerator (IAA/IAX) support](../get-started/Velox.md#intel-in-memory-analytics-accelerator-iaaiax-support)

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan /path/to/saveDir/plan_{stageId}_{partitionId}_{vId}.json \
--conf /path/to/saveDir/conf_{stageId}_{partitionId}_{vId}.ini \
--split /path/to/saveDir/split_{stageId}_{partitionId}_{vId}_{splitIdx}.json \
--with-shuffle \
--shuffle-writer sort \
--partitioning hash \
--threads 1
```

### Run shuffle write/read task only

Developers can only run shuffle write task via specifying `--run-shuffle` and `--data` options.
The parquet format input will be read from arrow-parquet reader and sent to shuffle writer.
The `--run-shuffle` option is similar to the `--with-shuffle` option, but it doesn't require the plan and split files.
The round-robin partitioner is used by default. Besides, random partitioning can be used for testing purpose.
By specifying option `--partitioning random`, the partitioner will generate a random partition id for each row.
To evaluate the shuffle reader performance, developers can set `--run-shuffle-read` option to add read process after the write task finishes.

The below command will run shuffle write/read in single thread, using sort shuffle writer with 40000 partitions and random partition id.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--run-shuffle \
--run-shuffle-read \
--data /path/to/input_for_shuffle_write.parquet
--shuffle-writer sort \
--partitioning random \
--shuffle-partitions 40000 \
--threads 1
```

The output should be like:

```
-------------------------------------------------------------------------------------------------------------------------
Benchmark                                                               Time             CPU   Iterations UserCounters...
-------------------------------------------------------------------------------------------------------------------------
ShuffleWriteRead/iterations:1/process_time/real_time/threads:1 121637629714 ns   121309450910 ns            1 elapsed_time=121.638G read_input_time=25.2637G shuffle_compress_time=10.8311G shuffle_decompress_time=4.04055G shuffle_deserialize_time=7.24289G shuffle_spill_time=0 shuffle_split_time=69.9098G shuffle_write_time=2.03274G
```

## Enable debug mode

`spark.gluten.sql.debug`(debug mode) is set to false by default thereby the google glog levels are limited to only print `WARNING` or higher severity logs.
Unless `spark.gluten.sql.debug` is set in the INI file via `--conf`, the logging behavior is same as debug mode off.
Developers can use `--debug-mode` command line flag to turn on debug mode when needed, and set verbosity/severity level via command line flags `--v` and `--minloglevel`. Note that constructing and deconstructing log strings can be very time-consuming, which may cause benchmark times to be inaccurate.


## Enable HDFS support

After enabling the dynamic loading of libhdfs.so at runtime to support HDFS, if you run the benchmark with an HDFS file, you need to set the classpath for Hadoop. You can do this by running
```
export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
```
Otherwise, the HDFS connection will fail. If you have replaced ${HADOOP_HOME}/lib/native/libhdfs.so with libhdfs3.so, there is no need to set the `CLASSPATH`.

## Simulate write tasks

The last operator for a write task is a file write operator, and the output from Velox pipeline only
contains several columns of statistics data. Therefore, specifying
options `--with-shuffle` and `--save-output` does not take effect. You can specify the output path
for the writer via `--write-path` option. Default is /tmp.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
--plan /absolute_path/to/plan.json \
--split /absolute_path/to/split.json \
--write-path /absolute_path/<dir>
```

## Simulate task spilling

You can simulate task spilling by specify a memory hard limit from `--memory_limit`. By default, spilled files are written to the `/tmp` directory.
To simulate real Gluten workloads, which utilize multiple spill directories, set the environment variable GLUTEN_SPARK_LOCAL_DIRS to a comma-separated string.
Please check [Simulate Gluten workload with multiple processes and threads](#Simulate-Gluten-workload-with-multiple-processes-and-threads) for more details.

## Simulate Gluten workload with multiple processes and threads

You can use below command to launch several processes and threads to simulate parallel execution on
Spark. Each thread in the same process will be pinned to the core number starting from `--cpu`.

Suppose running on a bare-metal machine with 48C, 2-socket, HT-on, launching below command will
utilize all vcores.

```shell
processes=24 # Same value of spark.executor.instances
threads=8 # Same value of spark.executor.cores

for ((i=0; i<${processes}; i++)); do
    ./generic_benchmark --plan /path/to/plan.json --split /path/to/split.json --noprint-result --threads $threads --cpu $((i*threads)) &
done
```

To include the shuffle write process or trigger spilling via `--memory-limit`,
you can specify multiple directories by setting the `GLUTEN_SPARK_LOCAL_DIRS` environment variable
to a comma-separated string. This will distribute the I/O load across multiple disks, similar to how it works for Gluten workloads.
Temporary subdirectories will be created under each specified directory at runtime and will be automatically deleted if the process completes normally.

```shell
mkdir -p {/data1,/data2,/data3}/tmp # Make sure each directory has been already created.
export GLUTEN_SPARK_LOCAL_DIRS=/data1/tmp,/data2/tmp,/data3/tmp

processes=24 # Same value of spark.executor.instances
threads=8 # Same value of spark.executor.cores

for ((i=0; i<${processes}; i++)); do
    ./generic_benchmark --plan /path/to/plan.json --split /path/to/split.json --noprint-result --with-shuffle --threads $threads --cpu $((i*threads)) &
done
```

### Run Examples

We also provide some example inputs in [cpp/velox/benchmarks/data](../../cpp/velox/benchmarks/data).
E.g. Files under [generic_q5](../../cpp/velox/benchmarks/data/generic_q5) simulates a first-stage in
TPCH Q5, which has a heavy table scan. You can follow below steps to run this example.

1.

Open [generic_q5/q5_first_stage_0_split.json](../../cpp/velox/benchmarks/data/generic_q5/q5_first_stage_0_split.json)
with file editor. Search for `"uriFile": "LINEITEM"` and replace `LINEITEM` with the URI to one
partition file in lineitem. In the next line, replace the number in `"length": "..."` with the
actual file length. Suppose you are using the provided small TPCH table
in [cpp/velox/benchmarks/data/tpch_sf10m](../../cpp/velox/benchmarks/data/tpch_sf10m), the replaced
JSON should be like:

```
{
  "items": [
    {
      "uriFile": "file:///path/to/gluten/cpp/velox/benchmarks/data/tpch_sf10m/lineitem/part-00000-6c374e0a-7d76-401b-8458-a8e31f8ab704-c000.snappy.parquet",
      "length": "1863237",
      "parquet": {}
    }
  ]
}
```

2. Launch multiple processes and multiple threads. Set `GLUTEN_SPARK_LOCAL_DIRS` and add
   `--with-shuffle` to the command.

```
mkdir -p {/data1,/data2,/data3}/tmp # Make sure each directory has been already created.
export GLUTEN_SPARK_LOCAL_DIRS=/data1/tmp,/data2/tmp,/data3/tmp

processes=24 # Same value of spark.executor.instances
threads=8 # Same value of spark.executor.cores

for ((i=0; i<${processes}; i++)); do
    ./generic_benchmark --plan /path/to/gluten/cpp/velox/benchmarks/data/generic_q5/q5_first_stage_0.json --split /path/to/gluten/cpp/velox/benchmarks/data/generic_q5/q5_first_stage_0_split.json --noprint-result --with-shuffle --threads $threads --cpu $((i*threads)) &
done >stdout.log 2>stderr.log
```

You can find the "elapsed_time" and other metrics in stdout.log. In below output, the "elapsed_time"
is ~10.75s. If you run TPCH Q5 with Gluten on Spark, a single task in the same Spark stage should
take about the same time.

```
------------------------------------------------------------------------------------------------------------------
Benchmark                                                        Time             CPU   Iterations UserCounters...
------------------------------------------------------------------------------------------------------------------
SkipInput/iterations:1/process_time/real_time/threads:8 1317255379 ns   10061941861 ns            8 collect_batch_time=0 elapsed_time=10.7563G shuffle_compress_time=4.19964G shuffle_spill_time=0 shuffle_split_time=0 shuffle_write_time=1.91651G
```

![TPCH-Q5-first-stage](../image/TPCH-q5-first-stage.png)
