# Task 5: 格式检查、产物校验与 22 算例性能基准对比

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完成 Scala/C++ 代码格式化与 License Header 检查，校验 Native 与 Maven 产物一致性，并使用 `generator.json` 单进程运行全部 22 个 Generator 基准算例，对比 `SparkUnnest` 的性能收益。

**Architecture:** 通过 `dev/run-expression-bench.sh --skip-build --config` 在单个进程中测量全部 22 个 Generator 算例（`4M rows, batchSize=10240, warmup=10s, measurement=50s`，无 profiler），与无优化基线（Velox `0a477fa80`）同口径对比。

**Tech Stack:** Bash, Maven, Spotless, `clang-format-15`.

---

### Files

- Verify: `cpp/velox/operators/plannodes/SparkUnnest.h`
- Verify: `cpp/velox/operators/plannodes/SparkUnnest.cc`
- Verify: `cpp/velox/tests/SparkUnnestTest.cc`
- Verify: `cpp/velox/substrait/SubstraitToVeloxPlan.cc`
- Verify: `cpp/velox/compute/VeloxBackend.cc`
- Verify: `cpp/velox/compute/VeloxPlanEvaluator.cc`
- Verify: `backends-velox/src/main/scala/org/apache/gluten/execution/GenerateExecTransformer.scala`

---

- [ ] **Step 1: 运行 C++ 格式、Scala 格式与 License Header 检查**

```bash
clang-format-15 -i \
  cpp/velox/operators/plannodes/SparkUnnest.h \
  cpp/velox/operators/plannodes/SparkUnnest.cc \
  cpp/velox/tests/SparkUnnestTest.cc \
  cpp/velox/substrait/SubstraitToVeloxPlan.cc \
  cpp/velox/compute/VeloxBackend.cc \
  cpp/velox/compute/VeloxPlanEvaluator.cc
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./build/mvn \
  -Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta,spark-ut \
  -pl backends-velox,gluten-ut/spark41 spotless:apply
./.github/workflows/util/check.py header main --fix
```
Expected: 无格式差异与 license header 报错。

- [ ] **Step 2: 校验 `libvelox.so` 与 `setup-ubuntu.sh` SHA256 一致性**

```bash
sha256sum cpp/build/releases/libvelox.so backends-velox/target/scala-2.13/classes/linux/amd64/libvelox.so
sha256sum ep/build-velox/build/velox_ep/scripts/setup-ubuntu.sh
```
Expected: 两处 `libvelox.so` SHA256 完全相同；`setup-ubuntu.sh` 保持 `d8eee9f5ce8ba14a7054226fd2adb07579cb1b7d727b09e821dfd5c0f2e2cd4c`。

- [ ] **Step 3: 单进程运行全部 22 个 Generator 基准算例**

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1
./dev/run-expression-bench.sh --skip-build \
  --config /tmp/generator-bench-0f167cee5-velox-0a477fa80-20261028/generator.json
```
Expected: 22 个算例全部完成，按 `JVM/Native` 升序输出性能对比表。
