# Task 4: 一步打通 Substrait C++ 计划转换与 Scala `GenerateExecTransformer`（消除 `post-project`）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 C++ 侧 `SubstraitToVeloxPlan.cc` / `VeloxPlanEvaluator.cc` 与 Scala 侧 `GenerateExecTransformer.scala` 同步切换到 `SparkUnnestNode`，一步到位消除 `PullOutGenerateProjectHelper.pullOutPostProject` 中的所有后置 `ProjectExec`，并通过 C++ 与 Scala 端到端测试验证。

**Architecture:** 因为 C++ `SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::GenerateRel&)` 切换到 `SparkUnnestNode` 后直接输出 Spark 最终列顺序（`[replicated, optional 0-based INT pos, unnested/inlined columns]`，不再有 1-based `BIGINT ordinal` 和末尾 `isPresent` 列），它与 Scala 侧移除 `pullOutPostProject` 必须在同一步原子完成，保证两端 Schema 严格对齐。

**Tech Stack:** C++17, Scala 2.13, Substrait, Spark 4.1, ScalaTest.

---

### Files

- Modify: `cpp/velox/substrait/SubstraitToVeloxPlan.cc:1001-1115`
- Modify: `cpp/velox/compute/VeloxPlanEvaluator.cc:125-185, 295-340`
- Modify: `backends-velox/src/main/scala/org/apache/gluten/execution/GenerateExecTransformer.scala:98-156, 270-340`
- Modify: `gluten-ut/spark41/src/test/scala/org/apache/spark/sql/GlutenGeneratorFunctionSuite.scala`

---

- [ ] **Step 1: 在 `GenerateExecTransformer.scala` 中传入 `isInline` 并移除 `pullOutPostProject` 后置投影**

1. 在 `GenerateExecTransformer.getExtensionNode` 中写入 `isInline=`：
   ```scala
   val isInline =
     generator.isInstanceOf[Inline] || generator.isInstanceOf[JsonTupleExplode]
   parametersStr
     .append("isInline=")
     .append(if (isInline) "1" else "0")
     .append("\n")
   ```
2. 将 `PullOutGenerateProjectHelper.pullOutPostProject` 简化为直接返回 `generate`：
   ```scala
   def pullOutPostProject(generate: GenerateExec): SparkPlan = generate
   ```

- [ ] **Step 2: 在 `SubstraitToVeloxPlan.cc` 中将 `toVeloxPlan(const ::substrait::GenerateRel&)` 切换为 `SparkUnnestNode`**

解析 `isPosExplode=`、`isOuter=`、`isInline=`，按 `[replicated, optional INTEGER pos, unnested/inlined value columns]` 构造 `outputType`，并返回 `std::make_shared<SparkUnnestNode>(...)`。

- [ ] **Step 3: 在 `VeloxPlanEvaluator.cc` 中支持 `SparkUnnestNode`**

1. 在构造函数 stage 白名单与 `evaluate()` 分发中加入 `std::dynamic_pointer_cast<const SparkUnnestNode>(node)`；
2. 将 `UnnestOutput` 的算子指针类型从 `exec::Unnest*` 泛化为 `exec::Operator*`（通过 `driver_->findOperator(plan->id())` 获取），同时兼容 `SparkUnnestNode` 与 `core::UnnestNode`。

- [ ] **Step 4: 重新编译 Native 库、同步 Maven 产物并运行 C++ 与 Scala 单元测试**

```bash
export VCPKG_ROOT=/home/chang/SourceCode/gluten2/dev/vcpkg/.vcpkg
export VCPKG_TRIPLET=x64-linux-avx
export VCPKG_MANIFEST_DIR=/home/chang/SourceCode/gluten2/dev/vcpkg
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1

cmake --build cpp/build -j 4 --target velox spark_unnest_test
./cpp/build/velox/tests/spark_unnest_test

./build/mvn -Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta,spark-ut,fast-build \
  -pl backends-velox,gluten-ut/spark41 -am install -DskipTests

./dev/run-scala-test.sh \
  -Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta,spark-ut \
  -pl gluten-ut/spark41 \
  --jvm-arg -Dspark.sql.ansi.enabled=false \
  -s org.apache.spark.sql.GlutenGeneratorFunctionSuite

./dev/run-scala-test.sh \
  -Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta \
  -pl backends-velox \
  -s org.apache.spark.sql.execution.benchmark.expression.CatalogSuite \
  -s org.apache.spark.sql.execution.benchmark.expression.RunnerSuite \
  -s org.apache.spark.sql.execution.benchmark.expression.BenchmarkSuite \
  -s org.apache.spark.sql.execution.benchmark.expression.ExpressionEvaluatorSuite
```
Expected: C++ `spark_unnest_test`、Scala `GlutenGeneratorFunctionSuite`（35+ passed）与 Benchmark UT（354 passed）全部通过。
