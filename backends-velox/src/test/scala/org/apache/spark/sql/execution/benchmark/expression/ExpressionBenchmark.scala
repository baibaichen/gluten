/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.RowToVeloxColumnarExec
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.test.MockVeloxBackend
import org.apache.gluten.utils.Arm

import org.apache.spark.SparkFunSuite
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ReplaceExpressions}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.exceptions.TestCanceledException

import java.nio.file.Files
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/** Borrowed result reference; public so Spark's generated Java can call accept. */
final class ExpressionBenchmarkResult {
  var value: UTF8String = _
  def accept(result: UTF8String): Unit = { value = result }
}

/** Shared preparation and execution, not another timing framework. */
private[benchmark] object ExpressionBenchmark {
  val sqlConf: Seq[(String, String)] = Seq(
    SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
    SQLConf.CASE_SENSITIVE.key -> "false",
    SQLConf.ANSI_ENABLED.key -> "true",
    SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
    "spark.sql.alwaysInlineCommonExpr" -> "false"
  )

  /** Preserve Spark's recursive typed/nullability checks, except for unordered map entries. */
  trait TypedComparison extends ExpressionEvalHelper { self: SparkFunSuite =>
    override protected def checkResult(
        result: Any,
        expected: Any,
        dataType: DataType,
        nullable: Boolean): Boolean = (result, expected, dataType) match {
      case (actual: MapData, reference: MapData, MapType(keyType, valueType, valueNullable)) =>
        def key(map: MapData, i: Int): Any = map.keyArray().get(i, keyType)
        def value(map: MapData, i: Int): Any = map.valueArray().get(i, valueType)
        // ponytail: quadratic matching suits these small maps; use typed ordering for large maps.
        Seq(actual, reference).foreach {
          map =>
            require(
              map.keyArray().numElements() == map.valueArray().numElements(),
              "Invalid map entries")
            (0 until map.numElements()).foreach {
              i =>
                require(key(map, i) != null, "Null map key")
                require(
                  !(0 until i).exists(
                    j =>
                      checkResult(key(map, i), key(map, j), keyType, false)),
                  "Duplicate map key")
            }
        }
        actual.numElements() == reference.numElements() &&
        (0 until actual.numElements()).forall {
          i =>
            val j = (0 until reference.numElements()).find(
              j =>
                checkResult(key(actual, i), key(reference, j), keyType, false))
            j.exists(
              j => checkResult(value(actual, i), value(reference, j), valueType, valueNullable))
        }
      case _ => super.checkResult(result, expected, dataType, nullable)
    }
  }

  trait TypedPrepared {
    def nativeBatchSizes: Seq[Int]

    /** The callback borrows this row's typed JVM value, only until it returns. */
    def verify(result: (Long, Any) => Unit = (_, _) => ()): Unit

    /** Explicit and untimed; the returned actions borrow this callback's native resource scope. */
    def prepareMetric(): PreparedMetric
  }

  final class PreparedMetric private[ExpressionBenchmark] (
      val runVanilla: () => Unit,
      val runNative: () => Unit)

  private def checked[T](scenario: ExpressionBenchmarkCatalog.CaseDef)(f: => T): T =
    try f
    catch {
      case canceled: TestCanceledException => throw canceled
      case scala.util.control.NonFatal(cause) =>
        throw new IllegalArgumentException(
          s"${scenario.sourceLocation} case=${scenario.id}: ${cause.getMessage}",
          cause)
    }

  private def singleProject(plan: LogicalPlan, attributes: Seq[Attribute]): Project = {
    require(plan.resolved, s"Unresolved expression plan: $plan")
    plan match {
      case project @ Project(Seq(alias: Alias), relation: LocalRelation) =>
        require(relation.output == attributes, "Input schema or expression IDs changed")
        require(alias.references.subsetOf(relation.outputSet), "Expression has foreign inputs")
        project
      case _ => throw new IllegalArgumentException(s"Expected one Project(LocalRelation): $plan")
    }
  }

  def prepareAnalyzed(
      spark: SparkSession,
      scenario: ExpressionBenchmarkCatalog.CaseDef,
      schema: StructType,
      native: Boolean): Project = checked(scenario) {
    val relation = LocalRelation(SparkShimLoader.getSparkShims.attributesFromStruct(schema))
    val parsed = spark.sessionState.sqlParser.parseExpression(scenario.sql)
    require(!parsed.exists(_.isInstanceOf[SubqueryExpression]), "Subqueries are not scalar inputs")
    val analyzed = spark.sessionState.executePlan(
      Project(Seq(Alias(parsed, "result")()), relation)).analyzed
    analyzed.foreach(_.expressions.foreach(_.foreach {
      case expression @ (_: AggregateExpression | _: WindowExpression | _: Generator |
          _: SubqueryExpression) =>
        throw new IllegalArgumentException(s"Unsupported scalar expression: $expression")
      case expression =>
        require(expression.deterministic, s"Nondeterministic expression: $expression")
    }))
    val project = singleProject(analyzed, relation.output)
    def nativeReplacement(expression: Expression): Expression = expression match {
      case replaceable: RuntimeReplaceable with InheritAnalysisRules =>
        nativeReplacement(replaceable.replacement)
      // ArraySize is not in the converter map; Size is. Keep Encode and StructsToJson intact.
      case size: ArraySize => nativeReplacement(size.replacement)
      case other => other.mapChildren(nativeReplacement)
    }
    val replaced = if (native) project.mapExpressions(nativeReplacement)
    else ReplaceExpressions.apply(project)
    val rewritten = SparkShimLoader.getSparkShims.rewriteWithExpression(replaced)
    val prepared = singleProject(ConstantFolding(rewritten), relation.output)
    prepared.projectList.foreach(_.foreach {
      case expression: ArraySize =>
        throw new IllegalArgumentException(s"Unprepared expression: $expression")
      case expression: RuntimeReplaceable if !native =>
        throw new IllegalArgumentException(s"Unreplaced JVM expression: $expression")
      case _ =>
    })
    prepared
  }

  def withCorrectness(
      spark: SparkSession,
      scenario: ExpressionBenchmarkCatalog.CaseDef,
      context: ExpressionBenchmarkData.Context,
      batchSize: Int)(f: TypedPrepared => Unit): Unit =
    withCorrectness(
      spark,
      scenario,
      context,
      batchSize,
      ExpressionBenchmarkData.compile(scenario.inputs))(f)

  def withCorrectness(
      spark: SparkSession,
      scenario: ExpressionBenchmarkCatalog.CaseDef,
      context: ExpressionBenchmarkData.Context,
      batchSize: Int,
      data: ExpressionBenchmarkData.Plan)(f: TypedPrepared => Unit): Unit = checked(scenario) {
    require(batchSize > 0, "Batch size must be positive")
    require(context.rows <= Int.MaxValue, "Input rows exceed in-memory array capacity")
    new CorrectnessRunner(spark, scenario, context, batchSize, data).execute(f)
  }

  // No timer, metric consumer or blackhole is prepared on the correctness path.
  private class CorrectnessRunner(
      spark: SparkSession,
      scenario: ExpressionBenchmarkCatalog.CaseDef,
      context: ExpressionBenchmarkData.Context,
      batchSize: Int,
      data: ExpressionBenchmarkData.Plan)
    extends SparkFunSuite
    with NativeExpressionEvalHelper
    with TypedComparison {
    def execute(f: TypedPrepared => Unit): Unit = withSQLConf(sqlConf: _*) {
      TaskResources.runUnsafe {
        def fresh(native: Boolean): (Expression, Seq[Attribute]) = {
          val project = prepareAnalyzed(spark, scenario, data.inputSchema, native)
          (project.projectList.head.asInstanceOf[Alias].child, project.child.output)
        }
        val (jvmExpression, jvmAttributes) = fresh(native = false)
        val vanilla = MutableProjection.create(Seq(jvmExpression), jvmAttributes)
        vanilla.initialize(0)
        val (oracleExpression, oracleAttributes) = fresh(native = false)
        val oracle = UnsafeProjection.create(Seq(oracleExpression), oracleAttributes)
        oracle.initialize(0)
        val (nativeExpression, nativeAttributes) = fresh(native = true)
        require(nativeExpression.dataType == jvmExpression.dataType, "Engine result types differ")
        val native = prepareNativeExpression(Seq(nativeExpression), nativeAttributes)
        val encoder = UnsafeProjection.create(data.inputSchema)
        encoder.initialize(0)
        val inputs = new Array[UnsafeRow](context.rows.toInt)
        var start = 0
        while (start < inputs.length) {
          val count = math.min(batchSize, inputs.length - start)
          var local = 0
          while (local < count) {
            val row = data.row(context, (start + local).toLong, local, count)
            require(row.numFields == data.inputSchema.length, "Input row and schema disagree")
            assert(checkResult(row, row, data.inputSchema, false), "Invalid logical input")
            inputs(start + local) = encoder(row).copy()
            local += 1
          }
          start += count
        }
        val batches = retainedBatches(inputs, data.inputSchema, batchSize)
        // ColumnarBatchRow.get and ColumnarArray.get do not check primitive null bits.
        // Nullable readers preserve those bits; checkResult enforces the original nullability.
        val readInput = UnsafeProjection.create(data.inputSchema.asNullable)
        readInput.initialize(0)
        val readOutput = UnsafeProjection.create(
          new StructType().add("result", jvmExpression.dataType).asNullable)
        readOutput.initialize(0)
        f(new TypedPrepared {
          private val dataType: DataType = jvmExpression.dataType
          private val nullable: Boolean = jvmExpression.nullable
          override def nativeBatchSizes: Seq[Int] = batches.map(_.numRows())
          override def prepareMetric(): PreparedMetric = {
            new PreparedMetric(
              prepareVanillaMetric(() => fresh(native = false), inputs),
              () => {
                var i = 0
                while (i < batches.length) {
                  native.evaluate(batches(i)).close()
                  i += 1
                }
              })
          }
          override def verify(result: (Long, Any) => Unit): Unit = {
            assert(
              nativeBatchSizes == (0 until inputs.length by batchSize).map(
                start =>
                  math.min(batchSize, inputs.length - start)),
              "Native batch sizes differ")
            var offset = 0
            batches.foreach {
              input =>
                withReadableBatch(input) {
                  readable =>
                    assert(readable.numRows() == input.numRows())
                    assert(readable.numCols() == data.inputSchema.length)
                    (0 until input.numRows()).foreach {
                      local =>
                        assert(
                          checkResult(
                            readInput(readable.getRow(local)),
                            inputs(offset + local),
                            data.inputSchema,
                            false),
                          s"Native input differs at ${offset + local}")
                    }
                }
                Arm.withResource(native.evaluate(input)) {
                  output =>
                    assert(output.numCols() == 1, "Native result column count differs")
                    assert(output.numRows() == input.numRows(), "Native result row count differs")
                    withReadableBatch(output) {
                      readable =>
                        (0 until input.numRows()).foreach {
                          local =>
                            val index = offset + local
                            val reference = oracle(inputs(index)).get(0, dataType)
                            val actual = vanilla(inputs(index)).get(0, dataType)
                            assert(
                              checkResult(actual, reference, dataType, nullable),
                              s"JVM result differs at $index")
                            assert(
                              checkResult(
                                readOutput(readable.getRow(local)).get(0, dataType),
                                reference,
                                dataType,
                                nullable),
                              s"Native result differs at $index")
                            result(index.toLong, actual)
                        }
                    }
                }
                offset += input.numRows()
            }
            assert(offset == inputs.length, "Native inputs do not cover every logical row")
          }
        })
      }
    }
  }

  private[benchmark] def prepareVanillaMetric(
      fresh: () => (Expression, Seq[Attribute]),
      inputs: Array[UnsafeRow]): () => Unit = {
    BenchmarkBlackhole.requireEnabled()
    val (expression, attributes) = fresh()
    expression.dataType match {
      case _: StringType =>
        // Borrow the raw generated result: UnsafeProjection would hide off-heap results by copying.
        // This separate predicate and sink never enter the timed action.
        val (validationExpression, validationAttributes) = fresh()
        val validate = prepareVanilla(Seq(validationExpression), validationAttributes)
        var i = 0
        while (i < inputs.length) {
          val result = validate(inputs(i))
          require(
            result == null || result.getBaseObject != null,
            s"Metric requires on-heap UTF8String results; off-heap result at row $i")
          i += 1
        }
      case _ =>
    }
    expression.dataType match {
      case _: ArrayType | _: MapType | _: StructType =>
        val projection = UnsafeProjection.create(Seq(expression), attributes)
        projection.initialize(0)
        () => {
          var i = 0
          while (i < inputs.length) {
            val row = projection(inputs(i))
            BenchmarkBlackhole.consume(
              false,
              row.getBaseObject,
              row.getBaseOffset,
              row.getSizeInBytes)
            i += 1
          }
        }
      case _ =>
        val predicate = prepareVanillaBlackhole(Seq(expression), attributes)
        () => {
          var i = 0
          while (i < inputs.length) {
            predicate.eval(inputs(i))
            i += 1
          }
        }
    }
  }

  private def retainedBatches(
      rows: Array[UnsafeRow],
      schema: StructType,
      batchSize: Int): Seq[ColumnarBatch] = {
    RowToVeloxColumnarExec.toColumnarBatchIterator(
      rows.iterator,
      schema,
      batchSize,
      Long.MaxValue).zipWithIndex.map {
      case (batch, index) =>
        // The iterator recycles its previous payload when advanced.
        ColumnarBatches.retain(batch)
        TaskResources.addRecycler(s"ExpressionBenchmark input $index", 100)(batch.close())
        batch
    }.toVector
  }

  private val comparisonHeader = "对比" // scalastyle:ignore nonascii
  private val missingValue = "—" // scalastyle:ignore nonascii

  final case class Timing(medianMs: Double, stdevMs: Double)

  def medianMs(samples: Seq[Long]): Double = {
    require(samples.nonEmpty, "Median requires at least one measured sample")
    val sorted = samples.sorted
    val middle = sorted.size / 2
    if (sorted.size % 2 == 0) {
      (sorted(middle - 1).toDouble + sorted(middle).toDouble) / 2e6
    } else {
      sorted(middle) / 1e6
    }
  }

  def renderSummary(rows: Seq[(String, Option[Timing], Option[Timing])]): String = {
    def cell(timing: Option[Timing]): String = timing
      .map(t => "%.3f | %.3f".formatLocal(Locale.US, t.medianMs, t.stdevMs))
      .getOrElse(missingValue)

    val table = Seq(Seq("case", "vanilla", "native", comparisonHeader)) ++ rows.map {
      case (name, vanilla, native) =>
        val speedup = for {
          v <- vanilla
          n <- native
          if java.lang.Double.isFinite(v.medianMs) && v.medianMs > 0
          if java.lang.Double.isFinite(n.medianMs) && n.medianMs > 0
          ratio = v.medianMs / n.medianMs
          if java.lang.Double.isFinite(ratio) && ratio > 0
        } yield "%.2fx".formatLocal(Locale.US, ratio)
        Seq(name, cell(vanilla), cell(native), speedup.getOrElse(missingValue))
    }
    val widths = (0 until 3).map(i => table.map(_(i).length).max)
    table.map {
      row =>
        (row.take(3).zip(widths).map { case (value, width) => value.padTo(width, ' ') } :+ row(3))
          .mkString("  ")
    }.mkString("\n")
  }

  final case class Measurement(timing: Timing, samples: Seq[Long])

  /** Spark owns start/stopTiming; profiling stops before Spark computes summary statistics. */
  def measure(
      name: String,
      rows: Long,
      warmupTime: FiniteDuration,
      minTime: FiniteDuration,
      minNumIters: Int = 2,
      profiler: Option[ExpressionBenchmarkProfiler] = None)(action: Int => Unit): Measurement = {
    val benchmark = new Benchmark(
      name,
      rows,
      minNumIters,
      warmupTime,
      minTime,
      outputPerIteration = false)
    benchmark.addCase(name)(action)
    val c = benchmark.benchmarks.head
    val minNanos = minTime.toNanos
    var measuredNanos = 0L
    var stopped = false
    val samples = ArrayBuffer.empty[Long]
    def stop(): Unit = if (!stopped) {
      stopped = true
      profiler.foreach(_.close())
    }
    var primary: Throwable = null
    val result =
      try {
        benchmark.measure(rows, c.numIters) {
          timer =>
            if (timer.iteration == 0) profiler.foreach(_.start())
            c.fn(timer)
            if (timer.iteration >= 0) {
              val elapsed = timer.totalTime()
              samples += elapsed
              measuredNanos += elapsed
              // Match Spark's loop condition and stop before its summary statistics.
              if (timer.iteration + 1 >= minNumIters && measuredNanos >= minNanos) stop()
            }
        }
      } catch {
        case e: Throwable =>
          primary = e
          throw e
      } finally {
        try stop()
        catch {
          case e: Throwable => if (primary != null) primary.addSuppressed(e) else throw e
        }
        if (primary.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
      }
    val measured = samples.toVector
    Measurement(Timing(medianMs(measured), result.stdevMs), measured)
  }

  def run(options: ExpressionBenchmarkRunOptions): Unit = {
    val catalog = ExpressionBenchmarkCatalog.load()
    // scalastyle:off println
    if (options.list) {
      System.out.println(options.listing(catalog))
      return
    }
    val selected = options.selected(catalog)
    require(selected.nonEmpty, "No selected cases")
    BenchmarkBlackhole.requireEnabled()
    val out = System.out
    val profileRoot = options.asyncProfiler.map {
      _ =>
        Files.createDirectories(options.profileOutput)
        Files.createTempDirectory(options.profileOutput, "profile-")
    }
    var spark: SparkSession = null
    var primary: Throwable = null
    try {
      val conf = MockVeloxBackend.mockPluginContext().conf()
        .setMaster("local[1]").setAppName("ExpressionBenchmark")
        .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .set("spark.memory.offHeap.enabled", "true").set("spark.memory.offHeap.size", "8g")
        .set("spark.ui.enabled", "false")
      sqlConf.foreach { case (key, value) => conf.set(key, value) }
      spark = SparkSession.builder().config(conf).getOrCreate()
      out.println(s"median | stdev (ms); $comparisonHeader = vanilla/native")
      selected.foreach {
        scenario =>
          val timings = scala.collection.mutable.Map.empty[String, Timing]
          try {
            System.err.println(s"${scenario.id}: ${scenario.description}")
            withCorrectness(
              spark,
              scenario,
              ExpressionBenchmarkData.Context(options.rows, options.keyCardinality, options.seed),
              options.batchSize) {
              typed =>
                typed.verify()
                val metric = typed.prepareMetric()
                Seq("vanilla", "native").foreach {
                  engine =>
                    val profiler = options.asyncProfiler.map {
                      home =>
                        new ExpressionBenchmarkProfiler(
                          home,
                          options.profileEvent,
                          profileRoot.get.resolve(scenario.id).resolve(engine))
                    }
                    val result = Console.withOut(System.err) {
                      measure(
                        scenario.id,
                        options.rows,
                        options.warmup,
                        options.minTime,
                        options.minNumIters,
                        profiler = profiler) {
                        _ => if (engine == "vanilla") metric.runVanilla() else metric.runNative()
                      }
                    }
                    timings(engine) = result.timing
                    profiler.foreach(
                      p => System.err.println(s"${scenario.id} $engine profile=${p.output}"))
                }
            }
          } catch {
            case canceled: TestCanceledException =>
              System.err.println(s"SKIPPED ${scenario.id}: ${canceled.getMessage}")
          } finally {
            if (timings.nonEmpty) {
              out.println(renderSummary(Seq((
                scenario.id,
                timings.get("vanilla"),
                timings.get("native")))))
              out.flush()
            }
          }
      }
    } catch {
      case e: Throwable =>
        primary = e
        throw e
    } finally {
      try {
        if (spark != null) spark.stop()
      } catch {
        case e: Throwable =>
          if (primary == null) primary = e else primary.addSuppressed(e)
      }
      if (primary.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
      if (primary != null) throw primary
    }
    // scalastyle:on println
  }

  private case class ConsumeCodegenResult(child: Expression, sink: ExpressionBenchmarkResult)
    extends UnaryExpression
    with NonSQLExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false
    override def foldable: Boolean = false
    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("The benchmark terminal requires code generation")
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val result = child.genCode(ctx)
      val target =
        ctx.addReferenceObj("terminalResult", sink, classOf[ExpressionBenchmarkResult].getName)
      ev.copy(
        code = code"""
          ${result.code}
          $target.accept(${result.isNull} ? null : ${result.value});
        """,
        isNull = FalseLiteral,
        value = TrueLiteral
      )
    }
  }

  /** Borrow the generated child result for untimed full-content validation. */
  def prepareVanilla(
      expressions: Seq[Expression],
      attributes: Seq[Attribute]): InternalRow => UTF8String = {
    require(expressions.size == 1, "Exactly one output expression is currently supported")
    require(expressions.head.dataType.isInstanceOf[StringType], "String output is required")
    require(
      SQLConf.get.getConf(SQLConf.CODEGEN_FACTORY_MODE).toString ==
        CodegenObjectFactoryMode.CODEGEN_ONLY.toString,
      "The benchmark terminal requires CODEGEN_ONLY"
    )
    val sink = new ExpressionBenchmarkResult
    val predicate = Predicate.create(ConsumeCodegenResult(expressions.head, sink), attributes)
    predicate.initialize(0)
    input => {
      predicate.eval(input)
      sink.value
    }
  }

  private[benchmark] case class ConsumeCodegenBlackhole(child: Expression)
    extends UnaryExpression
    with NonSQLExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false
    override def foldable: Boolean = false
    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("The benchmark terminal requires code generation")
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val result = child.genCode(ctx)
      val consume = child.dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
            DoubleType | DateType | TimestampType | BinaryType =>
          code"""
            org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
              ${result.isNull}, ${result.value});
          """
        case _: StringType =>
          code"""
            org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole.consume(
              ${result.isNull},
              ${result.isNull} ? null : ${result.value}.getBaseObject(),
              ${result.isNull} ? 0L : ${result.value}.getBaseOffset(),
              ${result.isNull} ? 0 : ${result.value}.numBytes());
          """
        case other => throw new IllegalArgumentException(s"Unsupported metric output: $other")
      }
      ev.copy(
        code = code"""
          ${result.code}
          $consume
        """,
        isNull = FalseLiteral,
        value = TrueLiteral
      )
    }
  }

  def prepareVanillaBlackhole(
      expressions: Seq[Expression],
      attributes: Seq[Attribute]): BasePredicate = {
    BenchmarkBlackhole.requireEnabled()
    require(expressions.size == 1, "Exactly one output expression is currently supported")
    require(
      SQLConf.get.getConf(SQLConf.CODEGEN_FACTORY_MODE).toString ==
        CodegenObjectFactoryMode.CODEGEN_ONLY.toString,
      "The benchmark terminal requires CODEGEN_ONLY"
    )
    val predicate = Predicate.create(ConsumeCodegenBlackhole(expressions.head), attributes)
    predicate.initialize(0)
    predicate
  }

}
