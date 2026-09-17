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
package org.apache.spark.sql.execution.benchmark

import org.apache.gluten.backendsapi.{BackendsApiManager, SubstraitBackend}
import org.apache.gluten.backendsapi.velox.{VeloxBackend, VeloxListenerApi}
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper, VeloxOutputBatchJniWrapper}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.test.MockVeloxBackend
import org.apache.gluten.velox.vector.{VeloxInputBatch, VeloxWritableColumnVector}

import org.apache.spark.SparkFunSuite
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

/** Borrowed result reference; public so Spark's generated Java can call accept. */
final class ExpressionBenchmarkResult {
  var value: UTF8String = _
  def accept(result: UTF8String): Unit = { value = result }
}

/** Primitive-only accumulator passed to generated Java; no String result is retained. */
final class ExpressionBenchmarkLengths {
  private var sum: Long = 0L
  def acceptLength(length: Int): Unit = { sum += length.toLong }
  def reset(): Unit = { sum = 0L }
  def total: Long = sum
}

/** Shared preparation and execution, not another timing framework. */
private[benchmark] object ExpressionBenchmark extends Logging {
  // String columns cover these trim cases; unsupported shapes fail instead of falling back.
  final case class Case(
      name: String,
      inputSchema: StructType,
      expressions: Seq[Attribute] => Seq[Expression],
      input: (Long, Int) => InternalRow,
      batchInput: Option[(Long, Int, Int, Int) => InternalRow] = None)

  final case class Options(
      rows: Int,
      batchSize: Int,
      seed: Long,
      caseName: Option[String],
      engine: String = "both",
      order: String = "vanilla-first")

  def parseArgs(
      args: Array[String],
      cases: Seq[Case],
      collections: Map[String, Seq[Case]] = Map.empty): Options = {
    require(
      args.nonEmpty && args.length <= 6,
      "Expected rows [batch-size [seed [case-name [engine [order]]]]]")
    val options = Options(
      args(0).toInt,
      if (args.length > 1) args(1).toInt else 10240,
      if (args.length > 2) args(2).toLong else 20260912L,
      args.lift(3),
      args.lift(4).getOrElse("both"),
      args.lift(5).getOrElse("vanilla-first")
    )
    require(options.rows > 0, "Benchmark row count must be positive")
    require(options.batchSize > 0, "Batch size must be positive")
    selectedCases(options, cases, collections)
    require(Set("both", "vanilla", "native").contains(options.engine), "Unknown engine")
    require(Set("vanilla-first", "native-first").contains(options.order), "Unknown engine order")
    options
  }

  def selectedCases(
      options: Options,
      cases: Seq[Case],
      collections: Map[String, Seq[Case]] = Map.empty): Seq[Case] = {
    val names = cases.map(_.name)
    require(names.distinct.size == names.size, "Duplicate case name")
    require(collections.keySet.intersect(names.toSet).isEmpty, "Case and collection names collide")
    options.caseName match {
      case None => cases
      case Some(name) =>
        val selected = collections.getOrElse(name, cases.filter(_.name == name))
        require(selected.nonEmpty, s"Unknown or empty case collection: $name")
        require(selected.map(_.name).distinct.size == selected.size, "Duplicate collection member")
        require(selected.forall(cases.contains), "Unknown collection member")
        selected
    }
  }

  def run(
      base: BenchmarkBase,
      cases: Seq[Case],
      args: Array[String],
      collections: Map[String, Seq[Case]] = Map.empty): Unit = {
    val options = parseArgs(args, cases, collections)
    try {
      selectedCases(options, cases, collections).foreach {
        scenario =>
          base.runBenchmark(scenario.name) {
            val prepareStarted = System.nanoTime()
            withPrepared(scenario, options.rows, options.batchSize, options.seed) {
              prepared =>
                val verifyStarted = System.nanoTime()
                prepared.verify()
                val verified = System.nanoTime()
                val benchmark = new Benchmark(
                  scenario.name,
                  options.rows.toLong,
                  outputPerIteration = true,
                  output = base.output)
                // scalastyle:off println
                benchmark.out.println(
                  s"rows=${options.rows}, nativeBatchSize=${options.batchSize}, " +
                    s"seed=${options.seed}, engine=${options.engine}, order=${options.order}")
                benchmark.out.println(
                  "Untimed prepare (including compilation): " +
                    s"${(verifyStarted - prepareStarted) / 1e6} ms; " +
                    s"correctness: ${(verified - verifyStarted) / 1e6} ms")
                // scalastyle:on println
                registerCases(benchmark, prepared, options)
                benchmark.run()
            }
          }
      }
    } finally {
      shutdown()
    }
  }

  def registerCases(benchmark: Benchmark, prepared: Prepared, options: Options): Unit = {
    val order =
      if (options.order == "vanilla-first") Seq("vanilla", "native") else Seq("native", "vanilla")
    order.filter(engine => options.engine == "both" || options.engine == engine).foreach {
      engine =>
        if (engine == "vanilla") {
          benchmark.addCase("Vanilla / UnsafeRow")(_ => prepared.runVanilla())
        } else {
          benchmark.addCase("Gluten / Velox vectors")(_ => prepared.runNative())
        }
    }
  }

  trait Prepared {
    def vanillaRows: Array[UnsafeRow]
    def nativeBatchSizes: Seq[Int]
    def nativeInputValues: Seq[Seq[String]]
    def vanillaResults: Seq[String]
    def evaluateVanilla(input: InternalRow): UTF8String
    def nativeResults: Seq[String]
    def verify(): Unit
    def runVanilla(): Unit
    def runNative(): Unit
    def vanillaSignature: Long
    def nativeSignature: Long
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

  /** Borrow the actual generated child result; no final UnsafeRow writer or payload copy. */
  def prepareVanilla(
      expressions: Seq[Expression],
      attributes: Seq[Attribute]): InternalRow => UTF8String = {
    require(expressions.size == 1, "Exactly one output expression is currently supported")
    require(expressions.head.dataType == StringType, "String output is currently supported")
    require(
      SQLConf.get.codegenFactoryMode == CodegenObjectFactoryMode.CODEGEN_ONLY,
      "The benchmark terminal requires CODEGEN_ONLY")
    val sink = new ExpressionBenchmarkResult
    val predicate = Predicate.create(ConsumeCodegenResult(expressions.head, sink), attributes)
    predicate.initialize(0)
    input => {
      predicate.eval(input)
      sink.value
    }
  }

  private case class ConsumeCodegenLengths(child: Expression, sink: ExpressionBenchmarkLengths)
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
        ctx.addReferenceObj("terminalLengths", sink, classOf[ExpressionBenchmarkLengths].getName)
      ev.copy(
        code = code"""
          ${result.code}
          $target.acceptLength(${result.isNull} ? -1 : ${result.value}.numBytes());
        """,
        isNull = FalseLiteral,
        value = TrueLiteral
      )
    }
  }

  def prepareVanillaLengths(
      expressions: Seq[Expression],
      attributes: Seq[Attribute]): (BasePredicate, ExpressionBenchmarkLengths) = {
    require(expressions.size == 1, "Exactly one output expression is currently supported")
    require(expressions.head.dataType == StringType, "String output is currently supported")
    require(
      SQLConf.get.codegenFactoryMode == CodegenObjectFactoryMode.CODEGEN_ONLY,
      "The benchmark terminal requires CODEGEN_ONLY")
    val sink = new ExpressionBenchmarkLengths
    val predicate = Predicate.create(ConsumeCodegenLengths(expressions.head, sink), attributes)
    predicate.initialize(0)
    (predicate, sink)
  }

  def withPrepared[T](scenario: Case, rows: Int, batchSize: Int, seed: Long)(
      f: Prepared => T): T = {
    require(rows >= 0, "Row count must not be negative")
    require(batchSize > 0, "Batch size must be positive")
    require(scenario.inputSchema.nonEmpty, "At least one input column is required")
    require(
      scenario.inputSchema.forall(_.dataType == StringType),
      "Expression benchmark currently supports String input columns only")
    new Runner(scenario, rows, batchSize, seed).execute(f)
  }

  def shutdown(): Unit = new VeloxListenerApi().onExecutorShutdown()

  // A constructor argument and private nesting keep this adapter out of ScalaTest discovery.
  private class Runner(scenario: Case, rows: Int, batchSize: Int, seed: Long)
    extends SparkFunSuite
    with NativeExpressionEvalHelper {
    implicit override protected val backendClass: Class[_ <: SubstraitBackend] =
      classOf[VeloxBackend]

    private val attributes = DataTypeUtils.toAttributes(scenario.inputSchema)
    private val expressions = scenario.expressions(attributes)
    require(expressions.size == 1, "Exactly one output expression is currently supported")
    require(expressions.head.dataType == StringType, "String output is currently supported")
    private lazy val backendName = BackendsApiManager.getBackendName(backendClass)
    private val resultSchema = new StructType().add("result", StringType, nullable = true)

    def execute[T](f: Prepared => T): T = {
      val conf = MockVeloxBackend.mockPluginContext().conf()
      conf.set("spark.memory.offHeap.enabled", "true")
      conf.set("spark.memory.offHeap.size", "8g")
      MockVeloxBackend.initialize(conf)
      withSQLConf(
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        "spark.memory.offHeap.enabled" -> "true",
        "spark.memory.offHeap.size" -> (8L << 30).toString) {
        TaskResources.runUnsafe {
          Using.Manager {
            use =>
              val compileStarted = System.nanoTime()
              val (vanillaMetric, lengths) = prepareVanillaLengths(expressions, attributes)
              val vanillaCompiled = System.nanoTime()
              // Each engine gets its own expression instances from the same definition.
              val native =
                use(prepareNativeExpression(scenario.expressions(attributes), attributes))
              val nativeCompiled = System.nanoTime()
              val vanilla = prepareVanilla(scenario.expressions(attributes), attributes)
              val oracle = UnsafeProjection.create(scenario.expressions(attributes), attributes)
              oracle.initialize(0)
              val encodeInput = UnsafeProjection.create(scenario.inputSchema)
              encodeInput.initialize(0)
              logInfo(
                "Untimed expression compilation: vanilla " +
                  s"${(vanillaCompiled - compileStarted) / 1e6} ms; " +
                  s"native ${(nativeCompiled - vanillaCompiled) / 1e6} ms")
              val vanillaRows = new Array[UnsafeRow](rows)
              val nativeBatches = ArrayBuffer.empty[ColumnarBatch]
              val runtime = Runtimes.contextInstance(backendName, "ExpressionBenchmarkInput")
              val wrapper = VeloxOutputBatchJniWrapper.create(runtime)
              val names = ConverterUtils
                .collectAttributeNamesWithExprId(attributes)
                .toArray(new Array[String](0))
              var start = 0
              while (start < rows) {
                val count = math.min(batchSize, rows - start)
                val batch = Using.Manager {
                  own =>
                    val columns = scenario.inputSchema.fields.map {
                      _ => own(new VeloxWritableColumnVector(count, StringType))
                    }
                    var local = 0
                    while (local < count) {
                      val index = start + local
                      val logical = scenario.batchInput match {
                        case Some(input) => input(seed, index, local, count)
                        case None => scenario.input(seed, index)
                      }
                      require(logical.numFields == columns.length, "Input row and schema disagree")
                      // The input encoder reuses storage. Stabilize once, outside every timer.
                      vanillaRows(index) = encodeInput(logical).copy()
                      columns.indices.foreach {
                        ordinal =>
                          if (logical.isNullAt(ordinal)) {
                            require(scenario.inputSchema(ordinal).nullable, "Unexpected null input")
                            columns(ordinal).putNull(local)
                          } else {
                            columns(ordinal)
                              .putByteArray(local, logical.getUTF8String(ordinal).getBytes)
                          }
                      }
                      local += 1
                    }
                    columns.foreach(_.finishStringColumn())
                    val handle = wrapper.makeVeloxBatch(columns.map(_.ownerHandle()), names)
                    try {
                      // Register the batch before column owners close; it retains their vectors.
                      use(ColumnarBatches.create(handle))
                    } catch {
                      case error: Throwable =>
                        ColumnarBatchJniWrapper.close(handle)
                        throw error
                    }
                }.get
                nativeBatches += batch
                start += count
              }
              f(
                new PreparedInputs(
                  vanillaRows,
                  nativeBatches.toVector,
                  vanilla,
                  vanillaMetric,
                  lengths,
                  oracle,
                  native))
          }.get
        }
      }
    }

    private class PreparedInputs(
        override val vanillaRows: Array[UnsafeRow],
        batches: Seq[ColumnarBatch],
        vanilla: InternalRow => UTF8String,
        vanillaMetric: BasePredicate,
        lengths: ExpressionBenchmarkLengths,
        oracle: UnsafeProjection,
        native: PreparedNativeExpression)
      extends Prepared {
      @volatile private var vanillaTotal: Long = 0L
      @volatile private var nativeTotal: Long = 0L

      override def vanillaSignature: Long = vanillaTotal
      override def nativeSignature: Long = nativeTotal

      override def nativeBatchSizes: Seq[Int] = batches.map(_.numRows())

      private def readBatch(batch: ColumnarBatch, schema: StructType): Seq[Seq[String]] = {
        val view = VeloxInputBatch.wrap(
          ColumnarBatches.getNativeHandle(backendName, batch),
          schema,
          batch.numRows())
        try {
          (0 until batch.numRows()).map {
            rowIndex =>
              val row = view.getRow(rowIndex)
              schema.indices.map {
                ordinal => if (row.isNullAt(ordinal)) null else row.getUTF8String(ordinal).toString
              }.toVector
          }.toVector
        } finally {
          view.close()
        }
      }

      override def nativeInputValues: Seq[Seq[String]] =
        batches.flatMap(batch => readBatch(batch, scenario.inputSchema))

      override def evaluateVanilla(input: InternalRow): UTF8String = vanilla(input)

      override def vanillaResults: Seq[String] = vanillaRows.map {
        input =>
          val output = evaluateVanilla(input)
          if (output == null) null else output.toString
      }.toVector

      override def nativeResults: Seq[String] = batches.flatMap {
        input =>
          val output = native.evaluate(input)
          try {
            readBatch(output, resultSchema).map(_.head)
          } finally {
            output.close()
          }
      }

      /** Validate every value, retaining at most one batch of untimed comparison strings. */
      override def verify(): Unit = {
        var start = 0
        var expectedTotal = 0L
        var nativeTotal = 0L
        batches.foreach {
          input =>
            val nativeInputs = readBatch(input, scenario.inputSchema)
            val output = native.evaluate(input)
            try {
              assert(output.numRows() == input.numRows(), "Native result row count differs")
              val nativeValues = readBatch(output, resultSchema)
              nativeTotal += native.consumeStringLengths(output)
              var local = 0
              while (local < input.numRows()) {
                val row = vanillaRows(start + local)
                val expectedInput = scenario.inputSchema.indices.map {
                  i => if (row.isNullAt(i)) null else row.getUTF8String(i).toString
                }
                assert(nativeInputs(local) == expectedInput, s"Input differs at ${start + local}")
                val evaluated = evaluateVanilla(row)
                val actual = if (evaluated == null) null else evaluated.toString
                val reference = oracle(row)
                val expected =
                  if (reference.isNullAt(0)) null else reference.getUTF8String(0).toString
                expectedTotal +=
                  (if (reference.isNullAt(0)) -1L else reference.getUTF8String(0).numBytes().toLong)
                assert(actual == expected, s"JVM terminal differs at ${start + local}")
                assert(
                  nativeValues(local).head == expected,
                  s"Native result differs at ${start + local}")
                local += 1
              }
            } finally {
              output.close()
            }
            start += input.numRows()
        }
        assert(start == vanillaRows.length, "Native inputs do not cover all Vanilla rows")
        runVanilla()
        assert(vanillaSignature == expectedTotal, "JVM byte/null signature differs")
        assert(nativeTotal == expectedTotal, "Native byte/null signature differs")
      }

      override def runVanilla(): Unit = {
        lengths.reset()
        var start = 0
        while (start < vanillaRows.length) {
          // JVM grouping is loop chunking, not ColumnarBatch conversion or payload publication.
          val end = start + math.min(4096, vanillaRows.length - start)
          var i = start
          while (i < end) {
            vanillaMetric.eval(vanillaRows(i))
            i += 1
          }
          start = end
        }
        // Publish a primitive once per pass, never the generated String result.
        vanillaTotal = lengths.total
      }

      override def runNative(): Unit = {
        var total = 0L
        var i = 0
        while (i < batches.length) {
          val output = native.evaluate(batches(i))
          try {
            // One extra JNI call per live output batch, not per row; no payload conversion.
            total += native.consumeStringLengths(output)
          } finally {
            output.close()
          }
          i += 1
        }
        nativeTotal = total
      }
    }
  }
}
