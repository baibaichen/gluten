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

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.benchmark.{Benchmark => SparkBenchmark}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, LeafExpression, NativeExpressionEvalHelper, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.execution.benchmark.expression.BenchmarkSuite.TypedPrepared
import org.apache.spark.sql.execution.benchmark.expression.Catalog._
import org.apache.spark.sql.execution.benchmark.expression.Data.{Context, Plan}
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes}
import org.codehaus.commons.compiler.util.reflect.ByteArrayClassLoader

import java.nio.file.Files
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class BenchmarkSuite extends VeloxWholeStageTransformerSuite with NativeExpressionEvalHelper {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  private val catalog = Catalog.load()
  catalog.foreach {
    scenario =>
      test(scenario.id) {
        withCorrectness(spark, scenario, Context(10), 4) {
          prepared =>
            assert(prepared.nativeBatchSizes == Seq(4, 4, 2))
            var checked = 0
            prepared.verify {
              (rowId, _) =>
                assert(rowId == checked.toLong)
                checked += 1
            }
            assert(checked == 10)
        }
      }
  }
  // Generated consumers, repeated execution, measurement and profiling.
  private val marker = "org/apache/spark/sql/execution/benchmark/expression/BenchmarkBlackhole"

  private def metricCase(id: String, description: String, sql: String = "input"): CaseDef =
    CaseDef(s"metric/$id", Seq.empty, sql, description, SourceLocation("metric", 1, 1))

  private val codegen =
    new Benchmark(null, metricCase("codegen", "Codegen only"), Context(0), isBenchmark = false)

  private def calls(generated: Class[_]): Seq[(String, String, String)] = {
    // Janino does not expose generated classes as classpath resources.
    val field = classOf[ByteArrayClassLoader].getDeclaredField("classes")
    field.setAccessible(true)
    val classes =
      field.get(generated.getClassLoader).asInstanceOf[java.util.Map[String, Array[Byte]]]
    val result = ArrayBuffer.empty[(String, String, String)]
    new ClassReader(classes.get(generated.getName)).accept(
      new ClassVisitor(Opcodes.ASM9) {
        override def visitMethod(
            access: Int,
            name: String,
            descriptor: String,
            signature: String,
            exceptions: Array[String]): MethodVisitor = new MethodVisitor(Opcodes.ASM9) {
          override def visitMethodInsn(
              opcode: Int,
              owner: String,
              name: String,
              descriptor: String,
              isInterface: Boolean): Unit = { result += ((owner, name, descriptor)) }
        }
      },
      0
    )
    result.toVector
  }

  test("metric: generated results use matching blackhole overloads and capture nulls") {
    withSQLConf(RunOptions.sqlConf: _*) {
      Seq(
        (BooleanType, true, "Z"),
        (ByteType, 7.toByte, "B"),
        (ShortType, 8.toShort, "S"),
        (IntegerType, 9, "I"),
        (LongType, 10L, "J"),
        (FloatType, 1.5f, "F"),
        (DoubleType, 2.5d, "D"),
        (DateType, 20, "I"),
        (TimestampType, 30L, "J"),
        (BinaryType, Array[Byte](1, 2), "[B"),
        (StringType, UTF8String.fromString("value"), "Ljava/lang/Object;JI"),
        (DecimalType(20, 0), Decimal(123L, 20, 0), "Ljava/lang/Object;")
      ).foreach {
        case (dataType, value, descriptor) =>
          val predicate =
            codegen.prepareJvm(BoundReference(0, dataType, nullable = true), Seq.empty)
          assert(predicate.eval(InternalRow(value)))
          assert(predicate.eval(InternalRow(null)))
          val instructions = calls(predicate.getClass)
          assert(
            instructions.filter(_._1 == marker) ==
              Seq((marker, "consume", s"(Z$descriptor)V")),
            dataType.toString)
          assert(!instructions.exists {
            case (_, name, _) =>
              Set("copy", "getBytes", "toUnscaledLong", "toJavaBigDecimal", "toDouble")
                .contains(name)
          })
          assert(!instructions.exists { case (_, name, _) => name == "valueOf" })
          assert(!instructions.exists { case (owner, _, _) => owner.contains("BoxesRunTime") })
          var captured: Any = null
          val capture = codegen.prepareJvm(
            BoundReference(0, dataType, nullable = true),
            Seq.empty,
            Some(value => captured = value))
          Seq(value, null).foreach {
            expected =>
              assert(capture.eval(InternalRow(expected)))
              assert(checkResult(captured, expected, dataType, true))
          }
      }
    }
  }
  private val cpuStart = "start,event=cpu,interval=1ms,cstack=dwarf,threads"
  private val measuredCommands = Seq(cpuStart, "stop")

  private def withProfile(
      options: RunOptions = RunOptions(Duration.Zero, Duration.Zero),
      response: String => String = _ => "")(
      f: (Benchmark, Profiler, ArrayBuffer[String], java.nio.file.Path) => Unit): Unit = {
    val root = Files.createTempDirectory("expression-shared-profiler")
    val commands = ArrayBuffer.empty[String]
    val profiler =
      org.mockito.Mockito.spy(new Profiler(ProfilerOptions(root, output = root)))
    org.mockito.Mockito
      .doReturn(
        (command: String) => {
          commands += command
          response(command)
        },
        Array.empty[Object]: _*)
      .when(profiler)
      .execute
    val scenario = metricCase("profile", "Profile")
    val benchmark = new Benchmark(null, scenario, Context(3), options, profiler = Some(profiler))
    try
      withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          f(
            benchmark,
            profiler,
            commands,
            profiler.profileRoot
              .resolve(scenario.id)
              .resolve("vanilla")
              .resolve("profile.collapsed"))
        }
      }
    finally Utils.deleteRecursively(root.toFile)
  }

  // Exercise the real registration callback without requiring native expression preparation.
  private def registerProfile(benchmark: Benchmark, engine: String = "vanilla")(
      action: => Unit): SparkBenchmark.Case = {
    benchmark.register(engine)(action)
    benchmark.benchmarks.last
  }

  test("metric: explicit warmup and Spark-owned measurement: 3 / 5.millis") {
    val minNumIters = 3
    val minTime = 5.millis
    withProfile(RunOptions(Duration.Zero, minTime, minNumIters)) {
      (benchmark, profiler, commands, path) =>
        val iterations = ArrayBuffer.empty[Int]
        var measuredMillis = -1L
        var timer = Option.empty[SparkBenchmark.Timer]
        val callback = registerProfile(benchmark) {
          iterations += timer.get.iteration
          if (timer.get.iteration < 0) assert(commands.isEmpty && !profiler.owned)
          else assert(profiler.owned && !Files.exists(path))
          Thread.sleep(1)
        }
        def runIteration(current: SparkBenchmark.Timer): Unit = {
          timer = Some(current)
          callback.fn(current)
          if (current.iteration < 0) {
            assert(commands.isEmpty && !profiler.owned && !Files.exists(path))
          } else assert(profiler.owned == !Files.exists(path))
        }
        val console = new java.io.PrintStream(new java.io.ByteArrayOutputStream) {
          override def println(value: Any): Unit = { // scalastyle:ignore println
            if (String.valueOf(value).contains("Stopped after")) {
              assert(!profiler.owned && commands.last == "collapsed,total")
              assert(Files.isReadable(path))
              val elapsed = "Stopped after [0-9]+ iterations, ([0-9]+) ms".r
              measuredMillis = elapsed.findFirstMatchIn(String.valueOf(value)).get.group(1).toLong
            }
          }
        }
        try
          Console.withOut(console) {
            // A real warmup deadline can expire before its first callback.
            (0 until 2).foreach(_ => runIteration(new SparkBenchmark.Timer(-1)))
            benchmark.measure(3L, callback.numIters)(runIteration)
          }
        finally console.close()
        val measured = iterations.filter(_ >= 0)
        assert(iterations.take(2).toSeq == Seq(-1, -1))
        assert(measured.toSeq == measured.indices)
        assert(measured.size >= minNumIters && measuredMillis >= minTime.toMillis)
        if (minTime == Duration.Zero) assert(measured.size == minNumIters)
        assert(commands.toSeq == measuredCommands :+ "collapsed,total")
        assert(Files.size(path) == 0)
    }
  }
  testWithMinSparkVersion("metric: profiling completes both engines across cases", "4.0") {
    withProfile() {
      (_, profiler, commands, _) =>
        catalog.filter(c => Set("date_sub/standard-date", "rtrim/l10-none")(c.id)).foreach {
          scenario =>
            val benchmark = new Benchmark(
              spark,
              scenario,
              Context(7),
              RunOptions.withBatchSize(3),
              profiler = Some(profiler),
              isBenchmark = false)
            benchmark.registerCases()
            benchmark.run()
            Seq("vanilla", "native").foreach {
              engine =>
                assert(
                  Files.isReadable(
                    profiler.profileRoot
                      .resolve(scenario.id)
                      .resolve(engine)
                      .resolve("profile.collapsed")))
            }
        }
        assert(commands.toSeq == Seq.fill(4)(measuredCommands :+ "collapsed,total").flatten)
        assert(!profiler.owned)
    }
  }
  test("metric: complex terminal does not serialize the generated result") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val reads = new java.util.concurrent.atomic.AtomicInteger
      val array = new GenericArrayData(Array(3, 1, 2)) {
        override def getInt(ordinal: Int): Int = {
          reads.incrementAndGet()
          super.getInt(ordinal)
        }
      }
      val row = new GenericInternalRow(Array[Any](3)) {
        override def getInt(ordinal: Int): Int = {
          reads.incrementAndGet()
          super.getInt(ordinal)
        }
      }
      val map = new ArrayBasedMapData(array, array)
      Seq(
        (array, ArrayType(IntegerType, false)),
        (map, MapType(IntegerType, IntegerType, false)),
        (row, new StructType().add("value", IntegerType))).foreach {
        case (value, dataType) =>
          val evaluations = new java.util.concurrent.atomic.AtomicInteger
          def expression: Expression = MetricTestResult(value, dataType, evaluations)
          val metric = codegen.prepareJvm(expression, Seq.empty)
          val inputs = Array.fill(5)(new UnsafeRow(0))
          assert(evaluations.get() == 0)
          codegen.runVanilla(metric, inputs, 0, inputs.length)
          assert(evaluations.get() == 5)
          codegen.runVanilla(metric, inputs, 0, inputs.length)
          assert(evaluations.get() == 10)
          assert(reads.get() == 0, "The terminal must not add an UnsafeProjection writer")
          assert(metric.eval(InternalRow.empty))
          assert(
            calls(metric.getClass).filter(_._1 == marker) ==
              Seq((marker, "consume", "(ZLjava/lang/Object;)V")))
          val nullable = codegen.prepareJvm(BoundReference(0, dataType, nullable = true), Seq.empty)
          assert(nullable.eval(InternalRow(null)))
      }
    }
  }

  /** Preserve Spark's recursive typed/nullability checks, except for unordered map entries. */
  override protected def checkResult(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = (result, expected, dataType) match {
    case (actual: MapData, reference: MapData, mapType: MapType) =>
      compareMaps(actual, reference, mapType)(checkResult)
    case _ => super.checkResult(result, expected, dataType, nullable)
  }

  private def checkOutputResult(
      result: Any,
      expected: Any,
      dataType: DataType,
      nullable: Boolean): Boolean = (result, expected, dataType) match {
    case (actual: Float, reference: Float, FloatType) =>
      checkResult(actual, reference, dataType, nullable) ||
      withinTolerance(actual.toDouble, reference.toDouble, absTol = 1e-6, relTol = 1e-5)
    case (actual: Double, reference: Double, DoubleType) =>
      checkResult(actual, reference, dataType, nullable) ||
      withinTolerance(actual, reference, absTol = 1e-12, relTol = 1e-12)
    case (actual: ArrayData, reference: ArrayData, ArrayType(elementType, containsNull)) =>
      actual.numElements() == reference.numElements() &&
      (0 until actual.numElements()).forall {
        i =>
          checkOutputResult(
            actual.get(i, elementType),
            reference.get(i, elementType),
            elementType,
            containsNull)
      }
    case (actual: InternalRow, reference: InternalRow, structType: StructType) =>
      assert(actual.numFields == structType.length && reference.numFields == structType.length)
      structType.zipWithIndex.forall {
        case (field, i) =>
          checkOutputResult(
            actual.get(i, field.dataType),
            reference.get(i, field.dataType),
            field.dataType,
            field.nullable)
      }
    case (actual: MapData, reference: MapData, mapType: MapType) =>
      compareMaps(actual, reference, mapType)(checkOutputResult)
    case _ => checkResult(result, expected, dataType, nullable)
  }

  private def withinTolerance(
      actual: Double,
      reference: Double,
      absTol: Double,
      relTol: Double): Boolean =
    java.lang.Double.isFinite(actual) && java.lang.Double.isFinite(reference) &&
      math.abs(actual - reference) <=
      math.max(absTol, relTol * math.max(math.abs(actual), math.abs(reference)))

  private def compareMaps(actual: MapData, reference: MapData, dataType: MapType)(
      compareValues: (Any, Any, DataType, Boolean) => Boolean): Boolean = {
    val MapType(keyType, valueType, valueNullable) = dataType
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
              !(0 until i).exists(j => checkResult(key(map, i), key(map, j), keyType, false)),
              "Duplicate map key")
        }
    }
    actual.numElements() == reference.numElements() &&
    (0 until actual.numElements()).forall {
      i =>
        val j = (0 until reference.numElements()).find(
          j => checkResult(key(actual, i), key(reference, j), keyType, false))
        j.exists(
          j => compareValues(value(actual, i), value(reference, j), valueType, valueNullable))
    }
  }

  def withCorrectness(spark: SparkSession, scenario: CaseDef, context: Context, batchSize: Int)(
      f: TypedPrepared => Unit): Unit =
    withCorrectness(spark, scenario, context, batchSize, Data.compile(scenario.inputs))(f)

  def withCorrectness(
      spark: SparkSession,
      scenario: CaseDef,
      context: Context,
      batchSize: Int,
      data: Plan)(f: TypedPrepared => Unit): Unit = {
    withSQLConf(RunOptions.sqlConf: _*) {
      TaskResources.runUnsafe {
        val checkedData = data.copy(row = (context, rowId, local, count) => {
          val row = data.row(context, rowId, local, count)
          assert(checkResult(row, row, data.inputSchema, false), "Invalid logical input")
          row
        })
        val prepared =
          new Benchmark(
            spark,
            scenario,
            context,
            RunOptions.withBatchSize(batchSize),
            data = Some(checkedData),
            isBenchmark = false
          )
        prepared.checked {
          assert(prepared.benchmarks.isEmpty)
          val inputs = prepared.inputs.rows
          val batches = prepared.inputs.batches
          val (expression, attributes) = prepared.freshExpression()
          if (!expression.deterministic) {
            logInfo(s"${scenario.id}: skipping nondeterministic result value comparison")
          }
          // Generic ColumnarBatchRow/ColumnarArray reads do not check primitive null bits.
          val readInput = UnsafeProjection.create(data.inputSchema.asNullable)
          readInput.initialize(0)
          val readOutput =
            UnsafeProjection.create(new StructType().add("result", expression.dataType).asNullable)
          readOutput.initialize(0)
          var compare: Any => Unit = null
          val collector: Consumer[Any] = value => compare(value)
          val predicate =
            prepared.prepareJvm(expression, attributes, Some(collector))
          f(new TypedPrepared {
            override def nativeBatchSizes: Seq[Int] = batches.map(_.numRows())
            override def verify(result: (Long, Any) => Unit): Unit = {
              assert(
                nativeBatchSizes == (inputs.indices by batchSize).map(
                  start => math.min(batchSize, inputs.length - start)),
                "Native batch sizes differ")
              var index = 0
              prepared.runNative {
                (input, output, offset) =>
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
                  assert(output.numCols() == 1, "Native result column count differs")
                  assert(output.numRows() == input.numRows(), "Native result row count differs")
                  withReadableBatch(output) {
                    readable =>
                      compare = value => {
                        val actual = readOutput(readable.getRow(index - offset))
                          .get(0, expression.dataType)
                        if (expression.deterministic) {
                          assert(
                            checkOutputResult(
                              actual,
                              value,
                              expression.dataType,
                              expression.nullable),
                            s"Native result differs at $index: actual=$actual, expected=$value")
                        } else {
                          assert(
                            checkResult(actual, actual, expression.dataType, expression.nullable),
                            s"Invalid Native result at $index")
                          assert(
                            checkResult(value, value, expression.dataType, expression.nullable),
                            s"Invalid JVM result at $index")
                        }
                        result(index.toLong, value)
                        index += 1
                      }
                      try prepared.runVanilla(predicate, inputs, offset, offset + input.numRows())
                      finally compare = null
                  }
              }
              assert(index == inputs.length, "Native inputs do not cover every logical row")
              assert(prepared.benchmarks.isEmpty, "Correctness must not register timing cases")
            }
          })
        }
      }
    }
  }

}

object BenchmarkSuite {
  private[expression] trait TypedPrepared {
    def nativeBatchSizes: Seq[Int]

    /** The callback borrows this row's typed JVM value, only until it returns. */
    def verify(result: (Long, Any) => Unit = (_, _) => ()): Unit

  }

}
private case class MetricTestResult(
    value: Any,
    override val dataType: DataType,
    evaluated: java.util.concurrent.atomic.AtomicInteger)
  extends LeafExpression
  with CodegenFallback {
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = {
    evaluated.incrementAndGet()
    value
  }
}
