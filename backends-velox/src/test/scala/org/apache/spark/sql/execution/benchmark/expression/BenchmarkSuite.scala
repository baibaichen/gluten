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
import org.apache.gluten.memory.SimpleMemoryUsageRecorder

import org.apache.spark.benchmark.{Benchmark => SparkBenchmark}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.execution.benchmark.expression.BenchmarkSuite.TypedPrepared
import org.apache.spark.sql.execution.benchmark.expression.Catalog._
import org.apache.spark.sql.execution.benchmark.expression.Data.{Context, Plan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes}
import org.codehaus.commons.compiler.util.reflect.ByteArrayClassLoader
import org.scalatest.exceptions.TestCanceledException

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class BenchmarkSuite
  extends VeloxWholeStageTransformerSuite
  with NativeExpressionEvalHelper {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  private val catalog = Catalog.load()

  catalog.foreach {
    scenario =>
      val reason = if (!matchSparkVersion(Some("4.0"))) Some("Requires Spark 4.0 or later")
      else BenchmarkSuite.unsupported.get(scenario.id)
      reason match {
        case Some(message) => ignore(scenario.id)(fail(message))
        case None => test(scenario.id) {
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
  }

  // Preparation, correctness capture and resource scopes.

  private def scenario(sql: String): CaseDef = CaseDef(
    "framework/golden",
    Seq(Binding(Seq("input"), "standard.long", Seq.empty)),
    sql,
    "Independent correctness regression",
    SourceLocation("framework.sql", 7, 11)
  )

  testWithMinSparkVersion(
    "preparation preserves native expressions and fresh input identities",
    "4.0") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val schema = new StructType().add("input", StringType)
      Seq(
        "encode(input, 'UTF-8')" -> classOf[Encode],
        "to_json(named_struct('value', input))" -> classOf[StructsToJson]).foreach {
        case (sql, expectedClass) =>
          val benchmark = new Benchmark(spark, scenario(sql), Context(0))
          def prepare: Project = benchmark.prepareAnalyzed(schema, native = true)
          val first = prepare
          val second = prepare
          assert(first.resolved && first.child.schema == schema)
          assert(first.projectList.head.exists(expectedClass.isInstance))
          assert(first.projectList.head.references.subsetOf(first.child.outputSet))
          assert(first.child.output.head.exprId != second.child.output.head.exprId)
      }
    }
  }

  testWithMinSparkVersion("preparation folds constant arrays for both engines", "4.0") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val schema = new StructType().add("input", ArrayType(IntegerType, containsNull = false))
      Seq(false, true).foreach {
        native =>
          val benchmark = new Benchmark(
            spark,
            scenario("array_intersect(input, array(1, 2, 3))"),
            Context(0),
            isBenchmark = false)
          val prepared = benchmark.prepareAnalyzed(schema, native)
          val expression = prepared.projectList.head.asInstanceOf[Alias].child
          assert(expression.isInstanceOf[ArrayIntersect])
          val intersect = expression.asInstanceOf[ArrayIntersect]
          assert(intersect.left.references == prepared.child.outputSet)
          assert(intersect.right.isInstanceOf[Literal], s"native=$native: ${intersect.right}")
          assert(checkResult(
            intersect.right.eval(),
            new GenericArrayData(Array(1, 2, 3)),
            intersect.right.dataType,
            false))
      }
    }
  }

  test("framework: invalid SQL and unsupported shapes retain case ID, source and cause") {
    val invalid = Seq(
      "missing + 1",
      "input +",
      "(SELECT 1)",
      "sum(input)",
      "row_number() OVER (ORDER BY input)",
      "explode(array(input))",
      "input_file_name()",
      "input_file_block_start()",
      "monotonically_increasing_id()",
      "rand()",
      "randn()"
    )
    invalid.foreach {
      sql =>
        val error = intercept[IllegalArgumentException] {
          withCorrectness(spark, scenario(sql), Context(0), 4)(_.verify())
        }
        assert(error.getMessage.contains("framework/golden"), sql)
        assert(error.getMessage.contains("framework.sql:7:11"), sql)
        assert(error.getCause != null, sql)
    }
  }

  testWithMinSparkVersion("correctness submits no Spark jobs and supports zero rows", "4.0") {
    val jobs = new AtomicInteger
    val listener = new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = { jobs.incrementAndGet() }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val empty = Plan(
        new StructType().add("input", LongType),
        (_, _, _, _) =>
          fail("Zero rows must not invoke the generator"))
      withCorrectness(spark, scenario("input + 1"), Context(0), 4, empty) {
        prepared =>
          assert(prepared.nativeBatchSizes.isEmpty)
          prepared.verify((_, _) => fail("Zero rows must not emit a result"))
      }
      withCorrectness(spark, scenario("input + 1"), Context(10), 4) {
        prepared =>
          prepared.verify()
          prepared.verify()
      }
      spark.sparkContext.listenerBus.waitUntilEmpty(10000)
      assert(jobs.get() == 0)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  testWithMinSparkVersion(
    "data materialization copies rows and preserves batch-local positions",
    "4.0") {
    val positions = scala.collection.mutable.ArrayBuffer.empty[(Long, Int, Int)]
    val row = new GenericInternalRow(Array[Any](0L))
    val data = Plan(
      new StructType().add("input", LongType),
      (_, rowId, local, count) => {
        positions += ((rowId, local, count))
        row.setLong(0, rowId)
        row
      })
    intercept[IllegalArgumentException](Data.materialize(data, Context(7), 0))
    intercept[IllegalArgumentException] {
      Data.materialize(data, Context(Int.MaxValue.toLong + 1), 3)
    }
    assert(positions.isEmpty)
    TaskResources.runUnsafe {
      val inputs = Data.materialize(data, Context(7), 3)
      assert(inputs.rows.map(_.getLong(0)).toSeq == (0L until 7L))
      assert(inputs.batches.map(_.numRows()) == Seq(3, 3, 1))
      assert(positions.toSeq == Seq(
        (0L, 0, 3),
        (1L, 1, 3),
        (2L, 2, 3),
        (3L, 0, 3),
        (4L, 1, 3),
        (5L, 2, 3),
        (6L, 0, 1)))
      val invalid = data.copy(row = (_, _, _, _) => InternalRow.empty)
      val error = intercept[IllegalArgumentException] {
        Data.materialize(invalid, Context(1), 1)
      }
      assert(error.getMessage.contains("Input row and schema disagree"))
    }
  }

  testWithMinSparkVersion("metric flags are independent of correctness", "4.0") {
    withCorrectness(spark, scenario("input + 1"), Context(10), 4)(_.verify())
    withSQLConf(RunOptions.sqlConf: _*) {
      TaskResources.runUnsafe {
        val benchmark = new Benchmark(
          spark,
          scenario("input + 1"),
          Context(10),
          RunOptions.withBatchSize(4))
        val preparationError = intercept[IllegalStateException](benchmark.inputs)
        assert(preparationError.getMessage.contains("Compiler blackhole requires JVM arguments"))
        val registrationError = intercept[IllegalStateException](benchmark.registerCases())
        assert(registrationError.getMessage.contains("Compiler blackhole requires JVM arguments"))
        assert(benchmark.benchmarks.isEmpty)

        val correctness = new Benchmark(
          spark,
          scenario("input + 1"),
          Context(10),
          RunOptions.withBatchSize(4),
          isBenchmark = false
        )
        assert(correctness.inputs.rows.length == 10)
        val (expression, attributes) = correctness.freshJvm()
        correctness.prepareJvm(expression, attributes)
        assert(correctness.benchmarks.isEmpty)
      }
    }
  }

  test("framework: map comparison is unordered, recursive and preserves binary key pairs") {
    def map(keys: Seq[Array[Byte]], values: Seq[Any]): ArrayBasedMapData =
      new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    val mapType = MapType(BinaryType, StringType, valueContainsNull = true)
    val first = map(Seq(Array[Byte](1), Array[Byte](2)), Seq(UTF8String.fromString("one"), null))
    val reversed = map(Seq(Array[Byte](2), Array[Byte](1)), Seq(null, UTF8String.fromString("one")))
    val wrongPairs =
      map(Seq(Array[Byte](2), Array[Byte](1)), Seq(UTF8String.fromString("one"), null))
    assert(checkResult(first, reversed, mapType, false))
    assert(!checkResult(first, wrongPairs, mapType, false))
    val nestedType = new StructType().add("maps", ArrayType(mapType))
    assert(checkResult(
      InternalRow(new GenericArrayData(Seq(first, null))),
      InternalRow(new GenericArrayData(Seq(reversed, null))),
      nestedType,
      false))
    val duplicate = map(Seq(Array[Byte](1), Array[Byte](1)), Seq(null, null))
    intercept[IllegalArgumentException](checkResult(duplicate, duplicate, mapType, false))
    intercept[org.scalatest.exceptions.TestFailedException](checkResult(null, null, mapType, false))
    intercept[org.scalatest.exceptions.TestFailedException] {
      checkResult(first, first, mapType.copy(valueContainsNull = false), false)
    }
    assert(checkResult(Double.NaN, Double.NaN, DoubleType, false))
    assert(checkResult(Array[Byte](1, 2), Array[Byte](1, 2), BinaryType, false))
  }

  testWithMinSparkVersion(
    "shared capture preserves null empty and same-length native contents",
    "4.0") {
    val string = (s: String) => UTF8String.fromString(s)
    val arrays = Seq(
      null,
      new GenericArrayData(Array.empty[Any]),
      new GenericArrayData(Array[Any](null, 1)),
      new GenericArrayData(Array[Any](null, 2)))
    val mapType = MapType(StringType, IntegerType, valueContainsNull = true)
    def map(value: Any): ArrayBasedMapData = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](string("key"))),
      new GenericArrayData(Array(value)))
    val structType = new StructType().add("value", IntegerType)
    val examples = Seq(
      (StringType, Seq(null, string(""), string("ab"), string("cd"))),
      (BinaryType, Seq(null, Array.emptyByteArray, Array[Byte](1, 2), Array[Byte](2, 1))),
      (ArrayType(IntegerType), arrays),
      (
        mapType,
        Seq(
          null,
          new ArrayBasedMapData(
            new GenericArrayData(Array.empty[Any]),
            new GenericArrayData(Array.empty[Any])),
          map(null),
          map(2))),
      (structType, Seq(null, InternalRow(null), InternalRow(1), InternalRow(2)))
    )
    examples.foreach {
      case (dataType, values) =>
        val data = Plan(
          new StructType().add("input", dataType),
          (_, rowId, _, _) => InternalRow(values(rowId.toInt)))
        withCorrectness(spark, scenario("input"), Context(values.size), 3, data) {
          prepared =>
            var captured = 0
            prepared.verify {
              (row, value) =>
                assert(checkResult(value, values(row.toInt), dataType, true))
                captured += 1
            }
            assert(captured == values.size)
        }
    }
  }

  test("shared JVM range captures borrowed values before the next evaluation") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val schema = new StructType().add("input", IntegerType)
      val encode = UnsafeProjection.create(schema)
      val inputs = (0 until 5).map(i => encode(InternalRow(i)).copy()).toArray
      val reused = new GenericArrayData(Array(0))
      val evaluations = new AtomicInteger
      val expression = BenchmarkBorrowedArray(
        BoundReference(0, IntegerType, nullable = true),
        reused,
        evaluations)
      var expected = 1
      val collector: Consumer[Any] = value => {
        assert(value.asInstanceOf[GenericArrayData] eq reused)
        assert(reused.getInt(0) == expected)
        assert(evaluations.get() == expected)
        expected += 1
      }
      val benchmark =
        new Benchmark(null, scenario("input"), Context(0), isBenchmark = false)
      val predicate = benchmark.prepareJvm(expression, Seq.empty, Some(collector))
      benchmark.runVanilla(predicate, inputs, 1, 4)
      assert(expected == 4 && evaluations.get() == 3)
      benchmark.runVanilla(predicate, inputs, 4, 4)
      assert(evaluations.get() == 3)
      assert(benchmark.benchmarks.isEmpty && !TaskResources.inSparkTask())
    }
  }

  testWithMinSparkVersion("unlisted native cancellation fails with its original cause", "4.0") {
    val data = Plan(
      new StructType().add("input", StringType),
      (_, _, _, _) => fail("Native validation must precede materialization"))
    val error = intercept[org.scalatest.exceptions.TestFailedException] {
      withCorrectness(spark, scenario("encode(input, 'UTF-8')"), Context(1), 1, data)(_.verify())
    }
    assert(error.getMessage.contains("case=framework/golden"))
    assert(error.getCause.isInstanceOf[TestCanceledException])
    assert(error.getCause.getMessage.contains("Native validation does not support"))
  }

  testWithMinSparkVersion("shared native loop closes results when immediate capture fails", "4.0") {
    Seq(false, true).foreach {
      failCapture =>
        var usage: SimpleMemoryUsageRecorder = null
        var released = false
        var generated = 0
        val data = Plan(
          new StructType().add("input", LongType),
          (_, rowId, _, _) => {
            generated += 1
            if (rowId == 0) {
              usage = TaskResources.getSharedUsage()
              TaskResources.addRecycler("capture test", 0) { released = true }
            }
            InternalRow(rowId)
          }
        )
        val failure = new IllegalStateException("capture failure")
        val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
          def execute(): Unit =
            withCorrectness(spark, scenario("input + 1"), Context(10), 4, data) {
              prepared =>
                assert(generated == 10 && !released && TaskResources.inSparkTask())
                (0 until 2).foreach(_ => prepared.verify((_, _) => if (failCapture) throw failure))
                assert(generated == 10)
            }
          if (failCapture) {
            val error = intercept[IllegalArgumentException](execute())
            assert(error.getCause eq failure)
          } else execute()
          assert(released && usage.current() == 0)
          assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
          assert(!TaskResources.inSparkTask())
          assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
        }
    }
  }

  // Generated consumers, repeated execution, measurement and profiling.
  private val marker = "org/apache/spark/sql/execution/benchmark/expression/BenchmarkBlackhole"

  private def metricCase(id: String, description: String, sql: String = "input"): CaseDef =
    CaseDef(s"metric/$id", Seq.empty, sql, description, SourceLocation("metric", 1, 1))

  private val codegen = new Benchmark(
    null,
    metricCase("codegen", "Codegen only"),
    Context(0),
    isBenchmark = false)

  private def withPreparedMetric(
      scenario: CaseDef,
      context: Context,
      batchSize: Int,
      data: Plan)(f: Benchmark => Unit): Unit =
    withSQLConf(RunOptions.sqlConf: _*) {
      TaskResources.runUnsafe {
        val prepared =
          new Benchmark(
            spark,
            scenario,
            context,
            RunOptions.withBatchSize(batchSize),
            data = Some(data),
            isBenchmark = false)
        prepared.checked {
          val _ = prepared.vanilla
          f(prepared)
        }
      }
    }

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
          val predicate = codegen.prepareJvm(
            BoundReference(0, dataType, nullable = true),
            Seq.empty)
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

  testWithMinSparkVersion(
    "metric: preparation does not evaluate non-string results before the callback",
    "4.0") {
    val scenario = metricCase("deferred-error", "Evaluation belongs to the action", "input % 0")
    val data = Plan(new StructType().add("input", LongType), (_, _, _, _) => InternalRow(1L))
    var entered = false
    withPreparedMetric(scenario, Context(10), 4, data) {
      prepared =>
        entered = true
        intercept[ArithmeticException](prepared.runVanilla())
        val error = intercept[org.apache.gluten.exception.GlutenException](prepared.runNative())
        assert(error.getMessage.contains("Division by zero"))
    }
    assert(entered)
  }

  private val cpuStart = "start,event=cpu,interval=1ms,cstack=dwarf,threads"
  private val measuredCommands = Seq(cpuStart, "stop")

  private def withProfile(
      options: RunOptions =
        RunOptions(Duration.Zero, Duration.Zero),
      response: String => String = _ => "")(
      f: (
          Benchmark,
          Profiler,
          ArrayBuffer[String],
          java.nio.file.Path) => Unit): Unit = {
    val root = Files.createTempDirectory("expression-shared-profiler")
    val commands = ArrayBuffer.empty[String]
    val profiler =
      org.mockito.Mockito.spy(Profiler(ProfilerOptions(root, output = root)))
    org.mockito.Mockito.doReturn(
      (command: String) => {
        commands += command
        response(command)
      },
      Array.empty[Object]: _*).when(profiler).execute
    val scenario = metricCase("profile", "Profile")
    val benchmark = new Benchmark(
      null,
      scenario,
      Context(3),
      options,
      profiler = Some(profiler))
    try withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          f(
            benchmark,
            profiler,
            commands,
            profiler.profileRoot.resolve(
              scenario.id).resolve("vanilla").resolve("profile.collapsed"))
        }
      }
    finally Utils.deleteRecursively(root.toFile)
  }

  // Exercise the real registration callback without requiring native expression preparation.
  private def registerProfile(
      benchmark: Benchmark,
      engine: String = "vanilla",
      numIters: Int = 0)(action: => Unit): SparkBenchmark.Case = {
    val register = classOf[Benchmark].getDeclaredMethod(
      "register",
      classOf[String],
      java.lang.Integer.TYPE,
      classOf[Function0[_]])
    register.setAccessible(true)
    register.invoke(benchmark, engine, Int.box(numIters), () => action)
    benchmark.benchmarks.last
  }

  Seq((2, 0.millis), (3, 0.millis), (2, 5.millis), (3, 5.millis)).foreach {
    case (minNumIters, minTime) =>
      test(s"metric: Spark owns warmup count and duration: $minNumIters / $minTime") {
        withProfile(RunOptions(3.millis, minTime, minNumIters)) {
          (benchmark, profiler, commands, path) =>
            val iterations = ArrayBuffer.empty[Int]
            var measuredMillis = -1L
            var timer = Option.empty[SparkBenchmark.Timer]
            val callback = registerProfile(benchmark) {
              if (timer.get.iteration < 0) assert(commands.isEmpty && !profiler.owned)
              else assert(profiler.owned && !Files.exists(path))
              Thread.sleep(1)
            }
            val console = new java.io.PrintStream(new java.io.ByteArrayOutputStream) {
              override def println(value: Any): Unit = { // scalastyle:ignore println
                if (String.valueOf(value).contains("Stopped after")) {
                  assert(!profiler.owned && commands.last == "collapsed,total")
                  assert(Files.isReadable(path))
                  val elapsed = "Stopped after [0-9]+ iterations, ([0-9]+) ms".r
                  measuredMillis =
                    elapsed.findFirstMatchIn(String.valueOf(value)).get.group(1).toLong
                }
              }
            }
            try Console.withOut(console) {
                benchmark.measure(3L, callback.numIters) {
                  current =>
                    timer = Some(current)
                    iterations += current.iteration
                    callback.fn(current)
                    if (current.iteration < 0) assert(!profiler.owned)
                    else assert(profiler.owned == !Files.exists(path))
                }
              }
            finally console.close()
            val measured = iterations.filter(_ >= 0)
            assert(iterations.contains(-1) && measured.toSeq == measured.indices)
            assert(measured.size >= minNumIters && measuredMillis >= minTime.toMillis)
            if (minTime == Duration.Zero) assert(measured.size == minNumIters)
            assert(commands.toSeq == measuredCommands :+ "collapsed,total")
            assert(Files.size(path) == 0)
        }
      }
  }

  Seq("warmup", "action", "interrupt", "start", "stop", "dump", "write")
    .foreach {
      phase =>
        Seq(false, true).foreach {
          cleanupFails =>
            test(s"metric: measure cleanup preserves primary: $phase / $cleanupFails") {
              val primary = if (phase == "interrupt") new InterruptedException(phase)
              else new IllegalStateException(phase)
              val cleanup = new IllegalStateException("cleanup")
              val options = RunOptions(
                if (phase == "warmup") 1.second else Duration.Zero,
                Duration.Zero)
              withProfile(
                options,
                command => {
                  assert(!Thread.currentThread().isInterrupted)
                  if (
                    (phase == "start" && command == cpuStart) ||
                    (phase == "stop" && command == "stop") ||
                    (phase == "dump" && command == "collapsed,total")
                  ) {
                    throw primary
                  }
                  if (cleanupFails && command == "collapsed,total") throw cleanup
                  ""
                }
              ) {
                (benchmark, profiler, commands, path) =>
                  val callback = registerProfile(benchmark) {
                    if (Set("warmup", "action", "interrupt")(phase)) throw primary
                    if (phase == "write") Files.createDirectories(path)
                  }
                  try {
                    val thrown = intercept[Exception] {
                      benchmark.measure(3L, callback.numIters)(callback.fn)
                    }
                    if (phase == "write") {
                      if (cleanupFails) assert(thrown eq cleanup)
                      else assert(thrown.isInstanceOf[java.io.IOException])
                    } else assert(thrown eq primary)
                    val dumpExpected = !Set("warmup", "start", "stop")(phase)
                    val suppressed = cleanupFails && Set("action", "interrupt")(phase)
                    assert(thrown.getSuppressed.toSeq == (if (suppressed) Seq(cleanup)
                                                          else Seq.empty))
                    assert(commands.count(_ == "collapsed,total") == (if (dumpExpected) 1 else 0))
                    assert(commands.count(_ == "stop") ==
                      (if (Set("warmup", "start")(phase)) 0 else 1))
                    val completed = commands.toVector
                    profiler.stop()
                    profiler.dump(path)
                    assert(commands.toVector == completed && !profiler.owned)
                    assert(Thread.currentThread().isInterrupted == (phase == "interrupt"))
                  } finally Thread.interrupted()
              }
              assert(!TaskResources.inSparkTask())
            }
        }
    }

  test("metric: action interruption retains a failed stop as suppressed without retry or dump") {
    val primary = new InterruptedException("action")
    val cleanup = new IllegalStateException("stop")
    withProfile(response = command => if (command == "stop") throw cleanup else "") {
      (benchmark, profiler, commands, path) =>
        val callback = registerProfile(benchmark)(throw primary)
        try {
          val thrown =
            intercept[InterruptedException](benchmark.measure(3L, callback.numIters)(callback.fn))
          assert(thrown eq primary)
          assert(thrown.getSuppressed.toSeq == Seq(cleanup))
          assert(Thread.currentThread().isInterrupted)
          profiler.stop()
          profiler.dump(path)
          assert(commands.toSeq == Seq(cpuStart, "stop"))
        } finally Thread.interrupted()
    }
  }

  test("metric: existing profile is rejected before any action and never overwritten") {
    withProfile() {
      (benchmark, _, commands, path) =>
        Files.createDirectories(path.getParent)
        Files.write(path, Array[Byte](1, 2, 3))
        val callback = registerProfile(benchmark)(fail("must reject before measurement"))
        intercept[IllegalArgumentException](benchmark.measure(3L, callback.numIters)(callback.fn))
        assert(commands.isEmpty && Files.readAllBytes(path).toSeq == Seq[Byte](1, 2, 3))
    }
  }

  testWithMinSparkVersion(
    "metric: profiling completes both engines across cases",
    "4.0") {
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
                assert(Files.isReadable(
                  profiler.profileRoot.resolve(scenario.id).resolve(engine)
                    .resolve("profile.collapsed")))
            }
        }
        assert(commands.toSeq == Seq.fill(4)(measuredCommands :+ "collapsed,total").flatten)
        assert(!profiler.owned)
    }
  }

  testWithMinSparkVersion(
    "metric: failed vanilla dump prevents native from starting in the real run",
    "4.0") {
    val scenario = catalog.find(_.id == "date_sub/standard-date").get
    val failure = new IllegalStateException("dump")
    withProfile(response = command => if (command == "collapsed,total") throw failure else "") {
      (_, profiler, commands, _) =>
        val benchmark = new Benchmark(
          spark,
          scenario,
          Context(7),
          RunOptions.withBatchSize(3),
          profiler = Some(profiler),
          isBenchmark = false
        )
        benchmark.registerCases()
        assert(intercept[IllegalStateException](benchmark.run()) eq failure)
        assert(commands.toSeq == measuredCommands :+ "collapsed,total")
        assert(benchmark.benchmarks.map(_.name).toSeq == Seq("vanilla", "native"))
        val completed = commands.toVector
        intercept[IllegalArgumentException](benchmark.run())
        assert(commands.toVector == completed && benchmark.benchmarks.size == 2)
    }
  }

  testWithMinSparkVersion(
    "metric: register once then rerun without rematerializing or creating a profiler",
    "4.0") {
    val scenario = catalog.find(_.id == "date_sub/standard-date").get
    val original = Data.compile(scenario.inputs)
    var generated = 0
    val data = original.copy(row = (context, rowId, local, count) => {
      generated += 1
      original.row(context, rowId, local, count)
    })
    val directory = Files.createTempDirectory("expression-run-registration")
    val profile = directory.resolve("not-created")
    try withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          val benchmark = new Benchmark(
            spark,
            scenario,
            Context(7),
            RunOptions.withBatchSize(3).copy(
              profiler = Some(ProfilerOptions(directory, output = profile))),
            data = Some(data),
            isBenchmark = false
          )
          assert(benchmark.benchmarks.isEmpty && generated == 0)
          benchmark.registerCases()
          assert(benchmark.benchmarks.map(_.name).toSeq == Seq("vanilla", "native"))
          assert(benchmark.inputs.batches.map(_.numRows()) == Seq(3, 3, 1))
          assert(generated == 7 && !Files.exists(profile))
          (0 until 2).foreach {
            _ =>
              benchmark.run()
              assert(benchmark.benchmarks.map(_.name).toSeq == Seq("vanilla", "native"))
              assert(generated == 7 && !Files.exists(profile))
          }
        }
      }
    finally Utils.deleteRecursively(directory.toFile)
  }

  testWithMinSparkVersion(
    "metric: preparation failure does not register cases or start the controller",
    "4.0") {
    val scenario = catalog.find(_.id == "date_sub/standard-date").get
    val failure = new IllegalStateException("materialization")
    val data = Data.compile(scenario.inputs).copy(
      row = (_, _, _, _) => throw failure)
    withProfile() {
      (_, profiler, commands, _) =>
        val benchmark = new Benchmark(
          spark,
          scenario,
          Context(1),
          RunOptions.withBatchSize(1),
          data = Some(data),
          profiler = Some(profiler),
          isBenchmark = false
        )
        assert(intercept[IllegalStateException](benchmark.registerCases()) eq failure)
        assert(benchmark.benchmarks.isEmpty && commands.isEmpty)
    }
  }

  test("metric: unsupported result types fail before generated code or row evaluation") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val evaluated = new java.util.concurrent.atomic.AtomicInteger
      def unsupported: Expression =
        MetricTestResult(null, CalendarIntervalType, evaluated)
      val error = intercept[IllegalArgumentException] {
        codegen.prepareJvm(unsupported, Seq.empty)
      }
      assert(error.getMessage.contains("Unsupported metric output"))
      assert(evaluated.get() == 0)
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
          assert(calls(metric.getClass).filter(_._1 == marker) ==
            Seq((marker, "consume", "(ZLjava/lang/Object;)V")))
          val nullable = codegen.prepareJvm(
            BoundReference(0, dataType, nullable = true),
            Seq.empty)
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

  def withCorrectness(
      spark: SparkSession,
      scenario: CaseDef,
      context: Context,
      batchSize: Int)(f: TypedPrepared => Unit): Unit =
    withCorrectness(
      spark,
      scenario,
      context,
      batchSize,
      Data.compile(scenario.inputs))(f)

  def withCorrectness(
      spark: SparkSession,
      scenario: CaseDef,
      context: Context,
      batchSize: Int,
      data: Plan)(f: TypedPrepared => Unit): Unit = {
    try withSQLConf(RunOptions.sqlConf: _*) {
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
            val (expression, attributes) = prepared.freshJvm()
            // Generic ColumnarBatchRow/ColumnarArray reads do not check primitive null bits.
            val readInput = UnsafeProjection.create(data.inputSchema.asNullable)
            readInput.initialize(0)
            val readOutput = UnsafeProjection.create(
              new StructType().add("result", expression.dataType).asNullable)
            readOutput.initialize(0)
            var compare: Any => Unit = null
            val collector: Consumer[Any] = value => compare(value)
            val predicate =
              prepared.prepareJvm(expression, attributes, Some(collector))
            f(new TypedPrepared {
              override def nativeBatchSizes: Seq[Int] = batches.map(_.numRows())
              override def verify(result: (Long, Any) => Unit): Unit = {
                assert(
                  nativeBatchSizes == (0 until inputs.length by batchSize).map(
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
                          assert(
                            checkResult(actual, value, expression.dataType, expression.nullable),
                            s"Native result differs at $index")
                          result(index.toLong, value)
                          index += 1
                        }
                        try prepared.runVanilla(
                            predicate,
                            inputs,
                            offset,
                            offset + input.numRows())
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
    catch {
      case canceled: TestCanceledException =>
        fail(
          s"${scenario.sourceLocation} case=${scenario.id}: unexpected native cancellation",
          canceled)
    }
  }

}

object BenchmarkSuite {
  private[expression] val unsupported: Map[String, String] = Map(
    "array_sort/int-array-lambda" -> "Native validation rejects array_sort comparator lambda",
    "bround/standard-double" -> "Native validation rejects bround(double, 2)",
    "encode/standard-string" -> "Native validation rejects encode(input, 'UTF-8')",
    "format_string/standard-string" -> "Native validation rejects format_string('%s', input)",
    "sequence/bounded-int" -> "Native validation rejects sequence(input, input + 3)",
    "substring/binary" -> "Native validation rejects substring(binary, 2, 8)"
  )

  private[expression] trait TypedPrepared {
    def nativeBatchSizes: Seq[Int]

    /** The callback borrows this row's typed JVM value, only until it returns. */
    def verify(result: (Long, Any) => Unit = (_, _) => ()): Unit

  }

}

private case class BenchmarkBorrowedArray(
    child: Expression,
    reused: GenericArrayData,
    evaluations: AtomicInteger)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = ArrayType(IntegerType, containsNull = false)
  override def nullable: Boolean = false
  override def eval(input: InternalRow): Any = {
    evaluations.incrementAndGet()
    reused.update(0, child.eval(input))
    reused
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

private case class MetricTestResult(
    value: Any,
    override val dataType: DataType,
    evaluated: java.util.concurrent.atomic.AtomicInteger)
  extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = {
    evaluated.incrementAndGet()
    value
  }
}
