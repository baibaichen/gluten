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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
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

  testWithMinSparkVersion("instance laziness stays inside caller SQLConf and task scopes", "4.0") {
    val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    var generated = 0
    var released = false
    var usage: SimpleMemoryUsageRecorder = null
    val data = Plan(
      new StructType().add("input", LongType),
      (_, rowId, _, _) => {
        assert(TaskResources.inSparkTask())
        assert(SQLConf.get.sessionLocalTimeZone == "UTC")
        generated += 1
        InternalRow(rowId)
      }
    )
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
      withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          usage = TaskResources.getSharedUsage()
          TaskResources.addRecycler("lazy instance test", 0) { released = true }
          val benchmark =
            new Benchmark(
              spark,
              scenario("input + 1"),
              Context(7),
              RunOptions.withBatchSize(3),
              data = Some(data),
              isBenchmark = false
            )
          assert(generated == 0 && benchmark.benchmarks.isEmpty)
          val inputs = benchmark.inputs
          assert(generated == 7 && (benchmark.inputs eq inputs))
          assert(inputs.batches.map(_.numRows()) == Seq(3, 3, 1))
          val (expression, attributes) = benchmark.freshJvm()
          val (independent, independentAttributes) = benchmark.freshJvm()
          assert(expression.dataType == independent.dataType)
          assert(attributes.head.exprId != independentAttributes.head.exprId)
          assert(generated == 7)
          var seen = 0
          val predicate = benchmark.prepareJvm(
            expression,
            attributes,
            Some(
              (value: Any) => {
                assert(value == seen.toLong + 1)
                seen += 1
              }))
          benchmark.runVanilla(predicate, inputs.rows, 0, inputs.rows.length)
          assert(seen == 7)
          benchmark.runNative()
          assert(generated == 7 && !released && benchmark.benchmarks.isEmpty)
        }
      }
      assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
    }
    assert(released && usage.current() == 0 && !TaskResources.inSparkTask())
    assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
  }

  testWithMinSparkVersion(
    "instance rejects native preparation outside a task before generating",
    "4.0") {
    var generated = false
    val data = Plan(
      new StructType().add("input", LongType),
      (_, _, _, _) => {
        generated = true
        InternalRow(1L)
      })
    withSQLConf(RunOptions.sqlConf: _*) {
      val benchmark =
        new Benchmark(
          spark,
          scenario("input + 1"),
          Context(1),
          RunOptions.withBatchSize(1),
          data = Some(data),
          isBenchmark = false
        )
      val error = intercept[IllegalArgumentException](benchmark.inputs)
      assert(error.getMessage.contains("TaskResources.runUnsafe"))
      assert(!generated && benchmark.benchmarks.isEmpty && !TaskResources.inSparkTask())
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

  test("metric: shared terminal generates and evaluates each child exactly once") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val child = Add(BoundReference(0, LongType, nullable = true), Literal(1L))
      Seq(false, true).foreach {
        capture =>
          val generations = new java.util.concurrent.atomic.AtomicInteger
          var observed: Any = null
          val collector: Option[Consumer[Any]] = if (capture) {
            Some(value => observed = value)
          } else None
          val predicate = codegen.prepareJvm(
            MetricTestCodegen(child, generations),
            Seq.empty,
            collector)
          assert(generations.get() == 1)
          assert(predicate.eval(InternalRow(8L)))
          if (capture) {
            assert(observed == 9L)
            assert(predicate.eval(InternalRow(null)))
            assert(observed == null)
          }
          val instructions = calls(predicate.getClass)
          if (capture) {
            assert(instructions.exists {
              case (owner, name, descriptor) =>
                owner == "java/util/function/Consumer" && name == "accept" &&
                descriptor == "(Ljava/lang/Object;)V"
            })
            assert(!instructions.exists(_._1 == marker))
          } else assert(instructions.contains((marker, "consume", "(ZJ)V")))
      }
    }
  }

  test("metric: generated primitive results use unboxed nullable marker overloads") {
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
        (TimestampType, 30L, "J")
      ).foreach {
        case (dataType, value, descriptor) =>
          val predicate = codegen.prepareJvm(
            BoundReference(0, dataType, nullable = true),
            Seq.empty)
          assert(predicate.eval(InternalRow(value)))
          assert(predicate.eval(InternalRow(null)))
          val instructions = calls(predicate.getClass)
          assert(instructions.contains((marker, "consume", s"(Z$descriptor)V")), dataType.toString)
          assert(!instructions.exists { case (_, name, _) => name == "valueOf" })
          assert(!instructions.exists { case (owner, _, _) => owner.contains("BoxesRunTime") })
      }
    }
  }

  test("metric: generated UTF8 result consumes representation without wrapper escape or copy") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val predicate = codegen.prepareJvm(
        StringTrimRight(BoundReference(0, StringType, nullable = true)),
        Seq.empty)
      Seq(null, "", "identity", "copy  ", "中  ").foreach { // scalastyle:ignore nonascii
        value => assert(predicate.eval(InternalRow(UTF8String.fromString(value))))
      }
      val instructions = calls(predicate.getClass)
      assert(instructions.filter(_._1 == marker) == Seq((
        marker,
        "consume",
        "(ZLjava/lang/Object;JI)V")))
      assert(instructions.exists {
        case (owner, name, _) =>
          owner.endsWith("$StringTrimRight") ||
          (owner == "org/apache/spark/unsafe/types/UTF8String" && name == "trimRight")
      })
      Seq("getBaseObject", "getBaseOffset", "numBytes").foreach {
        name => assert(instructions.exists(_._2 == name), name)
      }
      assert(!instructions.exists {
        case (_, name, _) =>
          Set("getBytes", "accept", "toString", "copy").contains(name)
      })
      val references = predicate.getClass.getDeclaredField("references")
      references.setAccessible(true)
      assert(!references.get(predicate).asInstanceOf[Array[AnyRef]]
        .exists(_.isInstanceOf[Consumer[_]]))
      var expected: UTF8String = null
      var captured = 0
      val collector: Consumer[Any] = value => {
        assert(value == expected)
        captured += 1
      }
      val suitePredicate = codegen.prepareJvm(
        StringTrimRight(BoundReference(0, StringType, nullable = true)),
        Seq.empty,
        Some(collector))
      Seq(null, "", "ab", "cd", "trim  ").foreach {
        value =>
          expected = UTF8String.fromString(if (value == null) null else value.trim)
          assert(suitePredicate.eval(InternalRow(UTF8String.fromString(value))))
      }
      assert(captured == 5)
      val suiteInstructions = calls(suitePredicate.getClass)
      assert(suiteInstructions.contains((
        "java/util/function/Consumer",
        "accept",
        "(Ljava/lang/Object;)V")))
      assert(!suiteInstructions.exists(_._1 == marker))
    }
  }

  testWithMinSparkVersion(
    "metric: shared prepared actions repeat both engines without regenerating inputs",
    "4.0") {
    val selected = Seq(
      "rtrim/l10-none",
      "rtrim/l13-half-even",
      "rtrim/l64-null50",
      "rtrim/l64-utf8-half-even",
      "rtrim/custom",
      "concat/binary",
      "concat/int-array",
      "map_from_arrays/standard-string-int",
      "cast/string-to-long",
      "array_contains/standard-int"
    )
    selected.foreach {
      id =>
        val scenario = catalog.find(_.id == id).get
        val data = Data.compile(scenario.inputs)
        var generated = 0
        val counted = data.copy(row = (context, rowId, local, count) => {
          generated += 1
          data.row(context, rowId, local, count)
        })
        withPreparedMetric(scenario, Context(10), 4, counted) {
          prepared =>
            assert(generated == 10)
            val order = ArrayBuffer.empty[(String, Int)]
            val benchmark = new SparkBenchmark(id, 10, 2, Duration.Zero, Duration.Zero)
            benchmark.addCase("vanilla") {
              iteration =>
                order += (("vanilla", iteration))
                prepared.runVanilla()
            }
            benchmark.addCase("native") {
              iteration =>
                order += (("native", iteration))
                val batchSizes = ArrayBuffer.empty[Int]
                var rows = 0
                prepared.runNative {
                  (input, output, offset) =>
                    assert(offset == rows && output.numRows() == input.numRows())
                    batchSizes += output.numRows()
                    rows += output.numRows()
                }
                assert(rows == 10 && batchSizes.toSeq == Seq(4, 4, 2))
            }
            benchmark.run()
            assert(order.toSeq == Seq(
              ("vanilla", 0),
              ("vanilla", 1),
              ("native", 0),
              ("native", 1)))
            assert(generated == 10)
        }
    }
  }

  testWithMinSparkVersion(
    "metric: null empty and struct outputs survive repeated shared execution",
    "4.0") {
    val scenario = metricCase("edges", "metric edges")
    val unicode = UTF8String.fromString("中  ") // scalastyle:ignore nonascii
    val examples = Seq(
      (
        "rtrim(input)",
        StringType,
        Seq(
          null,
          UTF8String.fromString(""),
          UTF8String.fromString("identity"),
          unicode)
      ),
      ("input", BinaryType, Seq(null, Array.emptyByteArray, Array[Byte](1, 0, -1))),
      ("named_struct('value', input)", LongType, Seq(null, 1L, 2L))
    )
    examples.foreach {
      case (sql, dataType, values) =>
        val data = Plan(
          new StructType().add("input", dataType),
          (_, rowId, _, _) => InternalRow(values(rowId.toInt)))
        withPreparedMetric(scenario.copy(sql = sql), Context(values.size), 2, data) {
          prepared =>
            (0 until 2).foreach {
              _ =>
                prepared.runVanilla()
                prepared.runNative()
            }
        }
    }
    val empty = Plan(
      new StructType().add("input", StringType),
      (_, _, _, _) => fail("Empty context must not generate rows"))
    withPreparedMetric(scenario, Context(0), 4, empty) {
      prepared =>
        prepared.runVanilla()
        prepared.runNative()
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

  testWithMinSparkVersion(
    "metric: normal and exceptional callbacks release task resources and restore SQLConf",
    "4.0") {
    val scenario = catalog.find(_.id == "concat/int-array").get
    Seq(false, true).foreach {
      failCallback =>
        val original = Data.compile(scenario.inputs)
        var usage: SimpleMemoryUsageRecorder = null
        var released = false
        val data = original.copy(row = (context, rowId, local, count) => {
          if (rowId == 0) {
            usage = TaskResources.getSharedUsage()
            TaskResources.addRecycler("metric test", 0) { released = true }
          }
          original.row(context, rowId, local, count)
        })
        val failure = new IllegalStateException("callback failure")
        val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
          def execute(): Unit = withPreparedMetric(scenario, Context(10), 4, data) {
            prepared =>
              assert(TaskResources.inSparkTask())
              assert(SQLConf.get.sessionLocalTimeZone == "UTC")
              assert(!released)
              (0 until 2).foreach {
                _ =>
                  prepared.runVanilla()
                  prepared.runNative()
              }
              if (failCallback) throw failure
          }
          if (failCallback) {
            val error = intercept[IllegalArgumentException](execute())
            assert(error.getCause eq failure)
          } else execute()
          assert(released)
          assert(usage.current() == 0)
          assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
          assert(!TaskResources.inSparkTask())
          assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
        }
    }
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

  Seq(2, 5).foreach {
    minimum =>
      test(s"metric: registered numIters overrides minimum count $minimum and duration") {
        val options = RunOptions(Duration.Zero, 1.second, minimum)
        withProfile(options) {
          (benchmark, profiler, commands, path) =>
            val registered = registerProfile(benchmark, numIters = 3)(())
            benchmark.measure(3L, registered.numIters)(registered.fn)
            assert(commands.toSeq == measuredCommands :+ "collapsed,total")
            assert(Files.isReadable(path) && !profiler.owned)
        }
        val unprofiled = new Benchmark(
          null,
          metricCase("unprofiled", "Count"),
          Context(3),
          options)
        Seq(3, 5).foreach {
          numIters =>
            var rounds = 0
            val registered = registerProfile(unprofiled, numIters = numIters)(rounds += 1)
            unprofiled.measure(3L, registered.numIters)(registered.fn)
            assert(registered.numIters == numIters && rounds == numIters)
        }
      }
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

  test("metric: profiling stays active between short measured iterations") {
    withProfile(RunOptions(Duration.Zero, Duration.Zero, minNumIters = 3)) {
      (benchmark, profiler, commands, path) =>
        val callback = registerProfile(benchmark) {
          assert(profiler.owned)
        }
        (0 until 3).foreach {
          iteration =>
            callback.fn(new SparkBenchmark.Timer(iteration) {
              override def totalTime(): Long = 310000L
            })
            if (iteration < 2) {
              assert(profiler.owned && commands.toSeq == Seq(cpuStart))
              assert(!Files.exists(path))
            }
        }
        assert(!profiler.owned && Files.isReadable(path))
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
    }
  }

  test("metric: callback resets elapsed at the first measured round on reuse") {
    withProfile(RunOptions(Duration.Zero, 5.nanos)) {
      (benchmark, profiler, commands, path) =>
        val registered = registerProfile(benchmark)(())
        (0 until 2).foreach {
          run =>
            (0 until 3).foreach {
              iteration =>
                registered.fn(new SparkBenchmark.Timer(iteration) {
                  override def totalTime(): Long = 2L
                })
                assert(Files.exists(path) == (iteration == 2))
                assert(profiler.owned == (iteration < 2))
            }
            assert(commands.toSeq == Seq.fill(run + 1)(
              measuredCommands :+ "collapsed,total").flatten)
            Files.delete(path)
        }
    }
  }

  test("metric: profiler commands are outside timing and Spark statistics failures still dump") {
    var timer = Option.empty[SparkBenchmark.Timer]
    withProfile(response = _ => {
      timer.foreach(_.totalTime())
      "sample 1\n"
    }) {
      (benchmark, profiler, commands, path) =>
        val callback = registerProfile(benchmark) {
          intercept[AssertionError](timer.get.totalTime())
        }
        val primary = new IllegalStateException("Spark statistics")
        val console = new java.io.PrintStream(new java.io.ByteArrayOutputStream) {
          override def println(value: Any): Unit = { // scalastyle:ignore println
            if (String.valueOf(value).contains("Stopped after")) throw primary
          }
        }
        try Console.withOut(console) {
            val thrown = intercept[IllegalStateException] {
              benchmark.measure(3L, callback.numIters) {
                current =>
                  timer = Some(current)
                  callback.fn(current)
              }
            }
            assert(thrown eq primary)
            assert(thrown.getSuppressed.isEmpty)
          }
        finally console.close()
        assert(commands.toSeq == measuredCommands :+ "collapsed,total")
        assert(Files.isReadable(path) && !profiler.owned)
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

  Seq(cpuStart, "stop", "collapsed,total").foreach {
    interruptedCommand =>
      test(
        s"metric: command interruption restores the flag: $interruptedCommand") {
        val failure = new InterruptedException(interruptedCommand)
        withProfile(response =
          command => if (command == interruptedCommand) throw failure else "") {
          (benchmark, profiler, commands, path) =>
            val callback = registerProfile(benchmark)(())
            try {
              val thrown = intercept[InterruptedException](benchmark.measure(
                3L,
                callback.numIters)(callback.fn))
              assert((thrown eq failure) && thrown.getSuppressed.isEmpty)
              assert(Thread.currentThread().isInterrupted)
              val completed = commands.toVector
              profiler.stop()
              profiler.dump(path)
              assert(commands.toVector == completed && commands.last == interruptedCommand)
            } finally Thread.interrupted()
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

  test("metric: a later engine warmup failure never dumps the previous engine again") {
    withProfile(RunOptions(2.millis, Duration.Zero)) {
      (benchmark, _, commands, path) =>
        val first = registerProfile(benchmark)(())
        benchmark.measure(3L, first.numIters)(first.fn)
        val completed = commands.toVector
        val saved = Files.readAllBytes(path).toSeq
        val failure = new IllegalStateException("second engine warmup")
        val callback = registerProfile(benchmark, "native")(throw failure)
        assert(intercept[IllegalStateException](benchmark.measure(
          3L,
          callback.numIters)(callback.fn)) eq failure)
        assert(commands.toVector == completed && Files.readAllBytes(path).toSeq == saved)
        assert(!Files.exists(path.getParent.getParent.resolve("native/profile.collapsed")))
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
    "metric: native path conflict is rejected before its engine starts",
    "4.0") {
    val scenario = catalog.find(_.id == "date_sub/standard-date").get
    withProfile() {
      (_, profiler, commands, _) =>
        val root = profiler.profileRoot
        val nativePath = root.resolve(scenario.id).resolve("native")
        Files.createDirectories(nativePath.getParent)
        Files.createFile(nativePath)
        val benchmark = new Benchmark(
          spark,
          scenario,
          Context(7),
          RunOptions.withBatchSize(3),
          profiler = Some(profiler),
          isBenchmark = false
        )
        benchmark.registerCases()
        intercept[java.nio.file.FileAlreadyExistsException](benchmark.run())
        assert(commands.toSeq == measuredCommands :+ "collapsed,total")
        assert(Files.isReadable(root.resolve(scenario.id).resolve("vanilla/profile.collapsed")))
        assert(benchmark.benchmarks.size == 2 && Files.isRegularFile(nativePath))
        val completed = commands.toVector
        intercept[IllegalArgumentException](benchmark.run())
        assert(commands.toVector == completed && benchmark.benchmarks.size == 2)
    }
  }

  testWithMinSparkVersion(
    "metric: run shares one controller across both engines and two CaseDefs before reset",
    "4.0") {
    val cases = catalog.filter(
      c =>
        Set("date_sub/standard-date", "rtrim/l10-none")(c.id))
    assert(cases.size == 2)
    val root = Files.createTempDirectory("expression-profile-shared-run")
    val commands = ArrayBuffer.empty[String]
    val dumps = ArrayBuffer.empty[Int]
    var samples = 0
    val profiler =
      org.mockito.Mockito.spy(Profiler(ProfilerOptions(root, output = root)))
    org.mockito.Mockito.doReturn(
      (command: String) => {
        commands += command
        if (command == cpuStart) {
          assert(dumps.size == commands.count(_ == cpuStart) - 1)
          samples = 0
        }
        if (command == "stop") samples += 2
        if (command == "collapsed,total") {
          dumps += samples
          s"sample $samples\n"
        } else ""
      },
      Array.empty[Object]: _*
    ).when(profiler).execute
    try withSQLConf(RunOptions.sqlConf: _*) {
        TaskResources.runUnsafe {
          cases.foreach {
            scenario =>
              val output = new java.io.ByteArrayOutputStream
              val benchmark = new Benchmark(
                spark,
                scenario,
                Context(7),
                RunOptions.withBatchSize(3).copy(minNumIters = 3),
                output = Some(output),
                profiler = Some(profiler),
                isBenchmark = false
              )
              benchmark.registerCases()
              benchmark.run()
              assert(benchmark.benchmarks.map(_.name).toSeq == Seq("vanilla", "native"))
              Seq("vanilla", "native").foreach {
                engine =>
                  val path = profiler.profileRoot.resolve(scenario.id).resolve(engine)
                    .resolve("profile.collapsed")
                  assert(new String(
                    Files.readAllBytes(path),
                    java.nio.charset.StandardCharsets.UTF_8) ==
                    "sample 2\n")
              }
              assert(!output.toString("UTF-8").contains("profile="))
              val completed = commands.toVector
              intercept[IllegalArgumentException](benchmark.run())
              assert(commands.toVector == completed && benchmark.benchmarks.size == 2)
          }
          assert(commands.toSeq == Seq.fill(4)(measuredCommands :+ "collapsed,total").flatten)
          assert(dumps.toSeq == Seq(2, 2, 2, 2))
        }
      }
    finally Utils.deleteRecursively(root.toFile)
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

  testWithMinSparkVersion(
    "metric: compiling a string expression does not evaluate its lambda",
    "4.0") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val scenario = metricCase(
        "lambda-state",
        "Compilation does not evaluate lambda state",
        "concat_ws(',', transform(array(input), x -> CAST(x + 1 AS STRING)))")
      val schema = new StructType().add("input", LongType)
      val benchmark = new Benchmark(spark, scenario, Context(0))
      val timed = benchmark.prepareAnalyzed(schema, native = false)
      val timedExpression = timed.projectList.head.asInstanceOf[Alias].child
      val timedVariables = timedExpression.collect {
        case variable: NamedLambdaVariable => variable
      }
      assert(timedVariables.nonEmpty)
      val encode = UnsafeProjection.create(schema)
      val inputs = Array(encode(InternalRow(1L)).copy(), encode(InternalRow(2L)).copy())
      val predicate = codegen.prepareJvm(timedExpression, timed.child.output)
      assert(timedVariables.forall(_.value.get() == null))
      codegen.runVanilla(predicate, inputs, 0, inputs.length)
      assert(timedVariables.exists(_.value.get() != null))
    }
  }

  test("metric: Decimal results use the object blackhole without conversion") {
    withSQLConf(RunOptions.sqlConf: _*) {
      Seq(DecimalType(10, 2), DecimalType(20, 0)).foreach {
        dataType =>
          val predicate = codegen.prepareJvm(
            BoundReference(0, dataType, nullable = true),
            Seq.empty)
          assert(predicate.eval(InternalRow(Decimal(123L, dataType.precision, dataType.scale))))
          assert(predicate.eval(InternalRow(null)))
          val instructions = calls(predicate.getClass)
          assert(instructions.filter(_._1 == marker) ==
            Seq((marker, "consume", "(ZLjava/lang/Object;)V")))
          assert(!instructions.exists {
            case (_, name, _) =>
              Set("toUnscaledLong", "toJavaBigDecimal", "toDouble").contains(name)
          })
      }
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

  test("metric: generated binary result consumes the actual byte array") {
    withSQLConf(RunOptions.sqlConf: _*) {
      val predicate = codegen.prepareJvm(
        BoundReference(0, BinaryType, nullable = true),
        Seq.empty)
      Seq(null, Array.emptyByteArray, Array[Byte](0, 1, -1)).foreach {
        value => assert(predicate.eval(InternalRow(value)))
      }
      val instructions = calls(predicate.getClass)
      assert(instructions.filter(_._1 == marker) == Seq((marker, "consume", "(Z[B)V")))
      assert(!instructions.exists { case (_, name, _) => Set("copyOf", "hashCode").contains(name) })
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

private case class MetricTestCodegen(
    child: Expression,
    generations: java.util.concurrent.atomic.AtomicInteger)
  extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    generations.incrementAndGet()
    child.genCode(ctx)
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
