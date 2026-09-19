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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, TrueLiteral}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.benchmark.expression.ExpressionBenchmarkCatalog._
import org.apache.spark.sql.execution.benchmark.expression.ExpressionBenchmarkData.{Context, Plan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.xbean.asm9.{ClassReader, ClassVisitor, MethodVisitor, Opcodes}
import org.codehaus.commons.compiler.util.reflect.ByteArrayClassLoader
import org.scalatest.DoNotDiscover

import scala.collection.mutable.ArrayBuffer

/** Run separately with the exact compiler-blackhole VM arguments. No timing assertions. */
@DoNotDiscover
class ExpressionBenchmarkMetricSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  private val marker = "org/apache/spark/sql/execution/benchmark/expression/BenchmarkBlackhole"

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

  test("metric: generated marker arguments reference the actual child ExprCode result") {
    val children = Seq(
      Add(BoundReference(0, LongType, nullable = true), Literal(1L)),
      StringTrimRight(BoundReference(0, StringType, nullable = true)),
      BoundReference(0, BinaryType, nullable = true)
    )
    children.foreach {
      child =>
        val actualChild = child.genCode(new CodegenContext)
        val code = ExpressionBenchmark.ConsumeCodegenBlackhole(child)
          .doGenCode(new CodegenContext, ExprCode.forNonNullValue(TrueLiteral)).code.toString
        assert(code.contains(actualChild.code.toString))
        val compact = code.replaceAll("\\s+", "")
        val args = child.dataType match {
          case _: StringType =>
            s"${actualChild.isNull}," +
              s"${actualChild.isNull}?null:${actualChild.value}.getBaseObject()," +
              s"${actualChild.isNull}?0L:${actualChild.value}.getBaseOffset()," +
              s"${actualChild.isNull}?0:${actualChild.value}.numBytes()"
          case _ => s"${actualChild.isNull},${actualChild.value}"
        }
        assert(compact.contains(s"BenchmarkBlackhole.consume($args);"))
    }
  }

  test("metric: generated primitive results use unboxed nullable marker overloads") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
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
          val predicate = ExpressionBenchmark.prepareVanillaBlackhole(
            Seq(BoundReference(0, dataType, nullable = true)),
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
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val predicate = ExpressionBenchmark.prepareVanillaBlackhole(
        Seq(StringTrimRight(BoundReference(0, StringType, nullable = true))),
        Seq.empty)
      Seq(null, "", "identity", "copy  ", "中  ").foreach { // scalastyle:ignore nonascii
        value => assert(predicate.eval(InternalRow(UTF8String.fromString(value))))
      }
      val instructions = calls(predicate.getClass)
      assert(instructions.filter(_._1 == marker) == Seq((
        marker,
        "consume",
        "(ZLjava/lang/Object;JI)V")))
      assert(instructions.exists { case (owner, _, _) => owner.endsWith("$StringTrimRight") })
      Seq("getBaseObject", "getBaseOffset", "numBytes").foreach {
        name => assert(instructions.exists(_._2 == name), name)
      }
      assert(!instructions.exists { case (_, name, _) => Set("getBytes", "accept").contains(name) })
    }
  }

  test(
    "metric: shared prepared actions repeat both engines without regenerating or mutating inputs") {
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
    val catalog = ExpressionBenchmarkCatalog.load()
    selected.foreach {
      id =>
        val scenario = catalog.find(_.id == id).get
        val data = ExpressionBenchmarkData.compile(scenario.inputs)
        var generated = 0
        val counted = data.copy(row = (context, rowId, local, count) => {
          generated += 1
          data.row(context, rowId, local, count)
        })
        ExpressionBenchmark.withCorrectness(spark, scenario, Context(10), 4, counted) {
          prepared =>
            prepared.verify()
            val metric = prepared.prepareMetric()
            assert(generated == 10)
            (0 until 2).foreach {
              _ =>
                metric.runVanilla()
                metric.runNative()
            }
            prepared.verify()
            assert(generated == 10)
            assert(prepared.nativeBatchSizes == Seq(4, 4, 2))
        }
    }
  }

  test("metric: null empty and struct outputs survive repeated shared execution") {
    val scenario = CaseDef(
      "metric/edges",
      Seq.empty,
      "input",
      "metric edges",
      SourceLocation("metric", 1, 1))
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
        ExpressionBenchmark.withCorrectness(
          spark,
          scenario.copy(sql = sql),
          Context(values.size),
          2,
          data) {
          prepared =>
            val metric = prepared.prepareMetric()
            prepared.verify()
            (0 until 2).foreach {
              _ =>
                metric.runVanilla()
                metric.runNative()
            }
            prepared.verify()
        }
    }
    val empty = Plan(
      new StructType().add("input", StringType),
      (_, _, _, _) => fail("Empty context must not generate rows"))
    ExpressionBenchmark.withCorrectness(spark, scenario, Context(0), 4, empty) {
      prepared =>
        val metric = prepared.prepareMetric()
        metric.runVanilla()
        metric.runNative()
        prepared.verify()
    }
  }

  test("metric: string preflight does not mutate the timed expression lambda state") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val scenario = CaseDef(
        "metric/lambda-state",
        Seq.empty,
        "concat_ws(',', transform(array(input), x -> CAST(x + 1 AS STRING)))",
        "Independent preflight state",
        SourceLocation("metric", 1, 1)
      )
      val schema = new StructType().add("input", LongType)
      def fresh(): org.apache.spark.sql.catalyst.plans.logical.Project =
        ExpressionBenchmark.prepareAnalyzed(spark, scenario, schema, native = false)
      val timed = fresh()
      val independent = fresh()
      def variables(expression: Expression): Seq[NamedLambdaVariable] = expression.collect {
        case variable: NamedLambdaVariable => variable
      }
      val timedExpression = timed.projectList.head.asInstanceOf[Alias].child
      val timedVariables = variables(timedExpression)
      assert(timedVariables.nonEmpty)
      assert(timedVariables.forall(
        v =>
          variables(independent.projectList.head)
            .forall(other => v.value ne other.value)))
      val encode = UnsafeProjection.create(schema)
      val inputs = Array(encode(InternalRow(1L)).copy(), encode(InternalRow(2L)).copy())
      val preparations = Iterator(timed, independent)
      val metric = ExpressionBenchmark.prepareVanillaMetric(
        () => {
          val project = preparations.next()
          (project.projectList.head.asInstanceOf[Alias].child, project.child.output)
        },
        inputs)
      assert(!preparations.hasNext)
      assert(timedVariables.forall(_.value.get() == null))
      assert(variables(independent.projectList.head).exists(_.value.get() != null))
      metric()
    }
  }

  test("metric: unsupported result types fail before generated code or row evaluation") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val evaluated = new java.util.concurrent.atomic.AtomicInteger
      def unsupported: Expression =
        MetricTestResult(new GenericArrayData(Array(1)), DecimalType(10, 2), evaluated)
      val error = intercept[IllegalArgumentException] {
        ExpressionBenchmark.prepareVanillaMetric(
          () => (unsupported, Seq.empty),
          Array.empty[UnsafeRow])
      }
      assert(error.getMessage.contains("Unsupported metric output"))
      assert(evaluated.get() == 0)
    }
  }

  test("metric: onheap preflight checks every raw string result and permits null and empty") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val schema = new StructType().add("input", StringType)
      val encode = UnsafeProjection.create(schema)
      val inputs = Array(null, "", "ok", "last").map {
        value => encode(InternalRow(UTF8String.fromString(value))).copy()
      }
      val address = org.apache.spark.unsafe.Platform.allocateMemory(1)
      try {
        org.apache.spark.unsafe.Platform.putByte(null, address, 1.toByte)
        val evaluated = new java.util.concurrent.atomic.AtomicInteger
        def expression: Expression = MetricTestOffheap(
          BoundReference(0, StringType, nullable = true),
          address,
          evaluated)
        val error = intercept[IllegalArgumentException] {
          ExpressionBenchmark.prepareVanillaMetric(() => (expression, Seq.empty), inputs)
        }
        assert(error.getMessage.contains("on-heap UTF8String"))
        assert(error.getMessage.contains("3"))
        assert(evaluated.get() == inputs.length)
        val valid = ExpressionBenchmark.prepareVanillaMetric(
          () => (BoundReference(0, StringType, nullable = true), Seq.empty),
          inputs)
        valid()
      } finally {
        org.apache.spark.unsafe.Platform.freeMemory(address)
      }
    }
  }

  test("metric: complex materialization evaluates each row with CodegenFallback") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val count = new java.util.concurrent.atomic.AtomicInteger
      def expression: Expression = MetricTestResult(
        new GenericArrayData(Array(3, 1, 2)),
        ArrayType(IntegerType, containsNull = false),
        count)
      val metric = ExpressionBenchmark.prepareVanillaMetric(
        () => (expression, Seq.empty),
        Array.fill(5)(new UnsafeRow(0)))
      metric()
      assert(count.get() == 5)
      metric()
      assert(count.get() == 10)
    }
  }

  test("metric: generated binary result consumes the actual byte array") {
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val predicate = ExpressionBenchmark.prepareVanillaBlackhole(
        Seq(BoundReference(0, BinaryType, nullable = true)),
        Seq.empty)
      Seq(null, Array.emptyByteArray, Array[Byte](0, 1, -1)).foreach {
        value => assert(predicate.eval(InternalRow(value)))
      }
      val instructions = calls(predicate.getClass)
      assert(instructions.filter(_._1 == marker) == Seq((marker, "consume", "(Z[B)V")))
      assert(!instructions.exists { case (_, name, _) => Set("copyOf", "hashCode").contains(name) })
    }
  }
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

private case class MetricTestOffheap(
    child: Expression,
    address: Long,
    evaluated: java.util.concurrent.atomic.AtomicInteger)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = {
    evaluated.incrementAndGet()
    val value = child.eval(input).asInstanceOf[UTF8String]
    if (value != null && value.toString == "last") UTF8String.fromAddress(null, address, 1)
    else value
  }
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
