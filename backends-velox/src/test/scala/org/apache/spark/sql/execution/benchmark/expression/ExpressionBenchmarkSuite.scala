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

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.benchmark.expression.ExpressionBenchmarkCatalog._
import org.apache.spark.sql.execution.benchmark.expression.ExpressionBenchmarkData.{Context, Plan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.atomic.AtomicInteger

class ExpressionBenchmarkSuite
  extends VeloxWholeStageTransformerSuite
  with ExpressionBenchmark.TypedComparison {
  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  ExpressionBenchmarkCatalog.load().foreach {
    scenario =>
      testWithMinSparkVersion(scenario.id, "4.0") {
        ExpressionBenchmark.withCorrectness(spark, scenario, Context(10), 4) {
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
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val schema = new StructType().add("input", StringType)
      Seq(
        "encode(input, 'UTF-8')" -> classOf[Encode],
        "to_json(named_struct('value', input))" -> classOf[StructsToJson]).foreach {
        case (sql, expectedClass) =>
          def prepare: Project =
            ExpressionBenchmark.prepareAnalyzed(spark, scenario(sql), schema, native = true)
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
    withSQLConf(ExpressionBenchmark.sqlConf: _*) {
      val schema = new StructType().add("input", ArrayType(IntegerType, containsNull = false))
      Seq(false, true).foreach {
        native =>
          val prepared = ExpressionBenchmark.prepareAnalyzed(
            spark,
            scenario("array_intersect(input, array(1, 2, 3))"),
            schema,
            native)
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
          ExpressionBenchmark.withCorrectness(spark, scenario(sql), Context(0), 4)(_.verify())
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
      ExpressionBenchmark.withCorrectness(spark, scenario("input + 1"), Context(0), 4, empty) {
        prepared =>
          assert(prepared.nativeBatchSizes.isEmpty)
          prepared.verify((_, _) => fail("Zero rows must not emit a result"))
      }
      ExpressionBenchmark.withCorrectness(spark, scenario("input + 1"), Context(10), 4) {
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

  testWithMinSparkVersion("metric flags are independent of correctness", "4.0") {
    ExpressionBenchmark.withCorrectness(spark, scenario("input + 1"), Context(10), 4) {
      prepared =>
        prepared.verify()
        val error = intercept[IllegalStateException](prepared.prepareMetric())
        assert(error.getMessage.contains("Compiler blackhole requires JVM arguments"))
        prepared.verify()
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
}
