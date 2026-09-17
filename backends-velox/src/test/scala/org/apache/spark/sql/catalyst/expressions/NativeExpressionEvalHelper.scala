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
package org.apache.spark.sql.catalyst.expressions

import org.apache.gluten.backendsapi.{BackendsApiManager, SubstraitBackend}
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter, ExpressionMappings}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.vectorized.VeloxExpressionEvaluatorJniWrapper
import org.apache.gluten.velox.vector.VeloxInputBatch

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import io.substrait.proto.{ExpressionReference, ExtendedExpression, NamedStruct, Type}
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConverters._

trait NativeExpressionEvalHelper extends ExpressionEvalHelper {
  self: SparkFunSuite =>

  implicit protected def backendClass: Class[_ <: SubstraitBackend]

  /** One output expression; backend, evaluator, inputs and outputs share a TaskResources scope. */
  protected def prepareNativeExpression(
      expressions: Seq[Expression],
      inputAttributes: Seq[Attribute]): PreparedNativeExpression = {
    require(expressions.size == 1, "Native preparation supports exactly one output expression")
    require(
      TaskResources.inSparkTask(),
      "Native expression evaluation requires a TaskResources.runUnsafe scope")
    val expr = NativeExpressionPreparation(expressions.head)
    require(expr.resolved, s"Unresolved native expression: $expr")
    expr.foreach {
      case ref: BoundReference =>
        require(
          ref.ordinal >= 0 && ref.ordinal < inputAttributes.size,
          s"Input ordinal ${ref.ordinal} is outside ${inputAttributes.size} columns")
        require(
          ref.dataType == inputAttributes(ref.ordinal).dataType,
          s"Input type mismatch for ordinal ${ref.ordinal}")
      case _ =>
    }

    val context = new SubstraitContext
    val expressionNode = ExpressionConverter
      .replaceWithExpressionTransformer(expr, inputAttributes)
      .doTransform(context)
    val types = ConverterUtils
      .collectAttributeTypeNodes(inputAttributes)
      .asScala
      .map(_.toProtobuf)
    val schema = NamedStruct
      .newBuilder()
      .addAllNames(ConverterUtils.collectAttributeNamesWithExprId(inputAttributes))
      .setStruct(
        Type.Struct
          .newBuilder()
          .addAllTypes(types.asJava)
          .setNullability(Type.Nullability.NULLABILITY_REQUIRED))
    val extensions = context.registeredFunction.asScala.map {
      case (name, id) => ExtensionBuilder.makeFunctionMapping(name, id).toProtobuf
    }
    val serialized = ExtendedExpression
      .newBuilder()
      .setBaseSchema(schema)
      .addAllExtensions(extensions.toSeq.asJava)
      .addReferredExpr(
        ExpressionReference
          .newBuilder()
          .setExpression(expressionNode.toProtobuf)
          .addOutputNames("result"))
      .build()
      .toByteArray
    val backendName = BackendsApiManager.getBackendName(backendClass)
    val runtimeConf = NativeExpressionPreparation
      .compatibleTimeZone(expr)
      .map(zone => Map(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone))
      .getOrElse(Map.empty[String, String])
    val runtime =
      Runtimes.contextInstance(backendName, "NativeExpressionEvalHelper", runtimeConf.asJava)
    val jni = VeloxExpressionEvaluatorJniWrapper.create(runtime)
    val prepared =
      new PreparedNativeExpression(jni, jni.compile(serialized), backendName, inputAttributes.size)
    TaskResources.addRecycler("Native expression evaluator", 100) {
      prepared.close()
    }
    prepared
  }

  /** Borrows an offloaded input batch. The caller owns and closes the returned native batch. */
  protected def evaluateWithNative(
      expression: Expression,
      input: ColumnarBatch,
      inputAttributes: Seq[Attribute]): ColumnarBatch = {
    val prepared = prepareNativeExpression(Seq(expression), inputAttributes)
    try {
      prepared.evaluate(input)
    } finally {
      prepared.close()
    }
  }

  /** Reusable across batches in the same resource scope; not thread-safe. */
  final protected class PreparedNativeExpression(
      jni: VeloxExpressionEvaluatorJniWrapper,
      handle: Long,
      backendName: String,
      numInputColumns: Int)
    extends AutoCloseable {
    private var closed = false

    def evaluate(input: ColumnarBatch): ColumnarBatch = {
      require(!closed, "Native expression evaluator is closed")
      require(input.numCols() == numInputColumns, "Input columns and attributes must match")
      ColumnarBatches.checkOffloaded(input)
      val outputHandle = jni.evaluate(handle, ColumnarBatches.getNativeHandle(backendName, input))
      try {
        ColumnarBatches.create(outputHandle)
      } catch {
        case error: Throwable =>
          ColumnarBatchJniWrapper.close(outputHandle)
          throw error
      }
    }

    /** Borrows a live output, including after this evaluator is closed within the task scope. */
    def consumeStringLengths(output: ColumnarBatch): Long = {
      ColumnarBatches.checkOffloaded(output)
      jni.consumeStringLengths(ColumnarBatches.getNativeHandle(backendName, output))
    }

    override def close(): Unit = {
      if (!closed) {
        jni.close(handle)
        closed = true
      }
    }
  }

  protected def checkEvaluationWithNative(
      expression: => Expression,
      expected: Seq[Any],
      input: ColumnarBatch,
      inputAttributes: Seq[Attribute]): Unit = {
    val expr = NativeExpressionPreparation(expression)
    val catalystValues = expected.map(CatalystTypeConverters.convertToCatalyst)
    require(catalystValues.size == input.numRows(), "Expected values must cover every input row")
    val batch = evaluateWithNative(expr, input, inputAttributes)
    try {
      assert(batch.numCols() == 1, "Native expression must produce one result column")
      assert(
        batch.numRows() == catalystValues.size,
        s"Native expression produced ${batch.numRows()} rows, expected ${catalystValues.size}")
      val backendName = BackendsApiManager.getBackendName(backendClass)
      val resultType = NativeExpressionReadTypes(expr.dataType)
      val view = VeloxInputBatch.wrap(
        ColumnarBatches.getNativeHandle(backendName, batch),
        StructType(Seq(StructField("result", resultType, expr.nullable))),
        batch.numRows())
      try {
        catalystValues.indices.foreach {
          rowIndex =>
            // ColumnarArray.get does not check nulls. Spark's result checker expects
            // null-safe Catalyst values, including arrays nested inside maps and structs.
            val actual = view.getRow(rowIndex).copy().get(0, resultType)
            val expectedValue = catalystValues(rowIndex)
            try {
              assert(
                checkResult(actual, expectedValue, expr),
                s"Incorrect native evaluation at row $rowIndex: $expr, " +
                  s"actual: $actual, expected: $expectedValue")
            } catch {
              case error: TestFailedException =>
                val zones = expr
                  .collect { case aware: TimeZoneAwareExpression => aware.timeZoneId }
                  .flatten
                  .distinct
                withClue(
                  s"Native row $rowIndex: expression=$expr, dataType=${expr.dataType}, " +
                    s"nullable=${expr.nullable}, timeZones=$zones, " +
                    s"actual=$actual, expected=$expectedValue. ") {
                  throw error
                }
            }
        }
      } finally {
        view.close()
      }
    } finally {
      batch.close()
    }
  }

  private[expressions] object NativeExpressionPreparation {
    def apply(expression: Expression)(implicit
        backendClass: Class[_ <: SubstraitBackend]): Expression = {
      lazy val knownNativeClasses =
        ExpressionMappings.expressionsMap.keySet ++ ExpressionMappings.blacklistExpressionMap.keySet

      def rewrite(expr: Expression): Expression = expr match {
        case runtime: RuntimeReplaceable if !knownNativeClasses.contains(runtime.getClass) =>
          rewrite(runtime.replacement)
        case other => other.mapChildren(rewrite)
      }

      ResolveTimeZone.resolveTimeZones(rewrite(ResolveTimeZone.resolveTimeZones(expression)))
    }

    def compatibleTimeZone(expression: Expression): Option[String] = {
      val zones = expression
        .collect { case aware: TimeZoneAwareExpression => aware.timeZoneId }
        .flatten
        .map(DateTimeUtils.getZoneId)
      if (zones.nonEmpty && zones.map(_.getRules).distinct.size == 1) {
        Some(zones.head.getId)
      } else {
        None
      }
    }
  }

  private[expressions] object NativeExpressionReadTypes {
    // Read existing native carriers without changing the native vector's logical type.
    // Expression types and expected values remain logical.
    def apply(dataType: DataType): DataType = dataType match {
      case _: YearMonthIntervalType => IntegerType
      case TimestampNTZType => TimestampType
      case ArrayType(elementType, containsNull) =>
        ArrayType(apply(elementType), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(apply(keyType), apply(valueType), valueContainsNull)
      case schema: StructType =>
        StructType(schema.fields.map(field => field.copy(dataType = apply(field.dataType))))
      case _ => dataType
    }
  }
}
