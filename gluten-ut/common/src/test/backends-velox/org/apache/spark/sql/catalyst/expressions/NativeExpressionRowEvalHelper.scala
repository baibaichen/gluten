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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper, VeloxOutputBatchJniWrapper}
import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.velox.vector.{VeloxColumnarRow, VeloxWritableColumnVector}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.task.TaskResources

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Row adaptation belongs to unit tests; the native evaluator remains columnar. */
trait NativeExpressionRowEvalHelper extends NativeExpressionEvalHelper {
  self: SparkFunSuite =>

  private case class NativeRow(
      expression: Expression,
      attributes: Seq[Attribute],
      values: () => Seq[Any])

  private def prepareNativeRow(expression: Expression, inputRow: InternalRow): NativeRow = {
    val expr = NativeExpressionPreparation(expression)
    require(expr.resolved, s"Unresolved native expression: $expr")
    val references = expr
      .collect { case ref: BoundReference => ref }
      .groupBy(_.ordinal)
      .toSeq
      .sortBy(_._1)
    references.foreach {
      case (ordinal, refs) =>
        require(
          ordinal >= 0 && ordinal < inputRow.numFields,
          s"Input ordinal $ordinal is outside ${inputRow.numFields} fields")
        require(
          refs.map(_.dataType).distinct.size == 1,
          s"Conflicting input types for ordinal $ordinal")
    }
    val inputAttributes = references.map {
      case (ordinal, refs) =>
        AttributeReference(s"input_$ordinal", refs.head.dataType, refs.exists(_.nullable))()
    }
    // Preserve the test literal's nonfoldable contract by supplying its value as input.
    val literals = expr.collect { case literal: NonFoldableLiteral => literal }.distinct
    val literalOrdinals = literals.zipWithIndex.toMap
    val attributes = inputAttributes ++ literals.zipWithIndex.map {
      case (literal, index) =>
        AttributeReference(s"non_foldable_$index", literal.dataType, literal.nullable)()
    }
    val ordinals = references.map(_._1).zipWithIndex.toMap
    val bound = expr.transform {
      case ref: BoundReference => ref.copy(ordinal = ordinals(ref.ordinal))
      case literal: NonFoldableLiteral =>
        BoundReference(
          references.size + literalOrdinals(literal),
          literal.dataType,
          literal.nullable)
    }
    NativeRow(
      bound,
      attributes,
      () =>
        references.map {
          case (ordinal, refs) =>
            if (inputRow.isNullAt(ordinal)) null else inputRow.get(ordinal, refs.head.dataType)
        } ++ literals.map(_.value)
    )
  }

  protected def nativeExpressionFallbackReason(
      expression: Expression,
      attributes: Seq[Attribute]): Option[String] = {
    if (!GlutenCoreConfig.get.enableGluten) {
      return Some("Gluten is disabled")
    }
    val context = new SubstraitContext
    val (node, inputType) =
      try {
        val transformer =
          ExpressionConverter.replaceWithExpressionTransformer(expression, attributes)
        val inputType = TypeBuilder.makeStruct(
          false,
          attributes.map(a => ConverterUtils.getTypeNode(a.dataType, a.nullable)).asJava)
        (transformer.doTransform(context), inputType)
      } catch {
        case error: GlutenNotSupportException => return Some(error.getMessage)
      }
    if (
      BackendsApiManager.getValidatorApiInstance
        .doNativeValidateExpression(context, node, inputType)
    ) {
      None
    } else {
      Some("Native expression validation returned false")
    }
  }

  protected def withNativeInput(expression: Expression, inputRow: InternalRow)(
      f: (Expression, ColumnarBatch, Seq[Attribute]) => Unit): Unit = {
    withNativeInput(prepareNativeRow(expression, inputRow))(f)
  }

  private def withNativeInput(prepared: NativeRow)(
      f: (Expression, ColumnarBatch, Seq[Attribute]) => Unit): Unit = {
    TaskResources.runUnsafe {
      val attributes = prepared.attributes
      val columns = ArrayBuffer.empty[VeloxWritableColumnVector]
      var batch: ColumnarBatch = null
      try {
        attributes.foreach(a => columns += new VeloxWritableColumnVector(1, a.dataType))
        if (columns.isEmpty) {
          // A literal still evaluates once, without inspecting unrelated fields in inputRow.
          batch = new ColumnarBatch(Array.empty[ColumnVector], 1)
        } else {
          val row = new VeloxColumnarRow(columns.toArray)
          prepared.values().zipWithIndex.foreach { case (value, index) => row.update(index, value) }
          row.finishWriteRow()
          val runtime = Runtimes.contextInstance(
            BackendsApiManager.getBackendName(backendClass),
            "NativeExpressionRowInput")
          val names = ConverterUtils.collectAttributeNamesWithExprId(attributes)
          val handle = VeloxOutputBatchJniWrapper
            .create(runtime)
            .makeVeloxBatch(
              columns.map(_.ownerHandle()).toArray,
              names.toArray(new Array[String](0)))
          try {
            batch = ColumnarBatches.create(handle)
          } finally {
            if (batch == null) {
              ColumnarBatchJniWrapper.close(handle)
            }
          }
        }
        f(prepared.expression, batch, attributes)
      } finally {
        try {
          if (batch != null) {
            batch.close()
          }
        } finally {
          columns.reverseIterator.foreach(_.close())
        }
      }
    }
  }

  protected def checkEvaluationWithNativeRowIfSupported(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow): Boolean = {
    val prepared = prepareNativeRow(expression, inputRow)
    nativeExpressionFallbackReason(prepared.expression, prepared.attributes) match {
      case Some(reason) =>
        logInfo(s"Native expression fallback: $expression; $reason")
        false
      case None =>
        // Allocation, compilation, execution and comparison failures are not fallback decisions.
        withNativeInput(prepared) {
          (expr, batch, attributes) =>
            checkEvaluationWithNative(expr, Seq(expected), batch, attributes)
        }
        true
    }
  }

  protected def checkEvaluationWithNativeRow(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    withNativeInput(expression, inputRow) {
      (expr, batch, attributes) => checkEvaluationWithNative(expr, Seq(expected), batch, attributes)
    }
  }
}
