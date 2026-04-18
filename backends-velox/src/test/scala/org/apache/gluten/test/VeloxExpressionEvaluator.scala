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
package org.apache.gluten.test

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.{InputIteratorRelNode, RelBuilder}
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchInIterator, ColumnarBatchOutIterator, NativeColumnarToRowJniWrapper, NativeRowToColumnarJniWrapper, PlanEvaluatorJniWrapper}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.unsafe.Platform

import org.apache.arrow.c.ArrowSchema

import java.util.{Arrays => JArrays, Collections => JCollections}

/**
 * Standalone expression evaluator: Expression → Substrait → JNI → Velox.
 * Bypasses Spark's DataFrame execution engine entirely.
 *
 * The JNI execution runs inside a Spark task (via sparkContext.runJob)
 * to satisfy Velox Runtime's TaskContext requirement.
 */
object VeloxExpressionEvaluator {

  def evaluate(expression: Expression, inputRow: InternalRow, spark: SparkSession): Any = {
    val resolvedExpr = resolveTimeZone(expression)

    // Build input schema from BoundReferences
    val inputSchema = buildInputSchema(resolvedExpr)
    val attributes = schemaToAttributes(inputSchema)

    // Step 1: Expression → Substrait (runs on driver, no TaskContext needed)
    val context = new SubstraitContext()
    val exprTransformer = ExpressionConverter.replaceWithExpressionTransformer(
      resolvedExpr, attributes)
    val exprNode = exprTransformer.doTransform(context)

    // Step 2: Build Substrait plan (InputIterator → Project)
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(attributes)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(attributes)
    val inputRel = new InputIteratorRelNode(typeNodes, nameList, 0L)

    val operatorId = context.nextOperatorId("project")
    val projRel = RelBuilder.makeProjectRel(
      inputRel,
      JCollections.singletonList(exprNode),
      context,
      operatorId,
      inputSchema.fields.length)

    val outNames = JCollections.singletonList("result")
    val plan = PlanBuilder.makePlan(context, JArrays.asList(projRel), outNames)
    val planBytes = plan.toProtobuf().toByteArray

    // Step 3: Serialize inputRow for shipping to executor
    val unsafeRow = toUnsafeRow(inputRow, inputSchema)
    val rowBytes = unsafeRow.getBytes

    // Step 4: Execute inside a Spark task (provides TaskContext for Velox Runtime)
    val dataType = resolvedExpr.dataType
    val rdd = spark.sparkContext.parallelize(Seq(rowBytes), 1)
    val results = rdd.map { bytes =>
      executeInTask(bytes, inputSchema, planBytes, dataType)
    }.collect()

    results.head
  }

  private def executeInTask(
      rowBytes: Array[Byte],
      inputSchema: StructType,
      planBytes: Array[Byte],
      dataType: DataType): Any = {
    val backendName = BackendsApiManager.getBackendName

    // Reconstruct UnsafeRow from bytes
    val unsafeRow = new UnsafeRow(inputSchema.fields.length)
    unsafeRow.pointTo(rowBytes, rowBytes.length)

    // Row → ColumnarBatch
    val batch = rowToColumnarBatch(unsafeRow, inputSchema, backendName)

    // Execute Substrait plan through JNI
    val runtime = Runtimes.contextInstance(backendName, "VeloxExprEval")
    val jniWrapper = PlanEvaluatorJniWrapper.create(runtime)
    val batchIter = new ColumnarBatchInIterator(
      backendName, JCollections.singletonList(batch).iterator())

    val itrHandle = jniWrapper.nativeCreateKernelWithIterator(
      planBytes,
      null,
      Array(batchIter),
      0, 0, 0L, false,
      {
        val spillDir = new java.io.File(System.getProperty("java.io.tmpdir"), "gluten-eval-spill-" + Thread.currentThread().getId)
        spillDir.mkdirs()
        spillDir.getAbsolutePath
      })

    val outIter = new ColumnarBatchOutIterator(runtime, itrHandle)
    outIter.noMoreSplits()
    try {
      if (outIter.hasNext) {
        val resultBatch = outIter.next()
        extractScalarResult(resultBatch, dataType, backendName)
      } else {
        null
      }
    } finally {
      outIter.close()
    }
  }

  private def resolveTimeZone(expr: Expression): Expression = {
    expr.transform {
      case tz: TimeZoneAwareExpression if tz.timeZoneId.isEmpty =>
        tz.withTimeZone(SQLConf.get.sessionLocalTimeZone)
    }
  }

  private def buildInputSchema(expr: Expression): StructType = {
    val refs = expr.collect { case b: BoundReference => b }
      .distinctBy(_.ordinal)
      .sortBy(_.ordinal)
    if (refs.isEmpty) {
      StructType(Nil)
    } else {
      StructType(refs.map(r => StructField(s"col${r.ordinal}", r.dataType, r.nullable)))
    }
  }

  private def schemaToAttributes(schema: StructType): Seq[Attribute] = {
    schema.zipWithIndex.map { case (field, _) =>
      AttributeReference(field.name, field.dataType, field.nullable)()
    }
  }

  private def toUnsafeRow(row: InternalRow, schema: StructType): UnsafeRow = {
    row match {
      case ur: UnsafeRow => ur
      case _ =>
        val converter = UnsafeProjection.create(schema)
        converter.apply(row)
    }
  }

  private def rowToColumnarBatch(
      row: UnsafeRow,
      schema: StructType,
      backendName: String): org.apache.spark.sql.vectorized.ColumnarBatch = {
    val arrowSchema = SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val runtime = Runtimes.contextInstance(backendName, "RowToColumnar")
    val jniWrapper = NativeRowToColumnarJniWrapper.create(runtime)
    val arrowAllocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(arrowAllocator)

    val r2cHandle = try {
      ArrowAbiUtil.exportSchema(arrowAllocator, arrowSchema, cSchema)
      jniWrapper.init(cSchema.memoryAddress())
    } finally {
      cSchema.close()
    }

    try {
      val sizeInBytes = row.getSizeInBytes
      val buffer = Platform.allocateMemory(sizeInBytes)
      Platform.copyMemory(
        row.getBaseObject, row.getBaseOffset,
        null, buffer,
        sizeInBytes)

      val handle = jniWrapper.nativeConvertRowToColumnar(
        r2cHandle,
        Array(sizeInBytes.toLong),
        buffer)

      Platform.freeMemory(buffer)
      ColumnarBatches.create(handle)
    } finally {
      jniWrapper.close(r2cHandle)
    }
  }

  private def extractScalarResult(
      batch: org.apache.spark.sql.vectorized.ColumnarBatch,
      dataType: DataType,
      backendName: String): Any = {
    val numRows = batch.numRows()
    if (numRows == 0) return null

    val batchHandle = ColumnarBatches.getNativeHandle(backendName, batch)
    val runtime = Runtimes.contextInstance(backendName, "ColToRow")
    val c2rWrapper = NativeColumnarToRowJniWrapper.create(runtime)
    val c2rHandle = c2rWrapper.nativeColumnarToRowInit()
    try {
      val info = c2rWrapper.nativeColumnarToRowConvert(c2rHandle, batchHandle, 0)
      val row = new UnsafeRow(1)
      row.pointTo(null, info.memoryAddress + info.offsets(0), info.lengths(0))
      if (row.isNullAt(0)) null
      else row.get(0, dataType)
    } finally {
      c2rWrapper.nativeClose(c2rHandle)
    }
  }
}
