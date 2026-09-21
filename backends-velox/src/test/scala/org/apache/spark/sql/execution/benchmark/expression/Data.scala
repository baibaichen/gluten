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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.types.UTF8String

import org.apache.commons.lang3.StringUtils

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

/**
 * Compile only the requested projections; no calibration rows or sketch fixtures are initialized.
 */
private[benchmark] object Data {
  import Catalog._

  final case class Context(
      rows: Long,
      keyCardinality: Option[Long] = None,
      seed: Long = 20260912L) {
    require(rows >= 0, "Row count must not be negative")
    val keys: Long = keyCardinality.getOrElse(if (rows == 0) 1L else rows)
    require(keys > 0, "Key cardinality must be positive")
  }

  final case class Plan(inputSchema: StructType, row: (Context, Long, Int, Int) => InternalRow)

  final case class Inputs(rows: Array[UnsafeRow], batches: Seq[ColumnarBatch])

  def materializeRows(plan: Plan, context: Context, batchSize: Int): Array[UnsafeRow] = {
    require(batchSize > 0, "Batch size must be positive")
    require(context.rows <= Int.MaxValue, "Input rows exceed in-memory array capacity")
    val encoder = UnsafeProjection.create(plan.inputSchema)
    encoder.initialize(0)
    Array.tabulate(context.rows.toInt) {
      rowId =>
        val local = rowId % batchSize
        val count = math.min(batchSize.toLong, context.rows - (rowId - local)).toInt
        val row = plan.row(context, rowId.toLong, local, count)
        require(row.numFields == plan.inputSchema.length, "Input row and schema disagree")
        encoder(row).copy()
    }
  }

  def materialize(plan: Plan, context: Context, batchSize: Int): Inputs = {
    val rows = materializeRows(plan, context, batchSize)
    val batches = RowToVeloxColumnarExec.toColumnarBatchIterator(
      rows.iterator,
      plan.inputSchema,
      batchSize,
      Long.MaxValue).zipWithIndex.map {
      case (batch, index) =>
        // The iterator recycles its previous payload when advanced.
        ColumnarBatches.retain(batch)
        TaskResources.addRecycler(s"ExpressionBenchmark input $index", 100)(batch.close())
        batch
    }.toVector
    Inputs(rows, batches)
  }

  private val standardFields: Map[String, StructField] = Seq(
    StructField("long", LongType, nullable = true),
    StructField("int", IntegerType, nullable = true),
    StructField("string", StringType, nullable = true),
    StructField("double", DoubleType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("date", DateType, nullable = true),
    StructField("binary", BinaryType, nullable = false),
    StructField("intArray", ArrayType(IntegerType, containsNull = false), nullable = false),
    StructField(
      "struct",
      StructType(Seq(
        StructField("f1", LongType, nullable = false),
        StructField("f2", StringType, nullable = false))),
      nullable = false),
    StructField("boundedInt", IntegerType, nullable = false),
    StructField("stringArray", ArrayType(StringType, containsNull = false), nullable = false),
    StructField("timestampString", StringType, nullable = false)
  ).map(f => s"standard.${f.name}" -> f).toMap
  private val patterns = Set("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  private val special = Set(
    "rtrimPattern",
    "ltrimPattern",
    "rtrimBoundary",
    "ltrimBoundary",
    "rtrimCustom",
    "ltrimCustom")
  private val strings = Set("standard.string", "standard.stringArray")

  private def argument(binding: Binding, name: String): Option[Argument] =
    binding.arguments.find(_._1 == name).map(_._2)

  private def integer(binding: Binding, name: String, default: Option[Int] = None): Int =
    argument(binding, name) match {
      case Some(LongArgument(value)) => Math.toIntExact(value)
      case None if default.isDefined => default.get
      case _ => throw new IllegalArgumentException(s"${binding.generator} requires integer $name")
    }

  private def pattern(binding: Binding): String = argument(binding, "pattern") match {
    case Some(StringArgument(value)) if patterns.contains(value) => value
    case other => throw new IllegalArgumentException(s"Invalid trim pattern: $other")
  }

  private def utf8(binding: Binding): Boolean = argument(binding, "utf8") match {
    case None => false
    case Some(BooleanArgument(value)) => value
    case _ => throw new IllegalArgumentException("utf8 must be boolean")
  }

  def validate(bindings: Seq[Binding]): Unit = {
    require(bindings.nonEmpty, "Inputs must not be empty")
    val names = scala.collection.mutable.HashSet.empty[String]
    bindings.foreach {
      binding =>
        require(
          standardFields.contains(binding.generator) || special.contains(binding.generator),
          s"Unknown generator: ${binding.generator}")
        require(
          binding.names.size == (if (binding.generator.endsWith("Custom")) 2 else 1),
          s"Wrong binding arity for ${binding.generator}")
        binding.names.foreach {
          name =>
            require(name.matches("[A-Za-z_][A-Za-z0-9_]*"), s"Invalid binding: $name")
            require(names.add(name.toLowerCase(Locale.ROOT)), s"Duplicate binding: $name")
        }
        val args = scala.collection.mutable.HashSet.empty[String]
        val allowed = if (strings.contains(binding.generator)) {
          Set("length")
        } else if (binding.generator == "rtrimPattern" || binding.generator == "ltrimPattern") {
          Set("length", "pattern", "nullPercent", "utf8")
        } else {
          Set.empty[String]
        }
        binding.arguments.foreach {
          case (name, _) =>
            require(args.add(name), s"Duplicate argument: $name")
            require(allowed.contains(name), s"Unknown argument: $name")
        }
        if (strings.contains(binding.generator)) {
          require(integer(binding, "length", Some(10)) >= 0, "length must not be negative")
        } else if (binding.generator == "rtrimPattern" || binding.generator == "ltrimPattern") {
          require(integer(binding, "length") >= 2, "Trim length must be at least two bytes")
          pattern(binding)
          val nulls = integer(binding, "nullPercent", Some(0))
          require(nulls >= 0 && nulls <= 100, "nullPercent must be between 0 and 100")
          utf8(binding)
        }
    }
  }

  def compile(bindings: Seq[Binding]): Plan = {
    validate(bindings)
    val projections = bindings.map {
      binding =>
        standardFields.get(binding.generator) match {
          case Some(field) =>
            val value = standard(binding.generator, integer(binding, "length", Some(10)))
            Plan(
              StructType(Seq(field.copy(name = binding.names.head))),
              (context, rowId, _, _) => InternalRow(value(context, rowId)))
          case None =>
            val input: (Context, Long, Int, Int) => InternalRow = binding.generator match {
              case "rtrimPattern" | "ltrimPattern" =>
                val size = integer(binding, "length")
                val selected = pattern(binding)
                val nulls = integer(binding, "nullPercent", Some(0))
                val unicode = utf8(binding)
                val leading = binding.generator == "ltrimPattern"
                (c, i, local, count) =>
                  trimPattern(c.seed, i, local, count, size, selected, nulls, unicode, leading)
              case "rtrimBoundary" | "ltrimBoundary" => (c, i, _, _) => trimBoundary(c.seed, i)
              case "rtrimCustom" | "ltrimCustom" => (c, i, _, _) => trimCustom(c.seed, i)
            }
            Plan(StructType(binding.names.map(StructField(_, StringType, nullable = true))), input)
        }
    }
    val generate: (Context, Long, Int, Int) => InternalRow = if (projections.size == 1) {
      projections.head.row
    } else {
      (context, rowId, local, count) =>
        InternalRow.fromSeq(projections.flatMap {
          projection =>
            val row = projection.row(context, rowId, local, count)
            projection.inputSchema.indices.map(i => row.get(i, projection.inputSchema(i).dataType))
        })
    }
    Plan(
      StructType(projections.flatMap(_.inputSchema.fields)),
      (context, rowId, local, count) => {
        require(rowId >= 0 && rowId < context.rows, "rowId is outside the input context")
        require(
          local >= 0 && local < count && local.toLong <= rowId &&
            rowId - local <= context.rows - count,
          "Invalid batch-local position")
        generate(context, rowId, local, count)
      }
    )
  }

  private def pad(value: Long, length: Int): String =
    StringUtils.leftPad(value.toString, length, '0')

  private def standard(name: String, length: Int): (Context, Long) => Any = {
    // Select the generator once, not once per row; every binding sees the same context and row ID.
    name match {
      case "standard.long" => (c, i) => i % c.keys
      case "standard.int" => (c, i) => (i % c.keys).toInt
      case "standard.string" => (c, i) => UTF8String.fromString(pad(i % c.keys, length))
      case "standard.double" => (c, i) => (i % c.keys).toDouble
      case "standard.timestamp" => (c, i) => Math.multiplyExact(i % c.keys, 1000000L)
      case "standard.date" => (c, i) => Math.toIntExact(Math.addExact(18262L, i % c.keys))
      case "standard.binary" => (c, i) => pad(i % c.keys, 10).getBytes(UTF_8)
      case "standard.intArray" => (c, i) =>
          new GenericArrayData(Array(
            (i % c.keys).toInt,
            ((i + 1) % c.keys).toInt,
            ((i + 2) % c.keys).toInt))
      case "standard.struct" => (c, i) =>
          InternalRow(i % c.keys, UTF8String.fromString(pad(i % c.keys, 10)))
      case "standard.boundedInt" => (c, i) => (i % c.keys % 20).toInt
      case "standard.stringArray" => (c, i) =>
          new GenericArrayData(Array(
            UTF8String.fromString(pad(i % c.keys, length)),
            UTF8String.fromString("fixed")))
      case "standard.timestampString" => (_, _) => UTF8String.fromString("2020-01-02 03:04:05")
      case other => throw new IllegalArgumentException(s"Unknown standard projection: $other")
    }
  }

  private def trimPattern(
      seed: Long,
      index: Long,
      local: Int,
      count: Int,
      length: Int,
      pattern: String,
      nullPercent: Int,
      utf8: Boolean,
      leading: Boolean): InternalRow = {
    // Penultimate retains the entire original row, including its identity and body bytes.
    val sourceLocal =
      if (pattern == "penultimate" && count > 1 && local == count - 2) count - 1
      else if (pattern == "penultimate" && count > 1 && local == count - 1) count - 2
      else local
    val global = index - local + sourceLocal
    if (((global % 200) * 73 + 37) % 200 < nullPercent * 2) {
      InternalRow.fromSeq(Seq(null))
    } else {
      val trim = pattern match {
        case "none" => false
        case "first" => sourceLocal == 0
        case "cluster" => sourceLocal < 16
        case "half-even" => sourceLocal % 2 == 0
        case "last" | "penultimate" => sourceLocal == count - 1
        case "all" => true
        case other => throw new IllegalArgumentException(s"Unknown trim pattern: $other")
      }
      val spaces = if (trim) 2 else 0
      val body = length - spaces
      val offset = if (leading) spaces else 0
      val bytes = new Array[Byte](length)
      if (trim) {
        val start = if (leading) 0 else body
        bytes(start) = ' '.toByte
        bytes(start + 1) = ' '.toByte
      }
      val rowIdentity = (global ^ seed) & 0xffffffffL
      val prefix = math.min(8, body)
      var position = 0
      while (position < prefix) {
        val digit = ((rowIdentity >>> ((prefix - position - 1) * 4)) & 15).toInt
        bytes(offset + position) = (if (digit < 10) '0' + digit else 'a' + digit - 10).toByte
        position += 1
      }
      if (utf8) {
        val codePoint = 0x4e00 + Math.floorMod(global + seed, 128L).toInt
        while (position + 3 <= body) {
          bytes(offset + position) = (0xe0 | (codePoint >>> 12)).toByte
          bytes(offset + position + 1) = (0x80 | ((codePoint >>> 6) & 63)).toByte
          bytes(offset + position + 2) = (0x80 | (codePoint & 63)).toByte
          position += 3
        }
      }
      while (position < body) {
        bytes(offset + position) =
          ('a' + Math.floorMod(seed + global * 31 + position * 17, 26L).toInt).toByte
        position += 1
      }
      InternalRow(UTF8String.fromBytes(bytes))
    }
  }

  private val boundaryString = "  " + "x" * 60 + "  "

  private def trimBoundary(seed: Long, index: Long): InternalRow = {
    val position = Math.floorMod(index + seed, 1000L)
    InternalRow(UTF8String.fromString(if (position % 101 == 0) "" else boundaryString))
  }

  private def trimCustom(seed: Long, index: Long): InternalRow = {
    val position = Math.floorMod(index + seed, 6L).toInt
    val chars = if (position % 2 == 0) "xy" else " "
    // Matching characters on the opposite side must be preserved.
    val value = if (position == 0) null else UTF8String.fromString(chars + "value" + chars)
    val trim = if (position == 1) null else UTF8String.fromString(chars)
    InternalRow(value, trim)
  }
}
