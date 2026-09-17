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

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Independent leading-trim inputs with the same41-case coverage as RTrimExpressionBenchmark. See
 * that entrypoint for Java17, native-library and offline full-reactor classpath setup; run:
 * {{{
 * "$JAVA_HOME/bin/java" @"$JAVA_MODULE_OPTIONS" -Dspark.testing=true -Xms4g -Xmx4g -cp "$CP" \
 *   org.apache.spark.sql.execution.benchmark.LTrimExpressionBenchmark \
 *   4000000 10240 20260912 aligned native
 * }}}
 * Arguments: rows [native-batch-size [seed [case-name [engine [order]]]]]. Default seed:20260912.
 * Omit case-name or use aligned for all41cases; representative selects the corresponding12cases. A
 * case ID selects one; engines are both/vanilla/native and orders vanilla-first/native-first.
 * TrimExpressionBenchmark combines both independent catalogs for the three-process comparison.
 * SPARK_GENERATE_BENCHMARK_FILES=1 enables standard results; use a distinct output cwd per process.
 *
 * The five-width grid uses native-batch-local none/first/cluster/half-even/last/penultimate/all.
 * Cluster selects the first min(16,count) rows. Penultimate swaps whole rows, including identity,
 * within full or partial batches. Selected rows have two leading spaces followed by the same seeded
 * body used in the right-trim specification; body positions do not include the leading offset. No
 * generator from the RTrim object is called, and UTF8 bytes are never reversed.
 * NULL50/UTF8-half-even, identity10/256, opposite-side and custom extras use matching coverage.
 * These replace the old128-row LTrim patterns and injected grid empty strings: do not merge results
 * from those older datasets with this protocol. Empty/all-space semantics remain covered by UTs.
 *
 * Both engines sum actual result byte lengths (NULL=-1, empty=0) into a signed Long per pass. The
 * timed JVM terminal retains no String reference. Native consumes each live result batch before
 * normal close. This is expression plus result-property throughput, not full String production or
 * pure trim latency. Full-content checks and the reference oracle remain untimed.
 */
object LTrimExpressionBenchmark extends BenchmarkBase {
  import ExpressionBenchmark.Case

  private val stringInput = new StructType().add("input", StringType, nullable = true)

  private def leadingCase(
      name: String,
      length: Int,
      pattern: String,
      nullPercent: Int = 0,
      utf8: Boolean = false,
      identity: Boolean = false): Case = {
    def input(seed: Long, index: Int, local: Int, count: Int): InternalRow = {
      val sourceLocal =
        if (pattern == "penultimate" && count > 1 && local == count - 2) count - 1
        else if (pattern == "penultimate" && count > 1 && local == count - 1) count - 2
        else local
      val global = index.toLong - local + sourceLocal
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
        val leading = if (trim) 2 else 0
        val body = length - leading
        val bytes = Array.fill[Byte](length)(' '.toByte)
        val rowIdentity = (global ^ seed) & 0xffffffffL
        val prefix = math.min(8, body)
        var position = 0
        while (position < prefix) {
          val digit = ((rowIdentity >>> ((prefix - position - 1) * 4)) & 15).toInt
          bytes(leading + position) = (if (digit < 10) '0' + digit else 'a' + digit - 10).toByte
          position += 1
        }
        if (utf8) {
          val codePoint = 0x4e00 + Math.floorMod(global + seed, 128L).toInt
          while (position + 3 <= body) {
            bytes(leading + position) = (0xe0 | (codePoint >>> 12)).toByte
            bytes(leading + position + 1) = (0x80 | ((codePoint >>> 6) & 63)).toByte
            bytes(leading + position + 2) = (0x80 | (codePoint & 63)).toByte
            position += 3
          }
        }
        while (position < body) {
          bytes(leading + position) =
            ('a' + Math.floorMod(seed + global * 31 + position * 17, 26L).toInt).toByte
          position += 1
        }
        InternalRow(UTF8String.fromBytes(bytes))
      }
    }
    Case(
      name,
      stringInput,
      attrs => if (identity) Seq(attrs.head) else Seq(StringTrimLeft(attrs.head)),
      (seed, index) => input(seed, index, index % 10240, 10240),
      Some(input _)
    )
  }

  private[benchmark] val representativeCases: Seq[Case] = Seq(
    leadingCase("ltrim-l10-none", 10, "none"),
    leadingCase("ltrim-l12-half-even", 12, "half-even"),
    leadingCase("ltrim-l13-half-even", 13, "half-even"),
    leadingCase("ltrim-l64-half-even", 64, "half-even"),
    leadingCase("ltrim-l256-none", 256, "none"),
    leadingCase("ltrim-l256-last", 256, "last"),
    leadingCase("ltrim-l256-penultimate", 256, "penultimate"),
    leadingCase("ltrim-l256-all", 256, "all"),
    leadingCase("ltrim-l64-null50", 64, "half-even", nullPercent = 50),
    leadingCase("ltrim-l64-utf8-half-even", 64, "half-even", utf8 = true),
    leadingCase("identity-l10", 10, "none", identity = true),
    leadingCase("identity-l256", 256, "none", identity = true)
  )

  private val grid: Seq[Case] = for {
    length <- Seq(10, 12, 13, 64, 256)
    pattern <- Seq("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  } yield {
    val name = s"ltrim-l$length-$pattern"
    representativeCases.find(_.name == name).getOrElse(leadingCase(name, length, pattern))
  }

  private[benchmark] val cases: Seq[Case] = grid ++ representativeCases.takeRight(4) ++ Seq(
    Case(
      "ltrim-trailing-boundary",
      stringInput,
      attrs => Seq(StringTrimLeft(attrs.head)),
      (seed, index) => {
        val position = Math.floorMod(index.toLong + seed, 1000L)
        InternalRow(UTF8String.fromString(if (position % 101 == 0) "" else "  " + "x" * 60 + "  "))
      }
    ),
    Case(
      "ltrim-custom",
      new StructType()
        .add("input", StringType, nullable = true)
        .add("trimChars", StringType, nullable = true),
      attrs => Seq(StringTrimLeft(attrs(0), Some(attrs(1)))),
      (seed, index) => {
        val position = Math.floorMod(index.toLong + seed, 6L).toInt
        val chars = if (position % 2 == 0) "xy" else " "
        // Matching suffix is a preservation boundary, not the trimming target.
        val value = if (position == 0) null else UTF8String.fromString(chars + "value" + chars)
        val trim = if (position == 1) null else UTF8String.fromString(chars)
        InternalRow(value, trim)
      }
    )
  )

  private[benchmark] val collections: Map[String, Seq[Case]] =
    Map("aligned" -> cases, "representative" -> representativeCases)

  override def runBenchmarkSuite(args: Array[String]): Unit =
    ExpressionBenchmark.run(this, cases, args, collections)
}
