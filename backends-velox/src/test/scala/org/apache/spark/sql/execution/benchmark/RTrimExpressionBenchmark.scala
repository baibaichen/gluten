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
 * Spark's Benchmark.addCase/run compares generated expressions over stable UnsafeRows with a
 * prepared Velox evaluator over real native vectors. Both receive identical logical input bytes.
 * Vanilla uses child.genCode in a primitive consumer compiled by Spark Predicate.create; its
 * Boolean result is control only. Each computed String contributes numBytes(), or -1 for NULL, to a
 * signed Long sum reset and published once per full pass. Empty contributes zero. The timed JVM
 * consumer never stores or returns the String reference. Native consumes StringView lengths from
 * the live ExprSet result with one additional JNI call per batch before normal output close.
 * Neither side scans/copies the payload for consumption or substitutes a length SQL expression.
 * Full-content validation and reference capture remain untimed, using an independent projection
 * oracle. This is original-expression plus result-property consumption, not full String production
 * or pure trim latency. Legal JIT elimination/materialization differences belong to this protocol.
 * Prove retained work and allocation behavior with a separate profile, not the length sum alone. No
 * evaluator-class Java template, timed payload hash, or Python launcher is involved.
 *
 * Build with Java 17 and this arm's native libraries in NATIVE's linux/amd64 directory. Verify
 * their hashes against the packaged backend resources; an older source mtime can leave stale target
 * resource copies. Do not use another worktree's libraries or classes.
 * {{{
 * export JAVA_HOME=/usr/lib/jvm/msopenjdk-17
 * export PATH="$JAVA_HOME/bin:$PATH"
 * export MAVEN_OPTS="${MAVEN_OPTS:-} -XX:ReservedCodeCacheSize=1g"
 * export SPARK_LOCAL_IP=127.0.0.1
 * export LD_LIBRARY_PATH="$NATIVE/linux/amd64:$JAVA_HOME/lib/server"
 * ./build/mvn -o -B -ntp -Pjava-17,backends-velox,spark-4.1,scala-2.13,delta,spark-ut \
 *   -pl gluten-ut/spark41 -am -DskipTests -Dspotless.check.skip=true \
 *   -Dcpp.releases.dir="$NATIVE/linux/amd64" \
 *   -Dmdep.outputFile=target/reviewer-classpath.txt package dependency:build-classpath
 *
 * CP=$(cat gluten-ut/spark41/target/reviewer-classpath.txt)
 * # Require current same-reactor Gluten paths, not cached same-version SNAPSHOT implementations.
 * case "$CP" in *"/.m2/repository/org/apache/gluten/"*) exit 1;; esac
 * CP="$NATIVE:$(pwd)/gluten-ut/spark41/target/scala-2.13/test-classes:$CP"
 * # JAVA_MODULE_OPTIONS is a new argfile, matching extraJavaTestArgs in this Spark 4.1 POM.
 * { printf '%s\n' '-XX:+IgnoreUnrecognizedVMOptions'
 *   for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.time \
 *     java.util java.util.concurrent java.util.concurrent.atomic jdk.internal.ref sun.nio.ch \
 *     sun.nio.cs sun.security.action sun.util.calendar; do
 *     printf '%s\n' "--add-opens=java.base/$p=ALL-UNNAMED"
 *   done
 *   printf '%s\n' '-Djdk.reflect.useDirectMethodHandle=false' \
 *     '-Dio.netty.tryReflectionSetAccessible=true' '-Dfile.encoding=UTF-8'
 * } > "$JAVA_MODULE_OPTIONS"
 * "$JAVA_HOME/bin/java" @"$JAVA_MODULE_OPTIONS" -Dspark.testing=true -Xms4g -Xmx4g -cp "$CP" \
 *   org.apache.spark.sql.execution.benchmark.RTrimExpressionBenchmark \
 *   4000000 10240 20260912 rtrim-l10-none both vanilla-first
 * }}}
 * Arguments: rows [native-batch-size [seed [case-name [engine [order]]]]]. Defaults: batch10240,
 * seed20260912, both, vanilla-first. Engines: both/vanilla/native. Orders:
 * vanilla-first/native-first. Omit case-name or use aligned for all41cases; representative
 * preserves the ordered12historical cases and their exact data. A case ID selects one. Engine
 * selection affects timing only: every mode checks both results against the untimed oracle.
 * TrimExpressionBenchmark combines this catalog with the independent LTrim catalog and documents
 * the three-process Vanilla/NativeA/NativeB run. Keep CPU, heap/GC/thread settings identical. Run
 * no concurrent builds or benchmarks. SPARK_GENERATE_BENCHMARK_FILES=1 enables standard output; use
 * a distinct working directory per process. Per-iteration Spark output and untimed
 * preparation/verification durations are reported separately.
 *
 * The aligned grid is five byte widths times none/first/cluster/half-even/last/penultimate/all. Row
 * positions are native-batch-local: first is row0, cluster is the first min(16,count) rows,
 * half-even selects even rows, and last includes the actual final row of a partial batch.
 * Penultimate swaps complete last/penultimate rows, including identity; a singleton is unchanged.
 * Selected values end with two spaces. The grid has no injected empty strings. Extras are NULL50,
 * UTF8-half-even, two identity controls, opposite-side preservation and two-input custom trim.
 * LTrimExpressionBenchmark independently generates the same bodies with leading target spaces.
 * Historical representative IDs, bytes and order remain unchanged; no old evaluator is restored.
 * Run RTrimExpressionBenchmarkSuite for the same definitions on small correctness inputs.
 */
object RTrimExpressionBenchmark extends BenchmarkBase {
  import ExpressionBenchmark.Case

  private val stringInput = new StructType().add("input", StringType, nullable = true)

  private def historicalCase(
      name: String,
      length: Int,
      pattern: String,
      nullPercent: Int = 0,
      utf8: Boolean = false,
      identity: Boolean = false): Case = {
    def input(seed: Long, index: Int, local: Int, count: Int): InternalRow = {
      // Penultimate retains the entire original row, including its identity and body bytes.
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
        val body = length - (if (trim) 2 else 0)
        val bytes = Array.fill[Byte](length)(' '.toByte)
        val rowIdentity = (global ^ seed) & 0xffffffffL
        val prefix = math.min(8, body)
        var position = 0
        while (position < prefix) {
          val digit = ((rowIdentity >>> ((prefix - position - 1) * 4)) & 15).toInt
          bytes(position) = (if (digit < 10) '0' + digit else 'a' + digit - 10).toByte
          position += 1
        }
        if (utf8) {
          val codePoint = 0x4e00 + Math.floorMod(global + seed, 128L).toInt
          while (position + 3 <= body) {
            bytes(position) = (0xe0 | (codePoint >>> 12)).toByte
            bytes(position + 1) = (0x80 | ((codePoint >>> 6) & 63)).toByte
            bytes(position + 2) = (0x80 | (codePoint & 63)).toByte
            position += 3
          }
        }
        while (position < body) {
          bytes(position) =
            ('a' + Math.floorMod(seed + global * 31 + position * 17, 26L).toInt).toByte
          position += 1
        }
        InternalRow(UTF8String.fromBytes(bytes))
      }
    }
    Case(
      name,
      stringInput,
      attrs => if (identity) Seq(attrs.head) else Seq(StringTrimRight(attrs.head)),
      (seed, index) => input(seed, index, index % 10240, 10240),
      Some(input _)
    )
  }

  private[benchmark] val representativeCases: Seq[Case] = Seq(
    historicalCase("rtrim-l10-none", 10, "none"),
    historicalCase("rtrim-l12-half-even", 12, "half-even"),
    historicalCase("rtrim-l13-half-even", 13, "half-even"),
    historicalCase("rtrim-l64-half-even", 64, "half-even"),
    historicalCase("rtrim-l256-none", 256, "none"),
    historicalCase("rtrim-l256-last", 256, "last"),
    historicalCase("rtrim-l256-penultimate", 256, "penultimate"),
    historicalCase("rtrim-l256-all", 256, "all"),
    historicalCase("rtrim-l64-null50", 64, "half-even", nullPercent = 50),
    historicalCase("rtrim-l64-utf8-half-even", 64, "half-even", utf8 = true),
    historicalCase("identity-l10", 10, "none", identity = true),
    historicalCase("identity-l256", 256, "none", identity = true)
  )

  private val grid: Seq[Case] = for {
    length <- Seq(10, 12, 13, 64, 256)
    pattern <- Seq("none", "first", "cluster", "half-even", "last", "penultimate", "all")
  } yield {
    val name = s"rtrim-l$length-$pattern"
    representativeCases.find(_.name == name).getOrElse(historicalCase(name, length, pattern))
  }

  private[benchmark] val cases: Seq[Case] = grid ++ representativeCases.takeRight(4) ++ Seq(
    Case(
      "rtrim-leading-boundary",
      stringInput,
      attrs => Seq(StringTrimRight(attrs.head)),
      (seed, index) => {
        val position = Math.floorMod(index.toLong + seed, 1000L)
        InternalRow(UTF8String.fromString(if (position % 101 == 0) "" else "  " + "x" * 60 + "  "))
      }
    ),
    Case(
      "rtrim-custom",
      new StructType()
        .add("input", StringType, nullable = true)
        .add("trimChars", StringType, nullable = true),
      attrs => Seq(StringTrimRight(attrs(0), Some(attrs(1)))),
      (seed, index) => {
        val position = Math.floorMod(index.toLong + seed, 6L).toInt
        val chars = if (position % 2 == 0) "xy" else " "
        // Matching prefix is a preservation boundary, not the trimming target.
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
