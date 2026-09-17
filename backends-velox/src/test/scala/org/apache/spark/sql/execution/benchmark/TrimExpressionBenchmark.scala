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

/**
 * Runs RTrim41 then LTrim41 through one shared runner and one final backend shutdown. Catalogs and
 * generators remain owned by their independent entrypoints; only result labels are qualified here.
 * See RTrimExpressionBenchmark for the Java17, offline full-reactor and native/classpath setup. Run
 * these commands sequentially, each in its own new output cwd and matching library environment:
 * {{{
 * # CP_A/CP_B and JAVA_MODULE_OPTIONS must be absolute, frozen, verified paths.
 * # Example affinity0-3 means four logical CPUs; choose the same available CPUs for every process.
 * JAVA_OPTS="-Dspark.testing=true -Xms4g -Xmx4g -XX:+UseG1GC"
 * JAVA_OPTS="$JAVA_OPTS -XX:ActiveProcessorCount=4 -XX:ReservedCodeCacheSize=1g"
 * SPARK_GENERATE_BENCHMARK_FILES=1 taskset -c 0-3 \
 *   "$JAVA_HOME/bin/java" @"$JAVA_MODULE_OPTIONS" $JAVA_OPTS -cp "$CP_A" \
 *   org.apache.spark.sql.execution.benchmark.TrimExpressionBenchmark \
 *   4000000 10240 20260912 aligned vanilla vanilla-first
 * SPARK_GENERATE_BENCHMARK_FILES=1 taskset -c 0-3 \
 *   "$JAVA_HOME/bin/java" @"$JAVA_MODULE_OPTIONS" $JAVA_OPTS -cp "$CP_A" \
 *   org.apache.spark.sql.execution.benchmark.TrimExpressionBenchmark \
 *   4000000 10240 20260912 aligned native vanilla-first
 * SPARK_GENERATE_BENCHMARK_FILES=1 taskset -c 0-3 \
 *   "$JAVA_HOME/bin/java" @"$JAVA_MODULE_OPTIONS" $JAVA_OPTS -cp "$CP_B" \
 *   org.apache.spark.sql.execution.benchmark.TrimExpressionBenchmark \
 *   4000000 10240 20260912 aligned native vanilla-first
 * }}}
 * Each JVM times one engine over82cases: three processes and246engine/case groups, not per-case
 * forks. Full input/content/property checks are untimed; each case is released before the next. A
 * qualified ID such as rtrim/identity-l10 selects one case. Unqualified IDs are not accepted. The
 * uniform native comparison uses A752d before both optimizations and B ab79 after both; older
 * LTrim6bec-baseline results have a different version contrast and must remain separate. Preserve
 * all iterations and source/jar/native identities. Within-process iteration variation is not
 * cross-process variance; this is expression plus byte-length/NULL consumption, not pure trim.
 */
object TrimExpressionBenchmark extends BenchmarkBase {
  private[benchmark] val cases: Seq[ExpressionBenchmark.Case] =
    RTrimExpressionBenchmark.cases.map(c => c.copy(name = s"rtrim/${c.name}")) ++
      LTrimExpressionBenchmark.cases.map(c => c.copy(name = s"ltrim/${c.name}"))

  private[benchmark] val collections: Map[String, Seq[ExpressionBenchmark.Case]] =
    Map("aligned" -> cases)

  override def runBenchmarkSuite(args: Array[String]): Unit =
    ExpressionBenchmark.run(this, cases, args, collections)
}
