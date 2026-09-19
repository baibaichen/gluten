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

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

import org.scalatest.DoNotDiscover

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class ExpressionBenchmarkRunnerSuite extends SparkFunSuite {
  test("metric suite requires explicit selection instead of default discovery") {
    val suite = classOf[ExpressionBenchmarkMetricSuite]
    assert(java.lang.reflect.Modifier.isPublic(suite.getModifiers))
    assert(suite.getConstructors.exists(_.getParameterCount == 0))
    assert(suite.isAnnotationPresent(classOf[DoNotDiscover]))
  }

  test("real Spark timer excludes warmup and uses stopped samples for median and sample stdev") {
    val output = new ByteArrayOutputStream
    val calls = ArrayBuffer.empty[Int]
    val result = Console.withOut(output) {
      ExpressionBenchmark.measure("protocol", 1, warmupTime = 5.millis, minTime = 5.millis) {
        iteration =>
          calls += iteration
          Thread.sleep(1)
      }
    }
    assert(calls.contains(-1))
    assert(calls.filter(_ >= 0) == result.samples.indices)
    assert(result.samples.size >= 2 && result.samples.sum >= 5.millis.toNanos)
    assert(result.timing.medianMs == ExpressionBenchmark.medianMs(result.samples))
    val avg = result.samples.sum.toDouble / result.samples.size
    val sd = math.sqrt(result.samples.map(n => math.pow(n - avg, 2)).sum /
      (result.samples.size - 1)) / 1e6
    assert(math.abs(result.timing.stdevMs - sd) < 1e-9)
    assert(!output.toString.contains("Iteration"))
  }

  test("runner writes results to stdout without Git or result archives") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("3."), "Runner requires Spark 4")
    val directory = Files.createTempDirectory("expression-stdout")
    val stdout = directory.resolve("stdout.log")
    val stderr = directory.resolve("stderr.log")
    val java = new File(System.getProperty("java.home"), "bin/java").getPath
    val command = Seq(
      java,
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true",
      "-Dgluten.expressionBenchmark.diagnostics=true",
      "-Xmx2g",
      "-XX:+UnlockExperimentalVMOptions",
      "-XX:CompileCommand=blackhole," + classOf[BenchmarkBlackhole].getName + "::consume",
      "-cp",
      System.getProperty("java.class.path"),
      RegisteredExpressionBenchmark.getClass.getName.stripSuffix("$"),
      "--cases",
      "encode/standard-string,trim/standard-string",
      "--rows",
      "10000",
      "--warmup-seconds",
      "1",
      "--measurement-seconds",
      "1"
    )
    val builder = new ProcessBuilder(command: _*)
      .directory(directory.toFile)
      .redirectOutput(stdout.toFile).redirectError(stderr.toFile)
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    builder.environment().put(
      "GLUTEN_EXPRESSION_BENCHMARK_RUNS",
      directory.resolve("removed").toString)
    builder.environment().put("SPARK_GENERATE_BENCHMARK_FILES", "1")
    var process = builder.start()
    try {
      assert(process.waitFor(90, TimeUnit.SECONDS), "Benchmark timed out")
      val output = new String(Files.readAllBytes(stdout), UTF_8)
      val errors = new String(Files.readAllBytes(stderr), UTF_8)
      assert(process.exitValue() == 0, errors)
      assert(output.contains("trim/standard-string") && output.contains("median | stdev"))
      val measured = output.linesIterator.find(_.startsWith("trim/standard-string")).get
      assert(!measured.contains("—")) // scalastyle:ignore nonascii
      val header = output.linesIterator.find(_.startsWith("case")).get
      assert(header.split("\\s+").take(3).toSeq == Seq("case", "vanilla", "native"))
      assert(!errors.contains("allocatedBytes=") && !errors.contains("generatedClass="))
      assert(!errors.contains("nanoTime="))
      assert(!errors.contains("fallback=") && !errors.contains("profiled="))
      assert(!output.contains("SKIPPED") && errors.contains("SKIPPED encode/standard-string"))
      assert(!Files.exists(directory.resolve("removed")))
      assert(!Files.exists(directory.resolve("target/expression-benchmark-runs")))
      assert(!Files.exists(directory.resolve("benchmarks")))

      val profiler = Files.createDirectory(directory.resolve("profiler"))
      val library = Files.createDirectory(profiler.resolve("lib"))
        .resolve(System.mapLibraryName("asyncProfiler"))
      val failureCommand = command.map {
        case "encode/standard-string,trim/standard-string" => "trim/standard-string"
        case other => other
      } ++ Seq("--async-profiler", profiler.toString)
      builder.command(failureCommand: _*)
      process = builder.start()
      assert(process.waitFor(90, TimeUnit.SECONDS), "Failing benchmark timed out")
      val failure = new String(Files.readAllBytes(stderr), UTF_8)
      assert(process.exitValue() != 0 && failure.contains("UnsatisfiedLinkError"), failure)
      assert(failure.contains(library.toString), failure)
      assert(!failure.contains("SKIPPED"))
      assert(!new String(Files.readAllBytes(stdout), UTF_8).contains("SKIPPED"))
      assert(!Files.exists(directory.resolve("removed")))
    } finally {
      if (process.isAlive) process.destroyForcibly().waitFor()
      Utils.deleteRecursively(directory.toFile)
    }
  }

  test("summary ratios use unrounded medians and preserve engine columns and missing values") {
    import ExpressionBenchmark._
    assert(medianMs(Seq(1000000L, 9000000L, 3000000L)) == 3.0)
    assert(medianMs(Seq(1000000L, 2000000L)) == 1.5)
    val text = renderSummary(Seq(
      (
        "id",
        Some(Timing(0.0014, 0.0)),
        Some(Timing(0.001, 0.0))),
      ("one", None, Some(Timing(1, 0)))))
    assert(text.contains("1.40x") && text.contains("id"))
    assert(text.contains("vanilla") && text.contains("native") && !text.contains("Iteration"))
  }
}
