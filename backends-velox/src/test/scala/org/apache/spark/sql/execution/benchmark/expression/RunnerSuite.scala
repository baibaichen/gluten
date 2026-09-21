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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.GlutenImplicits.withSQLConf
import org.apache.spark.sql.execution.benchmark.expression.Catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

class RunnerSuite extends SparkFunSuite {
  test("case suite registers exactly the catalog and six explicit ignores") {
    val suite = new BenchmarkSuite
    val catalog = Catalog.load().map(_.id).toSet
    assert(suite.testNames.filter(_.matches("[a-z0-9_-]+/[a-z0-9_-]+")) == catalog)
    val ignored = suite.tags.collect {
      case (name, tags) if tags.contains("org.scalatest.Ignore") => name
    }.toSet
    val unsupported = Set(
      "array_sort/int-array-lambda",
      "bround/standard-double",
      "encode/standard-string",
      "format_string/standard-string",
      "sequence/bounded-int",
      "substring/binary"
    )
    assert(ignored == (if (suite.matchSparkVersion(Some("4.0"))) unsupported else catalog))
  }

  test("constructor is inert without blackhole, Spark session or task resources") {
    BenchmarkBlackhole.requireEnabled(false)
    intercept[IllegalStateException](BenchmarkBlackhole.requireEnabled(true))
    assert(!TaskResources.inSparkTask())
    val directory = Files.createTempDirectory("expression-constructor")
    val profile = directory.resolve("not-created")
    val scenario = CaseDef(
      "constructor/inert",
      Seq(Binding(Seq("input"), "unknown-generator", Seq.empty)),
      "invalid sql +",
      "No eager preparation",
      SourceLocation("constructor", 1, 1)
    )
    val options = RunOptions(Duration.Zero, Duration.Zero)
    try {
      Seq(options, options.copy(profiler = Some(ProfilerOptions(directory, output = profile))))
        .foreach {
          runtime =>
            val constructed = scala.util.Try(new Benchmark(
              null,
              scenario,
              Data.Context(0),
              runtime,
              profiler = runtime.profiler.map(Profiler.apply)))
            assert(constructed.isSuccess, s"Constructor performed preparation: $constructed")
            assert(constructed.get.benchmarks.isEmpty)
            assert(!Files.exists(profile))
            assert(!TaskResources.inSparkTask())
        }
    } finally Utils.deleteRecursively(directory.toFile)
  }

  test("direct run uses supplied case order and input definitions without catalog selection") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("3."), "Runner requires Spark 4")
    val directory = Files.createTempDirectory("expression-direct-run")
    val log = directory.resolve("child.log")
    val builder = new ProcessBuilder(blackholeJavaCommand :+ getClass.getName: _*)
      .directory(directory.toFile)
      .redirectErrorStream(true).redirectOutput(log.toFile)
    builder.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    val process = builder.start()
    try {
      assert(process.waitFor(90, TimeUnit.SECONDS), "Direct-run test timed out")
      assert(process.exitValue() == 0, new String(Files.readAllBytes(log), UTF_8))
    } finally {
      if (process.isAlive) process.destroyForcibly().waitFor()
      Utils.deleteRecursively(directory.toFile)
    }
  }

  private[expression] def checkDirectRun(): Unit = {
    val second = CaseDef(
      "memory/second",
      Seq(Binding(Seq("right_input"), "standard.long", Seq.empty)),
      "right_input + 1",
      "Supplied long input",
      SourceLocation("memory", 1, 1))
    val first = CaseDef(
      "memory/first",
      Seq(Binding(Seq("left_input"), "standard.string", Seq("length" -> LongArgument(3)))),
      "rtrim(left_input)",
      "Supplied string input",
      SourceLocation("memory", 2, 1)
    )
    val options = RunOptions(
      Duration.Zero,
      Duration.Zero,
      input = Some(InputOptions(rows = 7, batchSize = 3, seed = -7L)))
    val bytes = new ByteArrayOutputStream
    val out = new PrintStream(bytes)
    val oldOut = System.out
    val oldErr = System.err
    val errors = new PrintStream(new ByteArrayOutputStream)
    val leaks = TaskResources.ACCUMULATED_LEAK_BYTES.get()
    try {
      System.setOut(out)
      System.setErr(errors)
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
        RegisteredExpressionBenchmark.run(Seq(second, first), options)
        assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
      }
      val text = bytes.toString(UTF_8.name())
      val results = text.linesIterator.filter(_.startsWith("memory/")).toSeq
      assert(results.map(_.takeWhile(_ != ' ').stripSuffix(":")) == Seq(second.id, first.id))
      assert(text.contains("Best Time(ms)") && text.contains("Avg Time(ms)"))
      assert(text.contains("Stdev(ms)") && text.contains("Relative"))
      assert(!text.contains("median"))
      val engines =
        text.linesIterator.filter(l => l.startsWith("vanilla") || l.startsWith("native"))
          .map(_.takeWhile(_ != ' ')).toSeq
      assert(engines == Seq("vanilla", "native", "vanilla", "native"))
      assert(!TaskResources.inSparkTask())
      assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
      assert(SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty)

      bytes.reset()
      val failure = withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
        val error = intercept[IllegalArgumentException] {
          RegisteredExpressionBenchmark.run(Seq(first, second.copy(sql = "missing + 1")), options)
        }
        assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
        error
      }
      assert(failure.getMessage.contains("memory/second") && failure.getCause != null)
      val partial = bytes.toString(UTF_8.name())
      assert(partial.linesIterator.exists(_.startsWith(first.id)))
      assert(!partial.linesIterator.exists(_.startsWith(second.id)))
      assert(!TaskResources.inSparkTask())
      assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
      assert(SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty)

      Seq(false, true).foreach {
        interrupted =>
          val primary = if (interrupted) new InterruptedException("Spark statistics")
          else new IllegalStateException("Spark statistics")
          val console = new PrintStream(new ByteArrayOutputStream) {
            override def println(value: Any): Unit = { // scalastyle:ignore println
              if (String.valueOf(value).contains("Stopped after")) throw primary
            }
          }
          try Console.withOut(console) {
              withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Tokyo") {
                val thrown = intercept[Exception] {
                  RegisteredExpressionBenchmark.run(Seq(first), options)
                }
                if (interrupted) assert(thrown eq primary)
                else {
                  assert(thrown.isInstanceOf[IllegalArgumentException])
                  assert(thrown.getCause eq primary)
                }
                assert(thrown.getSuppressed.isEmpty && primary.getSuppressed.isEmpty)
                assert(SQLConf.get.sessionLocalTimeZone == "Asia/Tokyo")
              }
              assert(Thread.currentThread().isInterrupted == interrupted)
              assert(!TaskResources.inSparkTask())
              assert(TaskResources.ACCUMULATED_LEAK_BYTES.get() == leaks)
              assert(
                SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty)
            }
          finally {
            Thread.interrupted()
            console.close()
          }
      }
    } finally {
      System.setOut(oldOut)
      System.setErr(oldErr)
      out.close()
      errors.close()
    }
  }

  private def blackholeJavaCommand: Seq[String] = Seq(
    new File(System.getProperty("java.home"), "bin/java").getPath,
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dspark.testing=true",
    "-Dgluten.expressionBenchmark.diagnostics=true",
    "-Xmx2g",
    "-XX:+UnlockExperimentalVMOptions",
    "-XX:CompileCommand=blackhole," + classOf[BenchmarkBlackhole].getName + "::consume",
    "-cp",
    System.getProperty("java.class.path")
  )

  test("runner inherits stdout and opt-in Spark result files without custom archives") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("3."), "Runner requires Spark 4")
    val directory = Files.createTempDirectory("expression-stdout")
    val stdout = directory.resolve("stdout.log")
    val stderr = directory.resolve("stderr.log")
    val command = blackholeJavaCommand ++ Seq(
      RegisteredExpressionBenchmark.getClass.getName.stripSuffix("$"),
      "--cases",
      "encode/standard-string,trim/standard-string",
      "--rows",
      "1000",
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
    builder.environment().remove("SPARK_GENERATE_BENCHMARK_FILES")
    var process = builder.start()
    try {
      assert(process.waitFor(90, TimeUnit.SECONDS), "Benchmark timed out")
      val output = new String(Files.readAllBytes(stdout), UTF_8)
      val errors = new String(Files.readAllBytes(stderr), UTF_8)
      assert(process.exitValue() == 0, errors)
      assert(output.contains("trim/standard-string:") && output.contains("Best Time(ms)"))
      assert(output.contains("Avg Time(ms)") && output.contains("Stdev(ms)"))
      assert(output.contains("Rate(M/s)") && output.contains("Per Row(ns)"))
      assert(output.contains("Relative") && !output.contains("median"))
      assert(output.linesIterator.filter(l => l.startsWith("vanilla") || l.startsWith("native"))
        .map(_.takeWhile(_ != ' ')).toSeq == Seq("vanilla", "native"))
      assert(!errors.contains("allocatedBytes=") && !errors.contains("generatedClass="))
      assert(!errors.contains("nanoTime="))
      assert(!errors.contains("fallback=") && !errors.contains("profiled="))
      assert(!output.contains("SKIPPED") && errors.contains("SKIPPED encode/standard-string"))
      assert(!Files.exists(directory.resolve("removed")))
      assert(!Files.exists(directory.resolve("target/expression-benchmark-runs")))
      assert(!Files.exists(directory.resolve("target/expression-benchmark-profiles")))
      assert(!Files.exists(directory.resolve("benchmarks")))

      builder.environment().put("SPARK_GENERATE_BENCHMARK_FILES", "1")
      process = builder.start()
      assert(process.waitFor(90, TimeUnit.SECONDS), "File-output benchmark timed out")
      assert(process.exitValue() == 0, new String(Files.readAllBytes(stderr), UTF_8))
      val resultFile = directory.resolve("benchmarks/RegisteredExpressionBenchmark-results.txt")
      assert(Files.isRegularFile(resultFile))
      val fileOutput = new String(Files.readAllBytes(resultFile), UTF_8)
      assert(fileOutput.contains("trim/standard-string") && fileOutput.contains("Best Time(ms)"))
      assert(fileOutput.contains("vanilla") && fileOutput.contains("native"))
      assert(!fileOutput.contains("median"))
      assert(!Files.exists(directory.resolve("target/expression-benchmark-profiles")))

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

}

object RunnerSuite {
  def main(args: Array[String]): Unit = new RunnerSuite().checkDirectRun()
}
