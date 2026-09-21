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
import org.apache.spark.util.Utils

import java.io.{ByteArrayOutputStream, IOException, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.duration._

class RunOptionsSuite extends SparkFunSuite {
  private def writeString(path: Path, text: String): Unit = {
    Files.write(path, text.getBytes(UTF_8))
  }

  private val catalog = Catalog.load()
  private def parse(args: String*): RunOptions.Parsed =
    RunOptions.parse(args.toArray)

  test("empty run and CLI listing need neither Spark nor compiler blackholes nor profiling") {
    val active = SparkSession.getActiveSession
    val default = SparkSession.getDefaultSession
    val directory = Files.createTempDirectory("empty-run")
    val output = directory.resolve("profiles")
    val options = RunOptions(
      Duration.Zero,
      Duration.Zero,
      profiler = Some(ProfilerOptions(directory.resolve("missing-profiler"), output = output)))
    val bytes = new ByteArrayOutputStream
    val out = new PrintStream(bytes)
    val oldOut = System.out
    try {
      System.setOut(out)
      RegisteredExpressionBenchmark.run(Seq.empty, options)
      assert(bytes.size() == 0 && !Files.exists(output))
      RegisteredExpressionBenchmark.run(Seq.empty, options.copy(profiler = None))
      RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--list", "trim"))
      assert(bytes.toString(UTF_8.name()).contains("trim/standard-string"))
      assert(SparkSession.getActiveSession == active && SparkSession.getDefaultSession == default)
      assert(!Files.exists(output))
    } finally {
      System.setOut(oldOut)
      out.close()
      Utils.deleteRecursively(directory.toFile)
    }
  }

  test("suite closes and clears standard output on success and parse failure") {
    Seq(false, true).foreach {
      invalid =>
        var closed = 0
        val stream = new ByteArrayOutputStream {
          override def close(): Unit = { closed += 1 }
        }
        assert(RegisteredExpressionBenchmark.output.isEmpty)
        RegisteredExpressionBenchmark.output = Some(stream)
        try {
          if (invalid) {
            intercept[IllegalArgumentException] {
              RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--unknown"))
            }
          } else RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--list", "trim"))
          assert(closed == 1 && RegisteredExpressionBenchmark.output.isEmpty)
        } finally RegisteredExpressionBenchmark.output = None
    }
  }

  test("suite preserves a primary parse error when closing standard output fails") {
    val cleanup = new IOException("output close failure")
    RegisteredExpressionBenchmark.output = Some(new ByteArrayOutputStream {
      override def close(): Unit = throw cleanup
    })
    try {
      val error = intercept[IllegalArgumentException] {
        RegisteredExpressionBenchmark.runBenchmarkSuite(Array("--unknown"))
      }
      assert(error.getMessage.contains("Unknown --unknown"))
      assert(error.getSuppressed.toSeq == Seq(cleanup))
      assert(RegisteredExpressionBenchmark.output.isEmpty)
    } finally RegisteredExpressionBenchmark.output = None
  }

  test("runtime input defaults resolve equally and cardinality follows overridden rows") {
    val options = RunOptions(Duration.Zero, Duration.Zero)
    assert(options.resolvedInput == options.copy(input = Some(InputOptions())).resolvedInput)
    assert(options.resolvedInput == InputOptions(4000000, 10240, 20260912L, None))
    val batched = RunOptions.withBatchSize(3)
    assert(batched.batchSize == 3)
    assert(batched.input.contains(InputOptions(batchSize = 3)))
    assert(batched.warmup == Duration.Zero && batched.minTime == Duration.Zero)
    val partial = options.copy(input = Some(InputOptions(rows = 17, seed = Long.MinValue)))
    val input = partial.resolvedInput
    assert(input.batchSize == 10240 && input.keyCardinality.isEmpty && input.seed == Long.MinValue)
    val context = Data.Context(input.rows, input.keyCardinality, input.seed)
    assert(context.rows == 17 && context.keys == 17 && context.seed == Long.MinValue)
    assert(InputOptions(seed = Long.MaxValue).seed == Long.MaxValue)
    assert(InputOptions(keyCardinality =
      Some(Long.MaxValue)).keyCardinality.contains(Long.MaxValue))
    Seq(0, -1).foreach {
      invalid =>
        intercept[IllegalArgumentException](InputOptions(rows = invalid))
        intercept[IllegalArgumentException](InputOptions(batchSize = invalid))
        intercept[IllegalArgumentException](RunOptions.withBatchSize(invalid))
        intercept[IllegalArgumentException](InputOptions(keyCardinality = Some(invalid.toLong)))
    }
    assert(Data.Context(0).rows == 0)
  }

  test("runtime timing permits zero but enforces nonnegative durations and at least two samples") {
    val options = RunOptions(Duration.Zero, Duration.Zero)
    assert(options.minNumIters == 2)
    assert(options.copy(minNumIters = 3).minNumIters == 3)
    Seq(-1, 0, 1).foreach(n => intercept[IllegalArgumentException](options.copy(minNumIters = n)))
    intercept[IllegalArgumentException](options.copy(warmup = -1.nanos))
    intercept[IllegalArgumentException](options.copy(minTime = -1.nanos))
  }

  test("runtime profiler is optional and only carries its own configuration") {
    val options = RunOptions(Duration.Zero, Duration.Zero)
    assert(options.profiler.isEmpty)
    val profiler = ProfilerOptions(Paths.get("profiler"))
    assert(profiler.event == "cpu")
    assert(profiler.output == Paths.get("target/expression-benchmark-profiles"))
    assert(options.copy(profiler = Some(profiler)).profiler.contains(profiler))
    assert(profiler.copy(event = "alloc").event == "alloc")
    intercept[IllegalArgumentException](profiler.copy(event = "unknown"))
    assert(parse("trim").runtime.profiler.isEmpty)
    assert(parse("trim").runtime.input.contains(InputOptions()))
  }

  test("list is pure and lists functions and full case definitions") {
    val all = parse("--list").listing(catalog)
    assert(all.linesIterator.size == 51)
    val subset = parse("--list", "rtrim,ltrim,trim").selected(catalog)
    assert(subset.count(_.id.startsWith("rtrim/")) == 42)
    assert(subset.count(_.id.startsWith("ltrim/")) == 42)
    assert(subset.count(_.id.startsWith("trim/")) == 1)
    val text = parse("--list", "trim").listing(catalog)
    assert(text.contains("trim/standard") && text.contains("trim(") && text.contains("inputs="))
  }

  test("selection preserves first occurrence and catalog order with slash-safe globs") {
    val substring = catalog.filter(_.id.startsWith("substring/"))
    assert(parse("substring,substring").selected(catalog) == substring)
    val chosen = parse("--cases", "substring/*,rtrim/l1?-half-even,substring/*")
      .selected(catalog)
    assert(chosen == substring ++ catalog.filter(c => c.id.matches("rtrim/l1.-half-even")))
    assertThrows[IllegalArgumentException](parse("--cases", "*").selected(catalog))
    assertThrows[IllegalArgumentException](parse("--cases", "trim/*,missing/*").selected(catalog))
  }

  test("empty unknown conflicting selectors and list execution flags fail") {
    Seq(
      Seq.empty,
      Seq("rtrim,,trim"),
      Seq("missing"),
      Seq("--cases", ""),
      Seq("trim", "--cases", "trim/*"),
      Seq("--list", "--rows", "1"),
      Seq("--list", "--config", "missing.json"),
      Seq("trim", "--profile-event", "cpu"),
      Seq("trim", "--profile-output", "x"),
      Seq("trim", "--unknown", "1")
    ).foreach(args => assertThrows[IllegalArgumentException](parse(args: _*).selected(catalog)))
    Seq("blackhole", "observable").foreach {
      value =>
        val error = intercept[IllegalArgumentException](parse("trim", "--consumer", value))
        assert(error.getMessage.contains("Unknown --consumer"))
    }
  }

  test("repeat is rejected in CLI and JSON") {
    Seq("--repeat", "--min-num-iters", "--iterations").foreach {
      flag =>
        val error = intercept[IllegalArgumentException](parse("trim", flag, "2"))
        assert(error.getMessage.contains(s"Unknown $flag"))
    }
    val file = Files.createTempFile("repeat-config", ".json")
    try {
      writeString(file, """{"functions":["trim"],"repeat":2}""")
      val error = intercept[IllegalArgumentException](parse("--config", file.toString))
      assert(error.getMessage.contains("Unknown config key"))
    } finally Files.delete(file)
  }

  test("removed CLI and JSON options and selector aliases are rejected") {
    Seq(Seq("--smoke"), Seq("--normal"), Seq("--engine", "both"), Seq("--order", "vanilla-first"))
      .foreach {
        args =>
          val error = intercept[IllegalArgumentException](parse(Seq("trim") ++ args: _*))
          assert(error.getMessage.contains(s"Unknown ${args.head}"))
      }
    val file = Files.createTempFile("removed-options", ".json")
    try {
      Seq("\"smoke\":true", "\"engine\":\"both\"", "\"order\":\"vanilla-first\"").foreach {
        field =>
          writeString(file, s"""{"functions":["trim"],$field}""")
          val error = intercept[IllegalArgumentException](parse("--config", file.toString))
          assert(error.getMessage.contains("Unknown config key"))
      }
    } finally Files.delete(file)
    assertThrows[IllegalArgumentException](parse("substr").selected(catalog))
    assertThrows[IllegalArgumentException](parse("--cases", "substr/*").selected(catalog))
  }

  test("defaults and input sizes are strict without forcing iteration counts") {
    val options = parse("trim")
    val input = options.runtime.resolvedInput
    assert(input.rows == 4000000 && input.batchSize == 10240)
    assert(input.seed == 20260912L && input.keyCardinality.isEmpty)
    assert(options.runtime.warmup.toSeconds == 10 && options.runtime.minTime.toSeconds == 60)
    assert(options.runtime.minNumIters == 2)
    Seq("--rows", "--batch-size").foreach {
      flag =>
        Seq("0", "-1", "2147483648", "1.5").foreach {
          value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
        }
    }
    assert(parse(
      "trim",
      "--seed",
      Long.MinValue.toString).runtime.resolvedInput.seed == Long.MinValue)
    assertThrows[IllegalArgumentException](parse("trim", "--key-cardinality", "0"))
    assertThrows[IllegalArgumentException](parse("trim", "--seed", "9223372036854775808"))
  }

  test("explicit duration pair uses 2 plus 10 seconds without forcing iteration counts") {
    val options = parse("trim", "--warmup-seconds", "2", "--measurement-seconds", "10")
    assert(options.runtime.warmup.toSeconds == 2 && options.runtime.minTime.toSeconds == 10)
    assert(options.runtime.minNumIters == 2)
    val max = parse(
      "trim",
      "--warmup-seconds",
      Int.MaxValue.toString,
      "--measurement-seconds",
      Int.MaxValue.toString)
    assert(max.runtime.warmup.toNanos == Int.MaxValue.toLong * 1000000000L)
    assert(max.runtime.minTime.toNanos == Int.MaxValue.toLong * 1000000000L)
  }

  test("duration pairs are strict positive Int values and conflict with listing") {
    Seq("--warmup-seconds", "--measurement-seconds").foreach {
      flag =>
        assertThrows[IllegalArgumentException](parse("trim", flag, "2"))
        Seq("0", "-1", "2147483648", "9223372036854775807", "1.5", "+2", " 2", "2e0")
          .foreach {
            value =>
              val other = if (flag == "--warmup-seconds") "--measurement-seconds"
              else "--warmup-seconds"
              assertThrows[IllegalArgumentException](parse("trim", flag, value, other, "10"))
          }
    }
    assertThrows[IllegalArgumentException](parse(
      "trim",
      "--list",
      "--warmup-seconds",
      "2",
      "--measurement-seconds",
      "10"))
    assertThrows[IllegalArgumentException](parse(
      "trim",
      "--warmup-seconds",
      "2",
      "--measurement-seconds",
      "10",
      "--warmup-seconds",
      "3"))
    assertThrows[IllegalArgumentException](parse(
      "trim",
      "--warmup-seconds",
      "2",
      "--measurement-seconds"))
  }

  test("duration overrides validate each source and replace whole timing groups") {
    val cwd = Files.createTempDirectory("duration-cwd")
    val dir = Files.createDirectory(cwd.resolve("config"))
    val file = dir.resolve("run.json")
    def fromFile(args: String*): RunOptions =
      RunOptions.parse(Array("--config", "config/run.json") ++ args, cwd).runtime
    def write(fields: String): Unit =
      writeString(file, s"""{"functions":["trim"],$fields}""")
    try {
      write(""""warmupSeconds":2,"measurementSeconds":10""")
      val unchanged = fromFile("--rows", "17")
      assert(unchanged.warmup.toSeconds == 2 && unchanged.minTime.toSeconds == 10)
      val replaced = fromFile("--warmup-seconds", "3", "--measurement-seconds", "11")
      assert(replaced.warmup.toSeconds == 3 && replaced.minTime.toSeconds == 11)
      assertThrows[IllegalArgumentException](fromFile("--warmup-seconds", "3"))
      assertThrows[IllegalArgumentException](fromFile("--measurement-seconds", "11"))
      Seq(
        """"warmupSeconds":2""",
        """"measurementSeconds":10""",
        """"warmupSeconds":0,"measurementSeconds":10""",
        """"warmupSeconds":2,"measurementSeconds":2147483648""",
        """"warmupSeconds":2.0,"measurementSeconds":10""",
        """"warmupSeconds":"2","measurementSeconds":10"""
      ).foreach {
        fields =>
          write(fields)
          assertThrows[IllegalArgumentException](fromFile())
          assertThrows[IllegalArgumentException](fromFile(
            "--warmup-seconds",
            "2",
            "--measurement-seconds",
            "10"))
      }
    } finally Utils.deleteRecursively(cwd.toFile)
  }

  test("strict JSON types keys UTF8 integers and trailing tokens") {
    val file = Files.createTempFile("expression-options", ".json")
    try {
      Seq(
        "{}",
        "[]",
        "{\"functions\":[\"trim\"],\"rows\":1.0}",
        "{\"functions\":[\"trim\"],\"rows\":2147483648}",
        "{\"functions\":[\"trim\"],\"seed\":9223372036854775808}",
        "{\"functions\":[\"trim\"],\"rows\":1,\"rows\":2}",
        "{\"functions\":[\"trim\"],\"sql\":\"trim(x)\"}",
        "{\"functions\":[1]}",
        "{\"functions\":[]}",
        "{\"functions\":[\"trim\"],\"cases\":[\"trim/*\"]}",
        "{\"functions\":[\"trim\"]} {}"
      ).foreach {
        json =>
          writeString(file, json)
          assertThrows[IllegalArgumentException](parse("--config", file.toString))
      }
      Seq("blackhole", "observable").foreach {
        value =>
          writeString(file, s"""{"functions":["trim"],"consumer":"$value"}""")
          val error = intercept[IllegalArgumentException](parse(
            "--config",
            file.toString,
            "--warmup-seconds",
            "2",
            "--measurement-seconds",
            "10"))
          assert(error.getMessage.contains("Unknown config key"))
      }
      val invalidUtf8 = """{"x":1}""".getBytes(UTF_8)
      invalidUtf8(2) = -1
      Files.write(file, invalidUtf8)
      assertThrows[IllegalArgumentException](parse("--config", file.toString))
    } finally Files.delete(file)
  }

  test("path options reject empty or blank values without trimming valid space paths") {
    Seq("", "   ").foreach {
      blank =>
        assertThrows[IllegalArgumentException](parse("trim", "--async-profiler", blank))
        assertThrows[IllegalArgumentException](parse(
          "trim",
          "--async-profiler",
          "ap",
          "--profile-output",
          blank))
        assertThrows[IllegalArgumentException](parse("--config", blank))
        val file = Files.createTempFile("blank-path-config", ".json")
        try {
          writeString(file, s"""{"functions":["trim"],"asyncProfiler":"$blank"}""")
          assertThrows[IllegalArgumentException](parse("--config", file.toString))
          writeString(
            file,
            s"""{"functions":["trim"],"asyncProfiler":"ap","profileOutput":"$blank"}""")
          assertThrows[IllegalArgumentException](parse("--config", file.toString))
        } finally Files.delete(file)
    }
    assert(parse("trim", "--async-profiler", "dir with spaces ").runtime.profiler.get.home
      .getFileName.toString == "dir with spaces ")
  }

  test("invalid JSON field types cannot be hidden by CLI overrides") {
    val file = Files.createTempFile("invalid-overridden-config", ".json")
    try {
      writeString(file, """{"functions":["trim"],"rows":"bad"}""")
      assertThrows[IllegalArgumentException](parse("--config", file.toString, "--rows", "17"))
      writeString(file, """{"functions":[1],"rows":17}""")
      assertThrows[IllegalArgumentException](parse("--config", file.toString, "trim"))
    } finally Files.delete(file)
  }

  Seq(
    ("profileEvent", "unknown", "--profile-event", "cpu"),
    ("asyncProfiler", "", "--async-profiler", "ap"),
    ("asyncProfiler", "   ", "--async-profiler", "ap"),
    ("profileOutput", "", "--profile-output", "out"),
    ("profileOutput", "   ", "--profile-output", "out")
  ).foreach {
    case (key, value, flag, replacement) =>
      test(s"invalid JSON $key='$value' cannot be hidden by a valid CLI override") {
        val file = Files.createTempFile("invalid-config-values", ".json")
        try {
          writeString(file, s"""{"functions":["trim"],"$key":"$value"}""")
          val profiler = if (key == "asyncProfiler") Nil else Seq("--async-profiler", "ap")
          assertThrows[IllegalArgumentException](parse(
            Seq("--config", file.toString, flag, replacement) ++ profiler: _*))
        } finally Files.delete(file)
      }
  }

  test("each scalar source is strict before valid overrides and selector replacement") {
    val file = Files.createTempFile("scalar-config-values", ".json")
    try {
      Seq(
        ("rows", "0", "--rows"),
        ("batchSize", "2147483648", "--batch-size"),
        ("rows", "false", "--rows"),
        ("seed", "9223372036854775808", "--seed"),
        ("keyCardinality", "-1", "--key-cardinality")
      ).foreach {
        case (key, value, flag) =>
          writeString(file, s"""{"cases":["trim/*"],"$key":$value}""")
          assertThrows[IllegalArgumentException](parse(
            "--config",
            file.toString,
            "trim",
            flag,
            "17"))
      }
      writeString(
        file,
        s"""{"cases":[" trim/* "],"seed":${Long.MinValue},""" +
          s""""keyCardinality":${Long.MaxValue},"rows":2}""")
      val options = parse("--config", file.toString, "substring", "--rows", "3")
      assert(options.cases.isEmpty && options.functions == Seq("substring"))
      val input = options.runtime.resolvedInput
      assert(input.seed == Long.MinValue && input.keyCardinality.contains(Long.MaxValue))
      assert(options.runtime.resolvedInput.rows == 3)
      Seq("--rows", "--seed").foreach {
        flag =>
          Seq("+1", " 1", "1e2").foreach {
            value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
          }
          assertThrows[IllegalArgumentException](parse("trim", flag, "1", flag, "2"))
          assertThrows[IllegalArgumentException](parse("trim", flag))
      }
      assert(parse("trim", "--rows", "001").runtime.resolvedInput.rows == 1)
      assert(parse("trim", "--seed", "-0").runtime.resolvedInput.seed == 0)
    } finally Files.delete(file)
  }

  test("CLI scalars and whole selectors override JSON and paths keep their own origin") {
    val cwd = Files.createTempDirectory("expression-cwd")
    val dir = Files.createDirectory(cwd.resolve("config"))
    val config = dir.resolve("run.json")
    try {
      writeString(
        config,
        """{"functions":["trim"],"rows":19,""" +
          """"asyncProfiler":"ap","profileOutput":"out"}""")
      val fromJson = RunOptions.parse(Array("--config", "config/run.json"), cwd)
      assert(fromJson.runtime.profiler.get.home == dir.resolve("ap"))
      assert(fromJson.runtime.profiler.get.output == dir.resolve("out"))
      val overrideJson = RunOptions.parse(
        Array(
          "--config",
          "config/run.json",
          "--cases",
          "rtrim/l13-half-even",
          "--rows",
          "17",
          "--async-profiler",
          "cli-ap",
          "--profile-output",
          "cli-out"),
        cwd
      )
      assert(overrideJson.selected(catalog).map(_.id) == Seq("rtrim/l13-half-even"))
      assert(overrideJson.runtime.resolvedInput.rows == 17)
      assert(overrideJson.runtime.profiler.get.home == cwd.resolve("cli-ap"))
      assert(overrideJson.runtime.profiler.get.output == cwd.resolve("cli-out"))
    } finally Utils.deleteRecursively(cwd.toFile)
  }
}
