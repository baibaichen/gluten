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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

class ExpressionBenchmarkRunOptionsSuite extends SparkFunSuite {
  private def writeString(path: Path, text: String): Unit = {
    Files.write(path, text.getBytes(UTF_8))
  }

  private val catalog = ExpressionBenchmarkCatalog.load()
  private def parse(args: String*): ExpressionBenchmarkRunOptions =
    ExpressionBenchmarkRunOptions.parse(args.toArray)

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
    val error = intercept[IllegalArgumentException](parse("trim", "--repeat", "2"))
    assert(error.getMessage.contains("Unknown --repeat"))
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
          val error = intercept[IllegalArgumentException](parse((Seq("trim") ++ args): _*))
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
    assert(options.rows == 4000000 && options.batchSize == 10240)
    assert(options.seed == 20260912L && options.keyCardinality.isEmpty)
    assert(options.warmup.toSeconds == 10 && options.minTime.toSeconds == 60)
    assert(options.minNumIters == 2)
    Seq("--rows", "--batch-size").foreach {
      flag =>
        Seq("0", "-1", "2147483648", "1.5").foreach {
          value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
        }
    }
    assert(parse("trim", "--seed", Long.MinValue.toString).seed == Long.MinValue)
    assertThrows[IllegalArgumentException](parse("trim", "--key-cardinality", "0"))
    assertThrows[IllegalArgumentException](parse("trim", "--seed", "9223372036854775808"))
  }

  test("explicit duration pair uses 2 plus 10 seconds without forcing iteration counts") {
    val options = parse("trim", "--warmup-seconds", "2", "--measurement-seconds", "10")
    assert(options.warmup.toSeconds == 2 && options.minTime.toSeconds == 10)
    assert(options.minNumIters == 2)
    val max = parse(
      "trim",
      "--warmup-seconds",
      Int.MaxValue.toString,
      "--measurement-seconds",
      Int.MaxValue.toString)
    assert(max.warmup.toNanos == Int.MaxValue.toLong * 1000000000L)
    assert(max.minTime.toNanos == Int.MaxValue.toLong * 1000000000L)
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
    def fromFile(args: String*): ExpressionBenchmarkRunOptions =
      ExpressionBenchmarkRunOptions.parse(Array("--config", "config/run.json") ++ args, cwd)
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
      Files.write(file, Array[Byte]('{', '"', -1, '"', ':', '1', '}'))
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
    assert(parse("trim", "--async-profiler", "dir with spaces ").asyncProfiler.get
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
            (Seq("--config", file.toString, flag, replacement) ++ profiler): _*))
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
      assert(options.seed == Long.MinValue && options.keyCardinality.contains(Long.MaxValue))
      assert(options.rows == 3)
      Seq("--rows", "--seed").foreach {
        flag =>
          Seq("+1", " 1", "1e2").foreach {
            value => assertThrows[IllegalArgumentException](parse("trim", flag, value))
          }
          assertThrows[IllegalArgumentException](parse("trim", flag, "1", flag, "2"))
          assertThrows[IllegalArgumentException](parse("trim", flag))
      }
      assert(parse("trim", "--rows", "001").rows == 1)
      assert(parse("trim", "--seed", "-0").seed == 0)
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
      val fromJson = ExpressionBenchmarkRunOptions.parse(Array("--config", "config/run.json"), cwd)
      assert(fromJson.asyncProfiler.contains(dir.resolve("ap")))
      assert(fromJson.profileOutput == dir.resolve("out"))
      val overrideJson = ExpressionBenchmarkRunOptions.parse(
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
      assert(overrideJson.rows == 17)
      assert(overrideJson.asyncProfiler.contains(cwd.resolve("cli-ap")))
      assert(overrideJson.profileOutput == cwd.resolve("cli-out"))
    } finally Utils.deleteRecursively(cwd.toFile)
  }
}
