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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

final private[benchmark] case class ExpressionBenchmarkRunOptions(
    functions: Seq[String],
    cases: Seq[String],
    list: Boolean,
    rows: Int,
    batchSize: Int,
    seed: Long,
    keyCardinality: Option[Long],
    asyncProfiler: Option[Path],
    profileEvent: String,
    profileOutput: Path,
    warmup: FiniteDuration,
    minTime: FiniteDuration) {
  def minNumIters: Int = 2

  def selected(catalog: Seq[ExpressionBenchmarkCatalog.CaseDef])
      : Seq[ExpressionBenchmarkCatalog.CaseDef] = {
    val groups = if (cases.nonEmpty) cases.map {
      pattern =>
        require(pattern.indexOf('/') > 0, s"Expected function/case pattern: $pattern")
        val regex = pattern.map {
          case '*' => "[^/]*"
          case '?' => "[^/]"
          case c => Pattern.quote(c.toString)
        }.mkString.r
        val matches = catalog.filter(c => regex.pattern.matcher(c.id).matches())
        require(matches.nonEmpty, s"No cases match: $pattern")
        matches
    }
    else functions.map {
      function =>
        val matches = catalog.filter(_.id.startsWith(function + "/"))
        require(matches.nonEmpty, s"Unknown function: $function")
        matches
    }
    groups.flatten.distinct
  }

  def listing(catalog: Seq[ExpressionBenchmarkCatalog.CaseDef]): String = {
    require(list, "Not a list request")
    if (functions.isEmpty) {
      catalog.map(_.id.takeWhile(_ != '/')).distinct.mkString("\n")
    } else selected(catalog).map {
      c => s"${c.id}\n  inputs=${c.inputs.mkString(", ")}\n  sql=${c.sql}\n  ${c.description}"
    }.mkString("\n")
  }
}

private[benchmark] object ExpressionBenchmarkRunOptions {
  val usage: String = "Usage: dev/run-expression-bench.sh functionsCSV [options] | " +
    "--cases function/case,glob [options] | --config file [options] | --list [functionsCSV]"
  private val names = Map(
    "rows" -> "rows",
    "batch-size" -> "batchSize",
    "seed" -> "seed",
    "key-cardinality" -> "keyCardinality",
    "warmup-seconds" -> "warmupSeconds",
    "measurement-seconds" -> "measurementSeconds",
    "async-profiler" -> "asyncProfiler",
    "profile-event" -> "profileEvent",
    "profile-output" -> "profileOutput"
  )
  private val selectors = Set("functions", "cases")
  private val durations = Set("warmupSeconds", "measurementSeconds")
  private val integers = Set("rows", "batchSize", "seed", "keyCardinality") ++ durations
  private val choices = Map("profileEvent" -> Set("cpu", "alloc"))
  private val fields = names.values.toSet ++ selectors
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  private def validate(values: collection.Map[String, JsonNode]): Unit = {
    require(!selectors.forall(values.contains), "Selectors conflict")
    if (durations.exists(values.contains)) {
      require(
        durations.forall(values.contains),
        "warmupSeconds and measurementSeconds must be paired")
    }
    values.foreach {
      case (key, n) =>
        key match {
          case "functions" | "cases" =>
            require(
              n.isArray && n.size() > 0 && n.elements().asScala.forall(
                v => v.isTextual && v.textValue().trim.nonEmpty),
              s"$key must be a nonempty string array")
          // Positive Int seconds also fit exactly in Long nanoseconds.
          case "rows" | "batchSize" | "warmupSeconds" | "measurementSeconds" =>
            require(
              n.isIntegralNumber && n.canConvertToInt && n.intValue() > 0,
              s"$key must be a positive Int")
          case "seed" | "keyCardinality" =>
            require(
              n.isIntegralNumber && n.canConvertToLong &&
                (key == "seed" || n.longValue() > 0),
              s"Invalid Long field: $key")
          case _ =>
            require(n.isTextual, s"$key must be a string")
            choices.get(key).foreach(
              allowed =>
                require(allowed(n.textValue()), s"Unknown $key: ${n.textValue()}"))
            if (key == "asyncProfiler" || key == "profileOutput") {
              require(n.textValue().trim.nonEmpty, "Path must not be empty or blank")
            }
        }
    }
  }

  def callerDirectory: Path = Paths.get(
    System.getProperty("gluten.expressionBenchmark.cwd", System.getProperty("user.dir")))
    .toAbsolutePath.normalize()

  private def csv(value: String): Seq[String] = {
    val values = value.split(",", -1).toSeq.map(_.trim)
    require(values.nonEmpty && values.forall(_.nonEmpty), "Empty selector item")
    values
  }

  def parse(args: Array[String], cwd: Path = callerDirectory): ExpressionBenchmarkRunOptions = {
    require(args.nonEmpty, usage)
    val cli = mutable.LinkedHashMap.empty[String, JsonNode]
    var list = false
    var config: Option[String] = None
    var i = 0
    while (i < args.length) {
      val arg = args(i)
      def value(): String = {
        i += 1
        require(i < args.length, s"Missing value for $arg")
        args(i)
      }
      def put(key: String, v: String): Unit = {
        require(!cli.contains(key), s"Repeated option: $arg")
        cli(key) = if (selectors(key)) {
          val items = mapper.createArrayNode()
          csv(v).foreach(items.add)
          items
        } else if (integers(key)) {
          require(v.matches("-?[0-9]+"), s"$key must be an integer")
          mapper.getNodeFactory.numberNode(new java.math.BigInteger(v))
        } else mapper.getNodeFactory.textNode(v)
      }
      arg match {
        case "--list" =>
          require(!list, "Repeated --list")
          list = true
        case "--config" =>
          require(config.isEmpty, "Repeated --config")
          config = Some(value())
        case "--cases" => put("cases", value())
        case flag if flag.startsWith("--") =>
          val key =
            names.getOrElse(flag.drop(2), throw new IllegalArgumentException(s"Unknown $flag"))
          put(key, value())
        case positional => put("functions", positional)
      }
      i += 1
    }
    validate(cli)
    require(
      !list || (config.isEmpty && cli.keySet.subsetOf(Set("functions"))),
      "--list conflicts with config and run options")
    def resolve(value: String, base: Path): Path = {
      require(value.trim.nonEmpty, "Path must not be empty or blank")
      base.resolve(value).toAbsolutePath.normalize()
    }
    val configPath = config.map(resolve(_, cwd))
    val json = configPath.map {
      path =>
        try {
          val text = UTF_8.newDecoder().decode(ByteBuffer.wrap(Files.readAllBytes(path))).toString
          val tree = mapper.readTree(text)
          require(tree != null && tree.isObject, "Config must be a JSON object")
          require(tree.fieldNames().asScala.forall(fields), "Unknown config key")
          tree.fields().asScala.map(e => e.getKey -> e.getValue).toMap
        } catch {
          case NonFatal(e) =>
            throw new IllegalArgumentException(s"Invalid config $path: ${e.getMessage}", e)
        }
    }.getOrElse(Map.empty[String, JsonNode])
    // Validate each source before merging: overrides must not hide an invalid config value.
    validate(json)
    val replaced = if (selectors.exists(cli.contains)) selectors else Set.empty[String]
    val values = (json -- replaced) ++ cli
    def string(key: String, default: String): String =
      values.get(key).map(_.textValue()).getOrElse(default)
    def integer(key: String, default: Int): Int =
      values.get(key).map(_.intValue()).getOrElse(default)
    def selector(key: String): Seq[String] = values.get(key).toSeq.flatMap {
      _.elements().asScala.map(_.textValue().trim)
    }
    def path(key: String, default: String): Path = {
      val base = if (cli.contains(key)) cwd else configPath.map(_.getParent).getOrElse(cwd)
      resolve(string(key, default), base)
    }
    val options = ExpressionBenchmarkRunOptions(
      functions = selector("functions"),
      cases = selector("cases"),
      list = list,
      rows = integer("rows", 4000000),
      batchSize = integer("batchSize", 10240),
      seed = values.get("seed").map(_.longValue()).getOrElse(20260912L),
      keyCardinality = values.get("keyCardinality").map(_.longValue()),
      asyncProfiler = values.get("asyncProfiler").map(_ => path("asyncProfiler", "")),
      profileEvent = string("profileEvent", "cpu"),
      profileOutput = path("profileOutput", "target/expression-benchmark-profiles"),
      warmup = integer("warmupSeconds", 10).seconds,
      minTime = integer("measurementSeconds", 60).seconds
    )
    require(list || options.functions.nonEmpty || options.cases.nonEmpty, usage)
    require(
      options.asyncProfiler.nonEmpty ||
        !Seq("profileEvent", "profileOutput").exists(values.contains),
      "Profile event/output requires --async-profiler"
    )
    options
  }
}
