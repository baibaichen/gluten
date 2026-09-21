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

import org.apache.gluten.test.MockVeloxBackend

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.GlutenImplicits.withSQLConf
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import org.scalatest.exceptions.TestCanceledException

/** Selects catalog cases and owns their Spark session and resource scopes. */
object RegisteredExpressionBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(args: Array[String]): Unit = {
    // BenchmarkBase.main has no finally; close its file even when parsing or execution fails.
    Utils.tryWithSafeFinally {
      val options = RunOptions.parse(args)
      val catalog = Catalog.load()
      if (options.list) {
        System.out.println(options.listing(catalog)) // scalastyle:ignore println
      } else {
        val cases = options.selected(catalog)
        require(cases.nonEmpty, "No selected cases")
        run(cases, options.runtime)
      }
    } {
      val stream = output
      output = None
      stream.foreach(_.close())
    }
  }

  private[benchmark] def run(
      cases: Seq[Catalog.CaseDef],
      options: RunOptions): Unit = {
    if (cases.isEmpty) return
    val input = options.resolvedInput
    val context = Data.Context(input.rows, input.keyCardinality, input.seed)
    BenchmarkBlackhole.requireEnabled(true)
    val profiler = options.profiler.map(Profiler.apply)
    try Utils.tryWithSafeFinally {
        val conf = MockVeloxBackend.mockPluginContext().conf()
          .setMaster("local[1]").setAppName("ExpressionBenchmark")
          .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
          .set("spark.memory.offHeap.enabled", "true").set("spark.memory.offHeap.size", "8g")
          .set("spark.ui.enabled", "false")
        RunOptions.sqlConf.foreach { case (key, value) => conf.set(key, value) }
        val spark = SparkSession.builder().config(conf).getOrCreate()
        Utils.tryWithSafeFinally {
          cases.foreach(runCase(spark, _, context, options, profiler))
        } {
          spark.stop()
        }
      } {
        profiler.foreach(_.stop())
      }
    catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw e
    }
  }

  // scalastyle:off println
  private def runCase(
      spark: SparkSession,
      scenario: Catalog.CaseDef,
      context: Data.Context,
      options: RunOptions,
      profiler: Option[Profiler]): Unit =
    withSQLConf(RunOptions.sqlConf: _*) {
      TaskResources.runUnsafe {
        val benchmark = new Benchmark(
          spark,
          scenario,
          context,
          options,
          output,
          profiler = profiler)
        try benchmark.checked {
            System.err.println(s"${scenario.id}: ${scenario.description}")
            benchmark.registerCases()
            runBenchmark(scenario.id)(benchmark.run())
          }
        catch {
          case canceled: TestCanceledException =>
            System.err.println(s"SKIPPED ${scenario.id}: ${canceled.getMessage}")
        }
      }
    }
  // scalastyle:on println
}
