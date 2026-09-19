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

import java.io.{ByteArrayOutputStream, IOException, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class ExpressionBenchmarkProfilerSuite extends SparkFunSuite {
  private val cpuStart = "start,event=cpu,interval=1ms,cstack=dwarf,threads"

  private def fixture(event: String = "cpu", response: String => String = _ => "")(
      f: (ExpressionBenchmarkProfiler, ArrayBuffer[String]) => Unit): Unit = {
    val root = Files.createTempDirectory("profiler space,comma ")
    val commands = ArrayBuffer.empty[String]
    try {
      val profiler = new ExpressionBenchmarkProfiler(
        event,
        root.resolve("output space,comma"),
        command => {
          commands += command
          response(command)
        })
      f(profiler, commands)
    } finally Utils.deleteRecursively(root.toFile)
  }

  Seq((0, 3.millis), (3, Duration.Zero)).foreach {
    case (minNumIters, minTime) =>
      test(
        s"start only for measured actions and stop before statistics with minIters=$minNumIters") {
        fixture() {
          (profiler, commands) =>
            var stoppedBeforeStatistics = Option.empty[Boolean]
            val iterations = ArrayBuffer.empty[Int]
            val console = new PrintStream(new ByteArrayOutputStream()) {
              override def println(value: Any): Unit = { // scalastyle:ignore println
                if (String.valueOf(value).contains("Stopped after")) {
                  stoppedBeforeStatistics = Some(
                    !profiler.owned && commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
                }
              }
            }
            val result =
              try Console.withOut(console) {
                  ExpressionBenchmark.measure(
                    "profile",
                    1,
                    3.millis,
                    minTime,
                    minNumIters = minNumIters,
                    profiler = Some(profiler)) {
                    i =>
                      iterations += i
                      if (i < 0) assert(commands.isEmpty && !profiler.owned)
                      else assert(commands.toSeq == Seq(cpuStart) && profiler.owned)
                      Thread.sleep(1)
                  }
                }
              finally console.close()
            assert(
              stoppedBeforeStatistics.contains(true),
              s"profiler must stop before Spark calculates statistics: $stoppedBeforeStatistics")
            assert(iterations.contains(-1))
            assert(iterations.filter(_ >= 0).toSeq == result.samples.indices)
            assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
            assert(Files.isReadable(profiler.output) && Files.size(profiler.output) == 0)
            assert(!profiler.owned)
            if (minTime == Duration.Zero) assert(result.samples.size == minNumIters)
            else assert(result.samples.nonEmpty && result.samples.sum >= minTime.toNanos)
        }
      }
  }

  test("allocation commands preserve total counts and write UTF-8 output with spaces and commas") {
    val collapsed = "worker;sample_λ 524288\n" // scalastyle:ignore nonascii
    fixture("alloc", command => if (command == "collapsed,total") collapsed else "") {
      (profiler, commands) =>
        profiler.start()
        profiler.close()
        assert(commands.toSeq == Seq(
          "start,event=alloc,alloc=512k,threads",
          "stop",
          "collapsed,total"))
        assert(new String(Files.readAllBytes(profiler.output), UTF_8) == collapsed)
    }
  }

  test("failed start never stops an unrelated profile") {
    val failure = new IllegalStateException("Profiler already started")
    fixture(response = _ => throw failure) {
      (profiler, commands) =>
        val thrown = intercept[IllegalStateException](ExpressionBenchmark.measure(
          "start failure",
          1,
          Duration.Zero,
          Duration.Zero,
          profiler = Some(profiler))(_ => fail("measurement must not run after failed start")))
        profiler.close()
        assert(thrown eq failure)
        assert(thrown.getSuppressed.isEmpty)
        assert(commands.toSeq == Seq(cpuStart) && !profiler.owned)
        assert(!Files.exists(profiler.output))
    }
  }

  test("close is idempotent and a successful interval cannot restart") {
    fixture() {
      (profiler, commands) =>
        profiler.close()
        assert(commands.isEmpty)
        profiler.start()
        profiler.close()
        profiler.close()
        intercept[IllegalArgumentException](profiler.start())
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total") && !profiler.owned)
    }
  }

  test("measurement failure still stops and writes the profile") {
    fixture() {
      (profiler, commands) =>
        val primary = new IllegalStateException("measurement failure")
        val thrown = intercept[IllegalStateException](ExpressionBenchmark.measure(
          "measurement failure",
          1,
          Duration.Zero,
          Duration.Zero,
          profiler = Some(profiler))(_ => throw primary))
        assert(thrown eq primary)
        assert(thrown.getSuppressed.isEmpty)
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
        assert(Files.isReadable(profiler.output))
    }
  }

  test("primary measurement failure retains a suppressed cleanup failure") {
    val cleanup = new IllegalStateException("stop failure")
    fixture(response = command => if (command == "stop") throw cleanup else "") {
      (profiler, commands) =>
        val primary = new IllegalStateException("measurement failure")
        val thrown = intercept[IllegalStateException](ExpressionBenchmark.measure(
          "measurement failure",
          1,
          Duration.Zero,
          Duration.Zero,
          profiler = Some(profiler))(_ => throw primary))
        profiler.close()
        assert(thrown eq primary)
        assert(thrown.getSuppressed.toSeq == Seq(cleanup))
        assert(commands.toSeq == Seq(cpuStart, "stop") && !profiler.owned)
    }
  }

  test("stop failure after successful measurement is not retried") {
    val failure = new IllegalStateException("stop failure")
    fixture(response = command => if (command == "stop") throw failure else "") {
      (profiler, commands) =>
        var measured = false
        val thrown = intercept[IllegalStateException](ExpressionBenchmark.measure(
          "stop failure",
          1,
          Duration.Zero,
          Duration.Zero,
          minNumIters = 1,
          profiler = Some(profiler))(_ => measured = true))
        profiler.close()
        assert(measured && (thrown eq failure))
        assert(thrown.getSuppressed.isEmpty)
        assert(commands.toSeq == Seq(cpuStart, "stop") && !profiler.owned)
    }
  }

  test("dump failure propagates without repeating stop") {
    val failure = new IllegalStateException("dump failure")
    fixture(response = command => if (command == "collapsed,total") throw failure else "") {
      (profiler, commands) =>
        profiler.start()
        assert(intercept[IllegalStateException](profiler.close()) eq failure)
        profiler.close()
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
        assert(!Files.exists(profiler.output))
    }
  }

  test("output write failure propagates without repeating stop") {
    fixture() {
      (profiler, commands) =>
        profiler.start()
        Files.createDirectory(profiler.output)
        intercept[IOException](profiler.close())
        profiler.close()
        assert(commands.toSeq == Seq(cpuStart, "stop", "collapsed,total"))
    }
  }

  Seq(false, true).foreach {
    case stopFails =>
      test(s"close restores the interrupt flag when stopFails=$stopFails") {
        fixture(response = command => {
          assert(!Thread.currentThread().isInterrupted)
          if (command == "stop" && stopFails) throw new IllegalStateException("stop failure")
          ""
        }) {
          (profiler, commands) =>
            profiler.start()
            Thread.currentThread().interrupt()
            try {
              if (stopFails) intercept[IllegalStateException](profiler.close())
              else profiler.close()
              assert(Thread.currentThread().isInterrupted)
              profiler.close()
              assert(commands.count(_ == "stop") == 1)
            } finally Thread.interrupted()
        }
      }
  }

  test("measurement interruption is restored and keeps cleanup errors suppressed") {
    val cleanup = new IllegalStateException("stop failure")
    fixture(response = command => if (command == "stop") throw cleanup else "") {
      (profiler, commands) =>
        val primary = new InterruptedException("measurement interrupted")
        try {
          val thrown = intercept[InterruptedException](ExpressionBenchmark.measure(
            "interrupted",
            1,
            Duration.Zero,
            Duration.Zero,
            profiler = Some(profiler))(_ => throw primary))
          assert(thrown eq primary)
          assert(thrown.getSuppressed.toSeq == Seq(cleanup))
          assert(Thread.currentThread().isInterrupted)
          assert(commands.toSeq == Seq(cpuStart, "stop"))
        } finally Thread.interrupted()
    }
  }
}
