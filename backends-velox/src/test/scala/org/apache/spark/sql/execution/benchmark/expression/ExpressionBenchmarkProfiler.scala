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

import one.profiler.AsyncProfiler

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

/** One measured engine interval. Only a successful start authorizes stopping the profiler. */
final private[benchmark] class ExpressionBenchmarkProfiler private[expression] (
    event: String,
    directory: Path,
    execute: String => String) extends AutoCloseable {
  def this(home: Path, event: String, directory: Path) =
    this(event, directory, ExpressionBenchmarkProfiler.commands(home))

  require(Set("cpu", "alloc")(event), s"Unknown profile event: $event")
  Files.createDirectories(directory)
  val output: Path = directory.resolve("profile.collapsed")
  require(!Files.exists(output), s"Profile already exists: $output")
  private var started = false
  private var stopAttempted = false
  def owned: Boolean = started && !stopAttempted

  def start(): Unit = {
    require(!started, "Profiler can only start once")
    val arguments = if (event == "cpu") "event=cpu,interval=1ms,cstack=dwarf,threads"
    else "event=alloc,alloc=512k,threads"
    execute(s"start,$arguments")
    started = true
  }

  override def close(): Unit = if (owned) {
    // Do not retry after a failed stop or dump, including from the measurement's finally block.
    stopAttempted = true
    val wasInterrupted = Thread.interrupted()
    try {
      execute("stop")
      Files.write(output, execute("collapsed,total").getBytes(UTF_8))
      // Empty output is a valid no-samples result, especially for allocation profiles.
    } finally {
      if (wasInterrupted) Thread.currentThread().interrupt()
    }
  }
}

private[benchmark] object ExpressionBenchmarkProfiler {
  private def commands(home: Path): String => String = {
    val library = home.resolve("lib").resolve(System.mapLibraryName("asyncProfiler"))
    AsyncProfiler.getInstance(library.toAbsolutePath.toString).execute _
  }
}
