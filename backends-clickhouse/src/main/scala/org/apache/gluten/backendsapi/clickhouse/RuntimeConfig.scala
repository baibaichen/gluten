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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.internal.SQLConf

object RuntimeConfig {

  private val RUNTIME_CONFIG = CHBackend.prefixOf(s"runtime_config")
  private def buildConf(k: String) = GlutenConfig.buildConf(apply(k))

  def get: RuntimeConfig = new RuntimeConfig(SQLConf.get)

  /** CH configuration prefix at Java side */
  def apply(key: String): String = s"$RUNTIME_CONFIG.$key"

  /** Clickhouse Configuration */
  val PATH =
    buildConf("path")
      .doc(
        "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#path")
      .stringConf
      .createWithDefault("/")

  // scalastyle:off line.size.limit
  val TMP_PATH =
    buildConf("tmp_path")
      .doc(
        "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#tmp-path")
      .stringConf
      .createWithDefault("/tmp/libch")
  // scalastyle:on line.size.limit

  // scalastyle:off line.size.limit
  val LOGGER_LEVEL =
    buildConf("logger.level")
      .doc(
        "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#logger")
      .stringConf
      .createWithDefault("warning")
  // scalastyle:on line.size.limit

  /** Gluten Configuration */
  val USE_CURRENT_DIRECTORY_AS_TMP =
    buildConf("use_current_directory_as_tmp")
      .doc("Use the current directory as the temporary directory.")
      .booleanConf
      .createWithDefault(false)

  val DUMP_PIPELINE =
    buildConf("dump_pipeline")
      .doc("Dump pipeline to file after execution")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_GLUTEN_LOCAL_FILE_CACHE =
    buildConf("gluten_cache.local.enabled")
      .internal()
      .doc("Enable local cache for CH backend.")
      .booleanConf
      .createWithDefault(false)

  // We can't use gluten_cache.local.enabled
  // because FileCacheSettings doesn't contain this field.
  // So we pass it to CH backend by runtime_config
  val ENABLE_GLUTEN_LOCAL_FILE_CACHE_BACKEND =
    buildConf("enable.gluten_cache.local")
      .internal()
      .doc("Enable local cache for CH backend.")
      .booleanConf
      .createWithDefault(false)

  val USE_LOCAL_FORMAT =
    buildConf("use_local_format")
      .doc("Use `VectorizedParquetRecordReader` to decode parquet format.")
      .booleanConf
      .createWithDefault(false)
}

class RuntimeConfig(conf: SQLConf) extends GlutenConfig(conf) {
  def enableGlutenLocalFileCache: Boolean = getConf(RuntimeConfig.ENABLE_GLUTEN_LOCAL_FILE_CACHE)
}
