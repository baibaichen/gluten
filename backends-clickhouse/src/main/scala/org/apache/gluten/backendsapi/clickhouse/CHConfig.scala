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

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

object CHConfig {

  private val RUNTIME_SETTINGS: String = CHBackend.prefixOf("runtime_settings")

  /** CH configuration prefix at Java side */
  def runtimeSettings(key: String): String = s"$RUNTIME_SETTINGS.$key"

  def startWithSettingsPrefix(key: String): Boolean = key.startsWith(RUNTIME_SETTINGS)
  def removeSettingsPrefix(key: String): String = key.substring(RUNTIME_SETTINGS.length + 1)

  def get: CHConfig = new CHConfig(SQLConf.get)

  import GlutenConfig._

  val ENABLE_ONEPIPELINE_MERGETREE_WRITE =
    buildConf(prefixOf("mergetree.write.pipeline"))
      .doc("Using one pipeline to write data to MergeTree table in Spark 3.5")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD =
    buildConf("spark.gluten.sql.columnar.backend.ch.spillThreshold")
      .internal()
      .doc("Shuffle spill threshold on ch backend")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0MB")

  val COLUMNAR_CH_MAX_SORT_BUFFER_SIZE =
    buildConf("spark.gluten.sql.columnar.backend.ch.maxSortBufferSize")
      .internal()
      .doc("The maximum size of sort shuffle buffer in CH backend.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_CH_FORCE_MEMORY_SORT_SHUFFLE =
    buildConf("spark.gluten.sql.columnar.backend.ch.forceMemorySortShuffle")
      .internal()
      .doc("Whether to force to use memory sort shuffle in CH backend. ")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_CH_REWRITE_DATE_CONVERSION =
    buildConf("spark.gluten.sql.columnar.backend.ch.rewrite.dateConversion")
      .internal()
      .doc(
        "Rewrite the conversion between date and string."
          + "For example `to_date(from_unixtime(unix_timestamp(stringType, 'yyyyMMdd')))`"
          + " will be rewritten to `to_date(stringType)`")
      .booleanConf
      .createWithDefault(true)
}

class CHConfig(conf: SQLConf) extends GlutenConfig(conf) {
  import CHConfig._

  def enableOnePipelineMergeTreeWrite: Boolean =
    getConf(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE)

  def chColumnarShuffleSpillThreshold: Long = {
    val threshold = getConf(COLUMNAR_CH_SHUFFLE_SPILL_THRESHOLD)
    if (threshold == 0) {
      (taskOffHeapMemorySize * 0.9).toLong
    } else {
      threshold
    }
  }

  def chColumnarMaxSortBufferSize: Long = getConf(COLUMNAR_CH_MAX_SORT_BUFFER_SIZE)

  def chColumnarForceMemorySortShuffle: Boolean =
    getConf(COLUMNAR_CH_FORCE_MEMORY_SORT_SHUFFLE)

  def enableCHRewriteDateConversion: Boolean =
    getConf(ENABLE_CH_REWRITE_DATE_CONVERSION)

}
