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
package org.apache.spark.sql.delta.files

import org.apache.spark.sql.execution.CHColumnarWrite
import org.apache.spark.sql.execution.datasources.{WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch

class MergeTreeDelayedCommitProtocol(
    val outputPath: String,
    randomPrefixLength: Option[Int],
    subdir: Option[String],
    val database: String,
    val tableName: String)
  extends DelayedCommitProtocol("delta-mergetree", outputPath, randomPrefixLength, subdir)
  with MergeTreeFileCommitProtocol {}

case class MergeTreeDelayedCommitProtocolWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: MergeTreeDelayedCommitProtocol)
  extends CHColumnarWrite[MergeTreeDelayedCommitProtocol] {
  override def doSetupNativeTask(): Unit = {}

  override def commitTask(batch: ColumnarBatch): Option[WriteTaskResult] = None
}
