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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.shim.GlutenTestsTrait

class GlutenCastWithAnsiOffSuite extends CastWithAnsiOffSuite with GlutenTestsTrait {
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Need to explicitly set spark.sql.preserveCharVarcharTypeInfo=true for gluten's test
    // framework. In Gluten, it overrides the checkEvaluation that invokes Spark's RowEncoder,
    // which requires this configuration to be set.
    // In Vanilla spark, the checkEvaluation method doesn't invoke RowEncoder.
    conf.setConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO, true)
  }
}
