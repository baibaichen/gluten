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
package org.apache.spark.sql

import org.apache.gluten.execution.GenerateExecTransformerBase

import org.apache.spark.sql.internal.SQLConf

class GlutenGeneratorFunctionSuite extends GeneratorFunctionSuite with GlutenSQLTestsTrait {
  testGluten("SparkUnnest preserves mixed null/empty collections and eliminates post-project") {
    for (batchSize <- Seq(2, 65)) {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate",
        "spark.gluten.sql.columnar.maxBatchSize" -> batchSize.toString,
        "spark.gluten.sql.columnar.backend.velox.unnest_split_output" -> "true"
      ) {
        val input = spark
          .range(0L, 65L, 1L, 1)
          .selectExpr(
            "id",
            "IF(id % 5 = 0, CAST(NULL AS ARRAY<BIGINT>), " +
              "IF(id % 5 = 1, array(), IF(id % 5 = 2, array(NULL, id), " +
              "IF(id % 5 = 3, array(id, id + 1, id + 2), array(id))))) AS a"
          )
          .selectExpr(
            "id",
            "a",
            "transform(a, x -> IF(x IS NULL, CAST(NULL AS STRUCT<v:BIGINT,s:STRING>), " +
              "named_struct('v', x, 's', repeat(CAST(x AS STRING), 20)))) AS s"
          )
        for (
          function <- Seq(
            "explode",
            "explode_outer",
            "posexplode",
            "posexplode_outer",
            "inline",
            "inline_outer")
        ) {
          val column = if (function.startsWith("inline")) "s" else "a"
          val expected = withSQLConf("spark.gluten.enabled" -> "false") {
            input.selectExpr("id", s"$function($column)").collect().toSeq
          }
          val result = input.selectExpr("id", s"$function($column)")
          checkAnswer(result, expected)
          assert(getExecutedPlan(result).exists(_.isInstanceOf[GenerateExecTransformerBase]))
          val pruned = result.drop("id")
          checkAnswer(pruned, expected.map(row => Row.fromSeq(row.toSeq.drop(1))))
          if (function.startsWith("posexplode")) {
            val posOnly = result.select("pos")
            checkAnswer(posOnly, expected.map(row => Row(row.get(1))))
          }
        }
      }
    }
  }

  testGluten("stack is offloaded") {
    val df = spark.range(2).selectExpr("stack(2, id, id + 1, id + 2)")
    checkAnswer(df, Seq(Row(0L, 1L), Row(2L, null), Row(1L, 2L), Row(3L, null)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }

  testGluten("stack without null padding is offloaded") {
    val df = spark.range(2).selectExpr("stack(2, id, id + 1, id + 2, id + 3)")
    checkAnswer(df, Seq(Row(0L, 1L), Row(2L, 3L), Row(1L, 2L), Row(3L, 4L)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }

  testGluten("single-column stack with null padding is offloaded") {
    val df = spark.range(2).selectExpr("stack(3, id, id + 1)")
    checkAnswer(df, Seq(Row(0L), Row(1L), Row(null), Row(1L), Row(2L), Row(null)))
    assert(
      df.queryExecution.executedPlan
        .find(_.isInstanceOf[GenerateExecTransformerBase])
        .isDefined)
  }

  testGluten("SPARK-45171: Handle evaluated nondeterministic expression") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val df = sql("select explode(array(rand(0)))")
      checkAnswer(df, Row(0.7604953758285915))
    }
  }
}
