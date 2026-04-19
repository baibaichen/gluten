<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Gluten ANSI 模式：表达式影响矩阵

> 日期：{{DATE}}
> 数据来源：Spark 4.x ANSI 合规文档 + Gluten issue #10134 + Velox/Gluten 代码库验证
> 目的：列出所有受 ANSI 模式影响的表达式，以及 Spark 原生 UT 在 Gluten 中的覆盖情况
> **Scope：Spark 4.x（4.0/4.1）**，不涉及 Spark 3.x
> 验证脚本：`dev/verify-ansi-expressions.sh <category> [spark41|spark40|all] [--clean]`
> 分析脚本：`python3 dev/analyze-panorama.py <日志文件或目录>`

---

## 一、ANSI 传递机制概述

Spark 中表达式与 ANSI 模式的关联有**三种机制**，加上一种间接影响：

| 机制 | 原理 | 典型表达式 | Gluten 处理现状 |
|------|------|-----------|---------------|
| **A. evalMode 三态** | 表达式携带 `EvalMode`（LEGACY/ANSI/TRY）。`ansiEnabled=true` → ANSI，`false` → LEGACY；TRY 由 `try_*` 函数硬编码 | Cast, Add, Sum | ✅ 已处理 |
| **B. failOnError boolean** | 构造时读 `SQLConf.get.ansiEnabled`，固化为 boolean（true=抛异常，false=返回 NULL） | Elt, element_at, MakeDate | ❌ 未处理 |
| **C. TRY 函数** | RuntimeReplaceable，**硬编码 TRY 模式**，不受 `ansiEnabled` 影响。支持了对应的 A/B 机制表达式，TRY 函数自动可用 | try_add, try_element_at | 部分（依赖 A/B） |
| **间接影响** | 自身无 ANSI 字段，但内部展开时用到有 evalMode 的表达式 | VAR_POP, STDDEV_POP | 取决于底层 |

> [!IMPORTANT]
> **默认行为**：`ansiFallback=true` 时所有表达式整计划回退到 Spark。**以下矩阵中描述设置 `spark.gluten.sql.ansiFallback.enabled=false` 后走 Velox 的实际行为**。

---

## 二、机制 A：evalMode 三态表达式

### 2.1 Cast（+ try_cast）

> [!NOTE]
> - UT
>   - Spark：`CastWithAnsiOnSuite`、`CastWithAnsiOffSuite`、`TryCastSuite`、`ComplexTypeSuite`
>   - Gluten：`GlutenCastWithAnsiOnSuite`、`GlutenCastWithAnsiOffSuite`、`GlutenTryCastSuite`
> - 验证：`dev/verify-ansi-expressions.sh cast`
> - **测试结果**：{{CAST_TEST_SUMMARY}}
> - **PANORAMA 分析**：{{CAST_PANORAMA_SUMMARY}}

{{CAST_TABLE}}

> **Cast 失败根因分类**（PANORAMA 分析）
>
> {{CAST_FAILURE_ROOT_CAUSE}}

---

### 2.2 算术（+ try 算术 + Abs/UnaryMinus）

> [!NOTE]
> - UT
>   - Spark：`ArithmeticExpressionSuite`、`TryEvalSuite`
>   - Gluten：`GlutenArithmeticExpressionSuite`、`GlutenTryEvalSuite`、`ArithmeticAnsiValidateSuite`、`MathFunctionsValidateSuiteAnsiOn`
> - 验证：`dev/verify-ansi-expressions.sh arithmetic`
> - **测试结果**：{{ARITHMETIC_TEST_SUMMARY}}
> - **PANORAMA 分析**：{{ARITHMETIC_PANORAMA_SUMMARY}}

{{ARITHMETIC_TABLE}}

> **Arithmetic 失败根因分类**（PANORAMA 分析）
>
> {{ARITHMETIC_FAILURE_ROOT_CAUSE}}

---

## 三、机制 B：failOnError / ansiEnabled boolean 表达式

### 3.1 集合（+ try_element_at）

> [!NOTE]
> - UT
>   - Spark：`CollectionExpressionsSuite`、`StringExpressionsSuite`（Elt）
>   - Gluten：`GlutenCollectionExpressionsSuite`
> - 验证：`dev/verify-ansi-expressions.sh collection`
> - **测试结果**：{{COLLECTION_TEST_SUMMARY}}

{{COLLECTION_TABLE}}

> **Collection 失败根因分类**
>
> {{COLLECTION_FAILURE_ROOT_CAUSE}}

### 3.2 日期时间 / Interval（+ try_to_timestamp 等）

> [!NOTE]
> - UT
>   - Spark：`DateExpressionsSuite`、`IntervalExpressionsSuite`、`DateFunctionsSuite`
>   - Gluten：`GlutenDateExpressionsSuite`、`GlutenIntervalExpressionsSuite`、`GlutenDateFunctionsSuite`
> - 验证：`dev/verify-ansi-expressions.sh datetime`
> - **测试结果**：{{DATETIME_TEST_SUMMARY}}

{{DATETIME_TABLE}}

> **Datetime 失败根因分类**
>
> {{DATETIME_FAILURE_ROOT_CAUSE}}

### 3.3 数学

> [!NOTE]
> - UT
>   - Spark：`MathExpressionsSuite`
>   - Gluten：`GlutenMathExpressionsSuite`
> - 验证：`dev/verify-ansi-expressions.sh math`
> - **测试结果**：{{MATH_TEST_SUMMARY}}

{{MATH_TABLE}}

> **Math 失败根因分类**
>
> {{MATH_FAILURE_ROOT_CAUSE}}

### 3.4 Decimal

> [!NOTE]
> - UT
>   - Spark：`DecimalExpressionSuite`
>   - Gluten：`GlutenDecimalExpressionSuite`
> - 验证：`dev/verify-ansi-expressions.sh decimal`
> - **测试结果**：{{DECIMAL_TEST_SUMMARY}}

{{DECIMAL_TABLE}}

> **Decimal 失败根因分类**
>
> {{DECIMAL_FAILURE_ROOT_CAUSE}}

### 3.5 字符串（+ try_parse_url）

> [!NOTE]
> - UT
>   - Spark：`StringExpressionsSuite`、`UrlFunctionsSuite`
>   - Gluten：`GlutenStringExpressionsSuite`、`GlutenUrlFunctionsSuite`
> - 验证：`dev/verify-ansi-expressions.sh string`
> - **测试结果**：{{STRING_TEST_SUMMARY}}

{{STRING_TABLE}}

> **String 失败根因分类**
>
> {{STRING_FAILURE_ROOT_CAUSE}}

---

## 四、聚合 + 间接（需人工校验）

> [!NOTE]
> - UT
>   - Spark：`DataFrameAggregateSuite`
>   - Gluten：`GlutenDataFrameAggregateSuite`
> - 验证：`dev/verify-ansi-expressions.sh aggregate`
> - **测试结果**：{{AGGREGATE_TEST_SUMMARY}}

{{AGGREGATE_TABLE}}

---

## 五、ANSI 错误语义

> [!NOTE]
> - UT
>   - Spark：`QueryExecutionAnsiErrorsSuite`
>   - Gluten：`GlutenQueryExecutionAnsiErrorsSuite`
> - 验证：`dev/verify-ansi-expressions.sh errors`
> - **测试结果**：{{ERRORS_TEST_SUMMARY}}

{{ERRORS_TABLE}}

---

## 六、覆盖率统计

{{COVERAGE_TABLE}}

{{COVERAGE_ANALYSIS}}

---

## 七、图例

| 符号 | 含义 |
|------|------|
| ✅ | 测试通过 + Velox offload |
| 🚫 | 应抛异常未抛（NO_EXCEPTION）— Velox 截断/忽略/返回 null，需 C++ 层修复 |
| ❌ | 其他失败（OTHER / WRONG_EXCEPTION / MSG_MISMATCH / 混合） |
| ⚠️ | FALLBACK — 表达式 fallback 到 Spark 执行，Velox 未被测试 |
| ➖ | 该 suite 未覆盖此表达式 |
