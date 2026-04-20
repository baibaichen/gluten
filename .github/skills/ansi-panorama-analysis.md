# Skill: ANSI Panorama Analysis

## 1. 定位

使用 `analyze-ansi.py` 分析 ANSI 表达式 offload 测试结果，生成矩阵报告，定位失败根因，提出修复建议。

| 文件 | 用途 |
|------|------|
| `.github/scripts/analyze-ansi.py` | 统一分析脚本（JSON + XML → summary / matrix / full / json） |
| `gluten-ut/common/.../GlutenExpressionOffloadTracker.scala` | Tracker：生成 `target/ansi-offload/<Suite>.json` |
| `gluten-substrait/.../ConverterUtils.scala` | `getTypeNode()` — Cast 类型支持检查点 |
| `gluten-core/.../ExpressionConverter.scala` | 表达式转换入口 |
| `gluten-ut/common/.../GlutenTestsTrait.scala` | `doCheckExpression` / `doCheckExceptionInExpression` 基础方法 |

## 2. 调用方式

```bash
# 表达式矩阵（markdown）
python3 .github/scripts/analyze-ansi.py --json-dir <dir> --mode matrix

# 完整报告（summary + matrix 折叠）
python3 .github/scripts/analyze-ansi.py --json-dir <dir> --mode full

# 结构化 JSON（供 Agent 消费）
python3 .github/scripts/analyze-ansi.py --json-dir <dir> --mode json
```

### JSON 数据来源

Tracker 在 `afterAll()` 中输出 `target/ansi-offload/<SuiteName>.json`：

```json
{
  "suite": "GlutenCastWithAnsiOnSuite",
  "category": "cast",
  "tests": [{
    "name": "test name",
    "status": "PASS|FAIL",
    "records": [{
      "method": "N|E",
      "expression": "Cast(...)",
      "meta": {"fromType": "String", "toType": "Int"},
      "offload": "OFFLOAD|FALLBACK",
      "status": "PASS|FAIL",
      "failCause": "error message or null",
      "failStackTrace": "stack trace or null"
    }]
  }]
}
```

## 3. 统一三色分类

| 颜色 | 含义 | 判定 |
|------|------|------|
| 🟢 | 通过 + Offload | PASS && offload=OFFLOAD |
| 🟡 | 失败 | FAIL（含 NO_EXCEPTION / WRONG_EXCEPTION / MSG_MISMATCH / OTHER） |
| 🔴 | Fallback | offload=FALLBACK（回退到 Spark） |
| ⚪ | 无数据 | 无 record |

## 4. 源码分析：定位失败根因

### 4.1 🔴 FALLBACK 根因

表达式回退到 Spark 意味着 Velox 不支持该操作。

**定位步骤**：
1. **Cast fallback**：检查 `gluten-substrait/.../ConverterUtils.scala` → `getTypeNode()` 方法
   - 搜索：`grep -n "getTypeNode" gluten-substrait/`
   - 确认缺失的类型映射（如 `DecimalType` → Substrait `Type::DECIMAL`）

2. **Expression fallback**：检查 `gluten-core/.../ExpressionConverter.scala`
   - 搜索：`grep -rn "ExpressionClassName" gluten-core/ backends-velox/`
   - 确认表达式是否在 `expressionMappings` 中注册

3. **Velox 层面**：某些操作在 Substrait 中有映射但 Velox 未实现
   - 检查 Velox 的 `registered_functions.json` 或对应的 C++ 实现

### 4.2 🟡 失败根因

**failCause 分类**（从 JSON 的 `failCause` 字段匹配）：

| failCause 类型 | 含义 | 定位方向 |
|---|---|---|
| `NO_EXCEPTION` | 应抛异常未抛 | Velox ANSI mode 未实现该类型的溢出/越界检查 |
| `WRONG_EXCEPTION` | 异常类型不匹配 | Velox 抛出的异常类型与 Spark 期望不同 |
| `MSG_MISMATCH` | 错误消息不匹配 | Gluten 的 `GlutenTestsTrait` 对 SparkThrowable 做了 errorClass 降级处理，但消息文本不同 |
| `OTHER` | 结果值不匹配或执行异常 | 计算结果与 Spark 不同，需检查 Velox 实现 |

**NO_EXCEPTION 深入分析**：
- 检查 `GlutenTestsTrait.doCheckExceptionInExpression` 的 `intercept` 逻辑
- Velox 执行该表达式时未抛出异常 → 可能是 Velox 的 ANSI mode 实现不完整
- 搜索：`grep -rn "expression class name" backends-velox/`

**WRONG_EXCEPTION 深入分析**：
- Velox 异常类型映射：`SparkException` vs `ArithmeticException` vs `DateTimeException`
- 检查 `JniError.scala` 或 `VeloxBackend` 中的异常包装逻辑

## 5. 结果总结：生成分析报告

Agent 应基于 `--mode json` 输出生成以下分析：

### 5.1 总体概览
- 总测试数、通过率
- 各 category 的 tests/passed/failed 分布

### 5.2 关键发现
- 失败集中在哪些 category（按失败数排序）
- FALLBACK 比例高的 category（Velox 不支持的操作集中区域）
- 按 failCause 类型统计：NO_EXCEPTION / WRONG_EXCEPTION / MSG_MISMATCH / OTHER 的占比

### 5.3 修复建议
按优先级排序（影响范围 × 修复难度）：

1. **高优先级**：大量 NO_EXCEPTION 的操作 → 需要在 Velox 中实现 ANSI 检查
2. **中优先级**：FALLBACK 的类型对/操作 → 需要在 ConverterUtils 添加类型映射
3. **低优先级**：MSG_MISMATCH → 调整错误消息格式

每条建议应包含：
- 具体的代码位置（文件路径 + 函数名）
- 改动方向（如「在 `getTypeNode` 中添加 `DecimalType(p,s)` → `Type::DECIMAL` 映射」）
- 预期影响（修复后多少测试会变绿）

## 6. 已知局限

1. **Aggregate / Errors 无 Tracker 数据**：这些 suite 不使用 `GlutenExpressionOffloadTracker`，仅有测试级结果
2. **常量折叠假 OFFLOAD**：`cast(null as X)` 被 Spark 优化器折叠后执行计划仍含 ProjectExecTransformer，产生假 OFFLOAD 记录
3. **backends-velox 无表达式数据**：仅有 Surefire XML 的测试方法级 pass/fail
