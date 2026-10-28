# Task 3: `SparkUnnest` 的 `inlineStruct`（直接拆解 `ARRAY<ROW<...>>` 子列）与 C++ UT

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `SparkUnnest` 中实现 `inlineStruct = true` 路径（对应 Spark `inline`、`inline_outer` 与 `json_tuple`），将 `ARRAY<ROW<f0, f1, ...>>` 直接拆解为独立的子列 `f0, f1, ...`，正确处理 Flat / Dictionary / Constant 编码的 `RowVector`、父结构体 NULL 与 `outer` 补齐行 NULL 的安全合并、以及 0 字段 `ROW<>`，并在 `SparkUnnestTest.cc` 中补充 C++ 单元测试。

**Architecture:** 在 `appendInlineStructColumns` 中先对 `ArrayVector::elements()` 處理编码与连续切片：若底层是无父级 NULL 的平坦 `RowVector` 且满足连续映射，直接取 `slicedRow->childAt(i)` 零拷贝输出；若存在父结构体 NULL、Dictionary/Constant 包装或 `outer` padding NULL，则构造合并后的顶层索引与独立可写 `nulls` 位图，通过 `wrapSingleColumn` 生成各字段列（严禁对共享或只读 `childAt(i)` 原地调用 `BaseVector::addNulls`）。

**Tech Stack:** C++17, Velox (`RowVector`, `DecodedVector`, `BaseVector`), GoogleTest.

---

### Files

- Modify: `cpp/velox/operators/plannodes/SparkUnnest.cc`
- Modify: `cpp/velox/tests/SparkUnnestTest.cc`

---

- [ ] **Step 1: 在 `SparkUnnest.cc` 中实现 `appendInlineStructColumns`**

1. **0 字段结构体（`rowType.size() == 0`）**：
   - 不向 `outputs` 写入任何展开值列，但 `extractRowRange` 已按数组元素个数（及 `isOuter_` 空集合 1 行）计算出 `range.numInnerRows`，保留列与 `pos` 列正常按该行数输出。
2. **连续平坦无父 NULL 快路径**：
   - 当 `encoding.contiguousMapping && elementsVector->encoding() == VectorEncoding::Simple::FLAT` 时，先調用 `auto slicedRow = elementsVector->slice(encoding.sourceStart, wrapSize)->as<RowVector>()`；
   - 若 `!slicedRow->mayHaveNulls()`，对每个字段 `i` 直接 `outputs[outputColumnIndex++] = slicedRow->childAt(i)`（**100% 零拷贝**）！
3. **含父结构体 NULL、非平坦 `RowVector`（Dictionary/Constant）或 `outer` `paddingNulls` 的通用安全路径**：
   - 使用 `DecodedVector decodedElements(*elementsVector)` 解码底层 `baseRow = decodedElements.base()->as<RowVector>()`；
   - 将 `encoding`（外层 Unnest 映射索引 + `paddingNulls`）与 `decodedElements`（结构体元素级索引 + 结构体级 NULL）合成为面向 `baseRow` 的扁平映射 `(combinedIndices, combinedNulls)`：
     - 对每个输出行 `r`：若 `paddingNulls` 为 NULL 或 `decodedElements.isNullAt(elemIdx)` 为 true，则在 `combinedNulls` 中置 `NULL`；否则 `combinedIndices[r] = decodedElements.index(elemIdx)`；
   - 对 `baseRow` 的每个子列 `child_i = baseRow->childAt(i)`，调用 `wrapSingleColumn({combinedIndices, combinedNulls, false, 0}, child_i, wrapSize)` 生成每个字段的独立输出向量（若存在多字段，为每个字段克隆独立的 `combinedNulls` 副本，确保所有输出列 `isNullsWritable()`）。

- [ ] **Step 2: 在 `cpp/velox/tests/SparkUnnestTest.cc` 中补充 `inlineStruct` 的 C++ UT**

新增测试：
1. `TEST_F(SparkUnnestTest, inlineContiguousZeroCopy)`：验证无父 NULL 的平坦 `ARRAY<ROW<a:BIGINT, b:VARCHAR>>` 走零拷贝快路径；
2. `TEST_F(SparkUnnestTest, inlineWithNullStructsAndNullFields)`：验证数组内含 `NULL` 结构体元素、结构体子字段含 `NULL`、以及 `inline_outer` 含空数组 `[]` / `NULL` 数组行的组合；
3. `TEST_F(SparkUnnestTest, inlineDictionaryAndConstantEncodedElements)`：构造 `elements()` 为 Dictionary / Constant 编码的 `RowVector`（且子列为 `ConstantVector`），验证不会触发 `isNullsWritable()` 断言失败；
4. `TEST_F(SparkUnnestTest, inlineEmptyStructZeroFields)`：验证 `ARRAY<ROW<>>`（0 字段结构体）仍按元素个数输出保留列。

- [ ] **Step 3: 编译并运行 `spark_unnest_test`**

```bash
cmake --build cpp/build -j 4 --target spark_unnest_test
./cpp/build/velox/tests/spark_unnest_test
```
Expected: `SparkUnnestTest` 全部测试 `PASSED`。
