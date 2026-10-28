# Task 2: `SparkUnnest` 的 `outer` 补齐、0-based `INTEGER pos`、多列 `Stack` 与 C++ UT

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `SparkUnnest` 中实现 `isOuter` 空/NULL 集合补 `NULL`（无 `isPresent` 列）、`withOrdinality` 0-based `INTEGER pos` 列（位于保留列后、展开值列前，且保障输出列 `nulls` 缓冲区独立可写）以及多列 `Stack` Zip 展开，并在 `SparkUnnestTest.cc` 中补充对应 C++ 单元测试。

**Architecture:** 在 `extractRowRange` 中当 `isOuter_ && size == 0` 时分配 1 行输出并标记 `hasEmptyUnnestValue = true`；在 `generateOutput` 中先生成保留列，再生成 0-based `int32_t` 的 `pos` 列，最后生成展开值列。

**Tech Stack:** C++17, Velox (`FlatVector<int32_t>`, `SelectivityVector`, `bits::setNull`), GoogleTest.

---

### Files

- Modify: `cpp/velox/operators/plannodes/SparkUnnest.cc`
- Modify: `cpp/velox/tests/SparkUnnestTest.cc`

---

- [ ] **Step 1: 实现 0-based `INTEGER pos` 生成（`generateOrdinalityVector`）**

1. 创建 `auto ordinalityVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), range.numInnerRows, pool());`
2. 当 `!(isOuter_ && range.hasEmptyUnnestValue)` 时：
   - 遍历 `range.forEachRow`，调用 `std::iota(rawOrdinality, rawOrdinality + size, static_cast<int32_t>(start))`，推进 `rawOrdinality += size`；
3. 当 `isOuter_ && range.hasEmptyUnnestValue` 时：
   -若单列值列已走 `FlatVector` 拷贝路径（此时 `encoding.nulls` 未被任何输出向量通过 `setNulls` 持有，`encoding.nulls->unique() == true`），直接 `ordinalityVector->setNulls(encoding.nulls)`，只填充 `rawOrdinality`；
   - 否则调用 `ordinalityVector->mutableRawNulls()`，在 `size == 0` 的补齐行调用 `bits::setNull(rawNulls, idx, true)`，确保每个输出向量的 `nulls_` 均满足 `isNullsWritable()`。

- [ ] **Step 2: 实现单列 `outer` 与多列 `Stack` 编码生成**

1. **单列 `outer` 且 `range.hasEmptyUnnestValue == true`**：
   - 分配 `indices` 与 `paddingNulls`；
   - 遍历 `range.forEachRow`：`size == 0` 时置 `bits::setNull(rawNulls, index++, true)`，`size > 0` 时调用 `std::iota(rawIndices + index, rawIndices + index + size, offsets[inputIndices[row]] + start)`；
   - 在 `wrapSingleColumn` 中：若 `base->isFlatEncoding()`，先 `rows.deselectNulls(rawPaddingNulls, 0, wrapSize)`，执行 `flat->copy(base.get(), rows, rawIndices)`，再 `rows.setAll(); flat->addNulls(rawPaddingNulls, rows)`（区分真正的 NULL 元素与合成补齐行 NULL）。
2. **多列 Zip（`unnestChannels_.size() > 1`，用于 `Stack`）**：
   - `addInput` 中对每个通道调用 `forEachNotNull` 计算 `rawMaxSizes_[row] = std::max(rawMaxSizes_[row], unnestSize)`；
   - `generateEncodingForChannel` 对短于 `rawMaxSizes_[row]` 的列或 `isOuter_ && size == 0` 的行在 `nulls` 位图上置 `NULL`。

- [ ] **Step 3: 在 `cpp/velox/tests/SparkUnnestTest.cc` 中补充 C++ UT**

新增测试：
1. `TEST_F(SparkUnnestTest, outerArrayAndMap)`：验证 `explode_outer` 在 `NULL`、空集合 `[]`、含 `NULL` 元素 `[null, 7]` 下的输出，确认无多余 `isPresent` 列；
2. `TEST_F(SparkUnnestTest, posExplodeInnerAndOuter)`：验证 `posexplode` / `posexplode_outer` 输出列顺序为 `[id, pos, col]`，`pos` 为 0-based `INTEGER`，空集合行 `pos` 为 `NULL`、真实 `NULL` 元素行 `pos` 为有效下标 `0, 1, ...`；并显式对输出的 `pos` 列和 `col` 列调用 `isNullsWritable()` / `addNulls` 验证缓冲区独立可写；
3. `TEST_F(SparkUnnestTest, multiColumnStackInnerAndOuter)`：验证多列 `ARRAY` Zip 展开与 `outer` 补齐。

- [ ] **Step 4: 编译并运行 `spark_unnest_test`**

```bash
cmake --build cpp/build -j 4 --target spark_unnest_test
./cpp/build/velox/tests/spark_unnest_test
```
Expected: 所有新增测试 `PASSED`。
