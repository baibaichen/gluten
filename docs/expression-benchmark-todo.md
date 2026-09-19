# Expression benchmark TODO

仅保留未决事项，按审查轮次区分编号。列入 TODO 不授权立即实施。

## 首轮审查

| 编号 | 问题 | 后续要求与证据边界 |
| --- | --- | --- |
| 3 | 嵌套 NULL 的结果比较可能失真 | **先复现，再决定是否修。**重点验证含 NULL 的原始类型数组及 Map value。普通顶层标量已检查 NULL。尚未实际复现，不能断言问题由删除 `row.copy()` 引入，也不能认定恢复 copy 即可解决。 |
| 6 | With 重写产生多层 Project，合法标量表达式可能被拒绝 | 例：`nullif(input + 1, 0)`。Spark 重写可能生成中间 Project，而 benchmark 只接受单层 Project。已核对静态逻辑，尚未运行复现；是否及如何处理待决策。 |
| 8 | 零列输入反复求值时，Native handle 可能积累 | **先复现。**核查同一资源作用域内，零列 batch 经 `getNativeHandle()` 反复登记的 handle 是否持续增长及何时释放。当前 Markdown 强制至少一个输入列，不能直接触发；将来同时支持 `rand()` 等无输入列表达式与零列输入时，可形成实际验证场景，但尚未决定扩展支持。不是每行登记一次，而是每次零列 batch 求值涉及登记。 |

## 2026-09-20 追加审查

| 编号 | 问题 | 后续要求与证据边界 |
| --- | --- | --- |
| 1 | Native 保留的 RuntimeReplaceable 节点可能无法直接常量折叠 | **先复现，再决定修法。**候选表达式：`concat(input, to_json(named_struct('n', 1)))`。Spark 4.1 的 ConstantFolding 传入 EmptyRow，而 RuntimeReplaceable.eval 要求 null。现有 `to_json(input)` 用例通过；候选场景尚未实际复现。 |
| 2 | 查询时间表达式在不同准备路径中可能冻结为不同值 | **TODO，暂不修改。**例如 `current_timestamp()` 经 JVM、参考结果、Native 各自的 fresh() 可能得到不同时间。与 `rand()` 一并考虑这类依赖执行上下文的表达式是否支持及如何比较，但二者不同：`rand()` 当前被 deterministic 检查拒绝，查询时间表达式可能通过检查。当前 Markdown 无该用例，尚未实际复现，也未决定扩展支持。 |
| 4 | CPU profiler 固定 DWARF 栈展开方式限制平台支持 | **TODO。**当前 `cstack=dwarf` 在 async-profiler 3.0 的 ppc64le 平台不受支持。已核对源码，未在该平台实测；当前 x86_64 不受此平台限制影响。是否开放配置或使用平台支持的方式待决定，不自动增加回退。 |

相关实现：

- 首轮第 3、8 项：`backends-velox/src/test/scala/org/apache/spark/sql/catalyst/expressions/NativeExpressionEvalHelper.scala`。
- 首轮第 6 项、追加第 1、2 项：`backends-velox/src/test/scala/org/apache/spark/sql/execution/benchmark/expression/ExpressionBenchmark.scala`。
- 追加第 4 项：`backends-velox/src/test/scala/org/apache/spark/sql/execution/benchmark/expression/ExpressionBenchmarkProfiler.scala`。
