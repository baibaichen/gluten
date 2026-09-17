---
layout: page
title: Native Expression Unit Tests
parent: Developer Overview
---
# Native Expression Unit Tests

Scalar expression suites keep the existing entrypoint:

```scala
checkEvaluation(expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow)
```

Each check selects one execution branch:

1. Prepare the existing row bindings and use
   `ExpressionConverter.replaceWithExpressionTransformer`, including the current
   expression mappings, blacklist and `doExprValidate` policy.
2. Validate the converted expression with
   `BackendsApiManager.getValidatorApiInstance.doNativeValidateExpression`.
3. If supported, evaluate directly in native code and compare with `expected`.
4. Otherwise, evaluate through Spark's existing `ExpressionEvalHelper` and
   compare with the same expectation.

There is no DataFrame construction or `collect()` to choose the scalar route,
no Spark-first baseline followed by native replay, and no routing mode flags,
route ledger or suite/function support whitelist. The existing backend APIs
are the support authority. A converter `GlutenNotSupportException` or a false
native validation result produces an explicit fallback diagnostic. The boolean
native API does not supply a detailed reason when it returns false.
Unexpected validation errors propagate. Native input allocation, evaluator
compilation, execution and comparison happen outside the unsupported-conversion
catch: failures there never retry in Spark.

## Existing suite contracts

`GlutenExpressionTestsTrait` retains `GlutenTestsTrait`'s lifecycle, semantic
SQLConf and virtual result comparator. Its Vanilla branch uses the existing
`checkEvaluationWithoutCodegen`: Spark's additional unsafe-row byte comparison
would incorrectly replace the historical numeric tolerance and map ordering
contract. No Spark optimizer is run to select or execute this scalar branch.
The standalone
`NativeExpressionTestsTrait` uses the same support decision without creating a
SparkContext or SparkSession, and retains Spark's stricter result comparator.
Its fallback keeps Spark's original interpretation, codegen and optimization checks.
Neither trait enables experimental native features to force support.

Explicit query, planner, generator, aggregate and code-generation mechanism
tests keep their existing entrypoints. Upstream suite bodies, offload tracking,
ANSI exception helpers and Spark 4.1 expression-resolution hooks are retained.
Query checks continue through the existing `doCheckExpression` extension point.
The seven historical
`GlutenTestsCommonTrait` wrapper families remain Spark-reference-only:
CallMethodViaReflection, CastWithAnsiOn, CollationExpression,
CollationRegexpExpressions, CsvExpressions, ScalaUDF and ToPrettyString.
Expected values, original exclusions and error-checking helpers are unchanged.

The query session still excludes Spark's `ConstantFolding`,
`ConvertToLocalRelation` and `NullPropagation`. Direct native preparation does
not run a Spark optimizer. Literal expressions remain literal expressions with
a one-row, zero-column input. Velox's normal `ExprSet` constant folding stays
enabled: it evaluates the native function and retains its constant result.
There is no folding configuration knob or rewrite of ordinary literals into
bound references. Test-only `NonFoldableLiteral` retains its separate contract
and is supplied as a native input column.

Mapped `RuntimeReplaceable` classes remain available to the converter; unmapped
wrappers are desugared without bypassing blacklisted classes. Declared
compatible timezones use the evaluator's existing runtime `extraConf`, not a
mutation of the JVM or session timezone. Mixed zones receive no expression-wide
override. Standalone Spark unsafe expected-row boxing still preserves integer
values exactly.

## Borrowed results and row adaptation

`NativeExpressionRowEvalHelper` derives types from bound references, compacts
sparse ordinals and validates conflicting or out-of-range bindings before
reading input. Support is checked before native input allocation. Supported
inputs use the existing `VeloxColumnarRow.update` bridge, preserving raw UTF8
bytes, nulls, arrays, maps and structs. Unrelated input fields are not inspected.
Unsupported logical types are not retagged as primitive types.

`NativeExpressionEvalHelper` reads each borrowed result and compares it while
the native batch and its view remain live. It does not copy the output row.
Comparators check nested null bits before calling row or array getters.
The historical query comparator retains its float/NaN/signed-zero, Decimal,
sorted-map and duplicate-key semantics; nested comparisons still dispatch
through suite overrides. Map sorting uses Spark's existing interpreted key
ordering directly, preserving last-value duplicates for borrowed array/struct
keys without relying on JVM object identity. Standalone comparisons retain Spark's exact numeric
and order-sensitive map semantics, with the same null-safe borrowed traversal.
Existing native interval/timestamp carriers are adapted only for result reads.

The columnar evaluator's compilation, reusable execution, initialization and
resource ownership APIs are unchanged:

* `prepareNativeExpression` accepts exactly one output expression and multiple
  input attributes within a `TaskResources` scope.
* Inputs are borrowed; returned native batches are caller-owned.
* Prepared evaluators can be reused and closed independently of live outputs.
* Explicit standalone native helper calls still throw for unsupported
  expressions; only scalar dispatch selects Spark fallback.
* Benchmark preparation, timed evaluate/close and `consumeStringLengths`
  retain their existing behavior.

`TestStats` records a scalar test as native only after a successful native
check and only when no check in that test selected fallback. A test with no
scalar evaluation does not acquire native coverage from the dispatch trait.

## Regression checks

The targeted framework suites are:

```text
org.apache.spark.sql.catalyst.expressions.GlutenExpressionRoutingSuite
org.apache.spark.sql.catalyst.expressions.GlutenNativeExpressionAdapterSuite
org.apache.spark.sql.catalyst.expressions.NativeExpressionEvalHelperSuite
```

They cover selected-branch execution, unsupported fallback, validation and
execution failure propagation, expected mismatches, literal/native folding,
sparse bound input, nonfoldable literals, strict and overridden comparators,
borrowed nested NULL results, evaluator reuse and actual ownership lifetimes.
Existing vector lifecycle tests remain separate; comparator tests do not
replace them with deep copies or dummy native buffers.

## Vector API regression contracts

Superclass bookkeeping columns use Spark's managed `OnHeapColumnVector`;
native values and descriptors retain their separate ownership. The leak
regression runs in an owned NMT-enabled JVM and measures native `Other`
malloc bytes after repeated construction and closure, not process RSS.

Use the standard decimal getter and setter signatures. Velox short decimals
use int64 slots even at precision <= 9; long decimals use int128 slots, not
Spark's byte-array representation. Getter NULL handling and setter null-bit
behavior follow Spark's contracts.

Java rows implement Spark's `InternalRow` API. CalendarInterval uses Spark's
standard logical field getters, including Spark 3.5's final
`getInterval`: the three field views read the existing packed 16-byte native
value without copying data or changing its native descriptor or arithmetic.

## Offline selection

The imported
[`native-expression-suites.json`](../../gluten-ut/spark41/src/test/resources/native-expression-suites.json)
records the original suite/exclusion inventory, not a support whitelist or a
replacement for upstream suite definitions. Specialized suites and
reference-only families keep their existing mechanisms.

Use the project's `./build/mvn` wrapper and ScalaTest tooling with Java 17.
The module is `gluten-ut/spark41`; build native libraries first and select
same-worktree reactor artifacts and matching native releases. Follow the build
guide when installing sibling modules for per-module tests. Offline execution
requires dependencies to be cached already; shared `.m2` SNAPSHOT coordinates
alone are not provenance. Target formatting to changed files; `spotlessFiles`
takes regexes, not globs.
