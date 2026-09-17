---
layout: page
title: Native Expression Unit Tests
parent: Developer Overview
---
# Native Expression Unit Tests

Spark 4.1 expression suites preserve the historical query route before deciding
whether an additional direct native check is eligible. A historical fallback
does **not** enter the new path. There is no catch-a-native-failure-and-fallback
retry.

Historical evaluator families must also be preserved. Seven wrappers originally
extended `GlutenTestsCommonTrait`, not the query-based `GlutenTestsTrait`:
CallMethodViaReflection, CastWithAnsiOn, CollationExpression,
CollationRegexpExpressions, CsvExpressions, ScalaUDF, and ToPrettyString.
They retain Spark's original `ExpressionEvalHelper`, without creating a
SparkSession or attempting query/native dispatch. Their observations are
`reference-only`, not invented native/query-fallback decisions. The JSON
inventory lists them explicitly in `original_reference_only_suites`.

The query observer extends only `GlutenTestsTrait`, preserving its original
lifecycle and comparator. The standalone native trait is not mixed into that
inheritance graph. A separate lazy native checker is created only after an
explicit non-baseline activation and a successful physical-native query;
its Mock lifecycle is never invoked by the query observer.
TimeExpressions retains Spark41's original resolve-then-replace query
preparation. Prior statistics collected with the wrong evaluator families
must not be treated as an untouched historical baseline.

## First run: baseline-only observation

Baseline-only observation is the default. Pass both options explicitly to the
test JVM and use a fresh ledger path:

```text
-Dgluten.expression.baselineOnly=true
-Dgluten.expression.routeLog=/path/to/A/baseline-routes.jsonl
```

In this mode the bridge executes only the historical query route and its
original assertions. Even a confirmed old-native plan does not call the new
direct hook. Every test summary asserts and records
`new_direct_hook_invocations=0`.

After one coordinated compilation, run this exact preflight through the
standard ScalaTest Runner:

```text
-s org.apache.spark.sql.catalyst.expressions.GlutenExpressionRoutingSuite
-t "baseline-only native preflight invokes zero new direct hooks"
```

It first proves the old RTRIM query actually meets the native-plan criterion,
then observes that route with a direct hook configured to throw if invoked.
Do not run the dispatch-activation tests or standalone native helper suites
as part of this baseline-only observation.

Select the 31 affected expression suites for the baseline routing inventory:

```sh
python3 -c 'import json; d=json.load(open("gluten-ut/spark41/src/test/resources/native-expression-suites.json")); g=d["registered_suites"]; print(",".join(g["native_scalar_candidates"]+g["mixed_scalar_and_query"]))'
```

Within these 31 suites, 24 use historical query observation and seven use
historical reference-only observation. The other 45 registered suites retain their original mechanism/query/aggregate
tests and are not reclassified as native scenarios merely because they pass.
Their existing tests can be run separately, without selecting standalone
direct-native framework suites.

Only after reporting the observed baseline inventory may a subsequent run opt
into the additional route with `-Dgluten.expression.baselineOnly=false`.
The full routing-regression invocation also uses that explicit opt-in; its
named baseline preflight still scopes itself to observation mode.

## Historical route is authoritative

`GlutenExpressionTestsTrait` first calls the existing
`GlutenTestsTrait.checkEvaluationWithQuery` implementation. That implementation:

1. Preserves `canConvertToDataFrame`. Unsupported input rows remain the old
   no-evaluation case and are reported as uncovered, never native coverage.
2. Executes the original DataFrame projection and `collect()`.
3. Records the original supported-result/child-type policy and original
   `ProjectExecTransformer` eligibility statistic separately from physical
   execution evidence.
4. Runs the original query result assertions and comparator, including its
   map ordering and floating-point comparison behavior.

After the old assertions pass, direct eligibility requires exactly one
`ProjectExecTransformer` and zero Spark `ProjectExec` nodes. This includes
physically native-only evaluations excluded by the old type policy. The old
policy statistic and ledger fields are preserved, not used as a substitute for
observed execution.

The replay expression is taken directly from that native project's
`projectList`, stripped of its output alias, and bound against its actual child
output attributes. The raw input expression is not assumed to be the operation
that Spark executed. For example, the observed planner rewrites legacy
`ArrayExists` null predicates to false and lowers LikeAll/LikeAny into string
predicates.

When the planner has replaced a non-literal operation with a literal, the old
query and its assertions remain the complete route. Such checks are recorded
as `classification="planner-only-native-projection"` and
`planner_only_native_projection=true`, with raw and projected expressions;
they are not fallback and do not claim direct coverage of the eliminated
operator. This distinction is based on the actual plan API, not test names or
failure-list classification.

### Known native coverage gap: folded comparison subcases

The user explicitly accepted deferring additional raw-native coverage for the
observed folded subcases within eight `GlutenPredicateSuite` tests:
`BinaryComparison: lessThan`, `BinaryComparison: LessThanOrEqual`,
`BinaryComparison: GreaterThan`, `BinaryComparison: GreaterThanOrEqual`,
`BinaryComparison: EqualTo`, `BinaryComparison: EqualNullSafe`,
`SPARK-32110: compare special double/float values in array`, and
`SPARK-32110: compare special double/float values in struct`.

This is a gap for those **subcases, not an exclusion of the eight whole tests**.
The original assertions still run, and each folded subcase retains its
planner-only evidence. A native projection of a Boolean literal does not prove
native execution of the original array, struct, or UDT comparison.
Future targeted tests may add bound/nonfoldable inputs to exercise those raw
operators; no such expansion or gate change is part of this work.

The route result is `None` for no evaluation, `Some(false)` for an evaluation
that is not physically native-only, or `Some(true)` for physical native-only
execution. Only the last result is eligible to invoke
`checkOffloadedExpression`, and only when baseline-only observation is
explicitly disabled. Confirmed Spark fallback, mixed plans, uncovered inputs,
and baseline errors are never promoted to the new route.

Query comparisons use their own recursive `checkQueryResult` implementation,
so Scala trait linearization cannot redirect them to the stricter Spark
reference comparator. Query errors retain their original exception path.
Neither fallback results nor unsupported-input rows receive additional Spark
reference or direct native checks.

The historical session still disables `ConstantFolding`,
`ConvertToLocalRelation`, and `NullPropagation`, and sets ANSI off. Math restores
its historical session-level experimental setting. Cast/TryCast retain their
RowEncoder configuration. Random keeps its original SQL selection and exact
result assertion while using the same executed-plan offload decision.
The mixed Regex suite keeps its explicit query, planner-counter, and
query-fallback exception checks on the query entrypoint.

## Strict additional native checks

After confirmed historical offload and the original query assertions, the
private checker runs a direct native ExprSet check of the captured planned
expression against the unchanged expectation. It does not add a different
Spark unsafe-row comparison contract to the historical query route.
Failures in this confirmed-native path propagate; they do not trigger fallback.

The private confirmed-native checker delegates result comparison to the
original outer `GlutenTestsTrait.checkResult`, preserving its exact relative
floating-point and map-key ordering contract. It does not introduce a new
tolerance or change expectations. The standalone strict trait and its tests
retain their original comparator.

Velox's ArrayExists conversion preserves Spark's
`followThreeValuedLogic=false` mode by wrapping the lambda predicate in
`coalesce(predicate, false)`. It does not coalesce the final result: a NULL
input array must still return NULL. Three-valued mode leaves the predicate
unchanged.

Native-only experimental gates and timezone configuration are applied **after**
the historical route decision. They cannot turn an old fallback into a new
native candidate. Compatible declared timezones use the runtime's `extraConf`,
not a mutation of Spark's oracle or the JVM default timezone; mixed zones
receive no expression-wide override.

The row adapter derives schema from bound references, compacts sparse ordinals,
and materializes test-only nonfoldable literals as input columns. Raw UTF8 bytes,
nulls, and supported nested values are preserved. Unsupported logical input
types are not retagged as primitive types. Existing native year-month and
timestamp-NTZ result carriers are adapted only for result reading.

Mapped RuntimeReplaceable classes remain available to the native converter.
Only unmapped wrappers are desugared; blacklisted classes are not bypassed.
The original Spark reference checks retain Spark's own replacement behavior.
Integer expectations are reboxed only for Spark's typed unsafe expected-row
builder, with exact value preservation checked.

## Standalone contract

`GlutenNativeExpressionAdapterSuite` extends `NativeExpressionTestsTrait`
directly. Its standalone checks remain SparkContext-free.
`NativeExpressionEvalHelper` retains the standalone execution contract:

* Inputs and outputs remain columnar and task-scoped.
* `prepareNativeExpression(expressions: Seq[Expression], inputAttributes)` accepts
  multiple input attributes and exactly one output expression; empty or multiple
  output sequences are rejected before native allocation. Prepared `evaluate`
  and `close` retain their API.
* Inputs are borrowed; returned native batches are caller-owned.
* Explicit unsupported expressions still throw.
* There is no DataFrame, Spark job, or Spark fallback in that standalone path.
* Benchmark preparation/timing and the timed native evaluate/close path are
  unaffected.

`GlutenExpressionRoutingSuite` separately verifies actual old fallback versus
offload decisions, legacy comparator preservation, propagation of a failure
after confirmed offload, RTRIM eligibility, and uncovered unsupported inputs.
Its routing-hook probes are control-flow tests, not claims of native evaluation.

## Vector API regression contracts

Superclass bookkeeping columns use Spark's managed `OnHeapColumnVector`;
native values and descriptors retain their separate ownership. The leak
regression runs in an owned NMT-enabled JVM and measures native `Other`
malloc bytes after repeated construction and closure, not process RSS.

Use the standard decimal getter and setter signatures. Velox short decimals
use int64 slots even at precision <= 9; long decimals use int128 slots, not
Spark's byte-array representation. Getter NULL handling and setter null-bit
behavior follow Spark's contracts.

Java rows use small version-selected Java bases because javac does not accept
the Scala shim's `Nothing` bridge methods as implementations of Spark 4.x's
abstract getters. Maven selects Spark 3.x, 4.0, or 4.1 sources. CalendarInterval
uses Spark's standard logical field getters, including Spark 3.5's final
`getInterval`: the three field views read the existing packed 16-byte native
value without copying data or changing its native descriptor or arithmetic.

## Counters and fixed inventory

Per-test `[expression-route]` counters distinguish:

* `observed-native`: the old query met the offload criterion without an observed
  Spark projection.
* `confirmed-spark-fallback`: the old gate was ineligible and the observed plan has a
  Spark projection but no native project transformer.
* `mixed-or-policy-excluded`: mixed projections, old type-policy or plan-shape ineligibility, not
  automatically a physical Spark fallback.
* `no-evaluation`: the historical unsupported-input path, still uncovered.

Separate `[native-expression]` counters report successfully validated direct
native checks and failed attempts. Zero validated checks means `not native
coverage`; it is not automatically a Spark-mechanism label. Legacy
`TestStats.offloadGluten` flags are not proof of direct evaluation.

### Exact observed fallback ledger

Pass a fresh per-run/per-arm path to the test JVM:

```text
-Dgluten.expression.routeLog=/path/to/A/expression-routes.jsonl
```

The default is `target/expression-routes.jsonl`, appended within the JVM's
working directory. The bridge writes standard JSONL directly from the old
query route-decision/error points; no log parser or failure-list inference is used.
Each check records suite, exact test name, expression, SQL, result
and child logical types, bound-input types, old fallback reason, executed-plan
evidence, and `occurrence_count=1`. It also records `legacy_eligible`,
`legacy_type_policy_eligible`, `project_exec_transformer_count`,
`spark_project_exec_count`, `confirmed_spark_projection_fallback`, and
`direct_native_eligible`.
Records are serialized across concurrent
timezone workers. Output errors fail loudly.

`event_type="evaluation"` records carry one of these baseline classifications:
`native`, `confirmed-spark-fallback`, `mixed-or-policy-excluded`,
`old-unconvertible-no-evaluation`, `reference-only`, or `baselineerror`. Native routes are recorded
too, even though observation mode never invokes the new direct hook.
`event_type="test-summary"` records contain the per-test category counts and
`mixed_test`; they must not be summed as additional evaluations.

Aggregate the JSON records by suite/test/expression/types/reason and sum
`occurrence_count` to produce the final CSV/Markdown ledger. Keep
`type-policy-excluded` and `plan-policy-excluded` records distinct from confirmed
`fallback` records: the old whitelist can reject a query whose observed plan
actually contains a native transformer. A missing native project without an
observed Spark projection is plan-policy evidence, not proof of physical Spark
fallback. Direct eligibility uses native-only physical evidence, independently
of the retained old type-policy flag. A test containing
both old-native and old-fallback checks contributes only its actual fallback
checks, not a whole-test classification. Use the fixed `registered_suites`
inventory to separate benchmark/routing regression probes from the user-facing
suite report.

Historical unrepresentable inputs emit `route="uncovered"` records separately.
Their expression is deliberately not constructed; it remains null in the
record, alongside the input-row class and field count. They are not fallback
or native success. Preparation, query, or original assertion errors emit
`baselineerror` with the error and available plan evidence; unavailable
eligibility remains null rather than being guessed. New direct-native failures
also never create fallback records. Use a new ledger path for each invocation
to avoid mixing previous appended runs.

The machine-readable
[`native-expression-suites.json`](../../gluten-ut/spark41/src/test/resources/native-expression-suites.json)
retains the fixed 79-suite inventory at baseline
`e06cf951b4a245e31059a5061dd98f4acd08888f`: 76 registered suites and three
unregistered suites. Its 30 scalar candidates and one mixed suite now use
historical routing, not unconditional direct evaluation. The 28 mechanism,
four query/generator/subquery, and 13 registered aggregate suites retain their
existing mechanisms.

The original 48 named exclusions, three wrapper ignores, seven commented
assertions, and three unregistered suites remain separately recorded and
unchanged. Preserving historical fallback is not a new source exclusion or
xfail. New direct failures must not be reclassified as old fallback.

## Unified selection

From the Gluten root in the Linux development container:

```sh
python3 -c 'import json; d=json.load(open("gluten-ut/spark41/src/test/resources/native-expression-suites.json")); print(",".join([s for g in d["registered_suites"].values() for s in g]+d["adapter_suites"]))'
```

This selects 78 suites: the original 76 plus the standalone adapter and routing
regressions. Use `-Dsuites` on the Spark41 invocation, or generate `-s` arguments
for the standard ScalaTest Runner. Do not apply Spark41 selectors to every
upstream reactor module.

The actual module is `gluten-ut/spark41`, not `gluten-ut/test`. Use the selected
same-worktree reactor artifacts, Java 17, and the existing root protobuf/Delta
runtime dependencies. Shared `.m2` SNAPSHOT coordinates are not provenance.
Select and verify the correct native libraries before running; an extracted
artifact's `linux/amd64` leaf is the `cpp.releases.dir` input, because the backend
POM adds that layout.

Keep one coordinated compile/test gate for the complete batch. Target
formatting to touched files (`spotlessFiles` takes regexes, not globs); do not
reformat unrelated sources to run these tests.
