# Expression benchmark usage

Run one scalar expression over prepared inputs using Spark JVM code generation
and the Velox native expression evaluator. This is a microbenchmark of the
expression execution path, not an end-to-end SQL query or a bare function loop.

## Prepare and run

Run from the Gluten checkout in the Linux development environment. Use Java 17
HotSpot and matching native libraries already built in `cpp/build`. The launcher
builds JVM artifacts; it does not build native code.

```bash
export JAVA_HOME=/usr/lib/jvm/msopenjdk-17

# Build/install the JVM reactor, export the test classpath, then list functions.
dev/run-expression-bench.sh --list

# Reuse those artifacts to inspect a function's cases.
dev/run-expression-bench.sh --skip-build --list ltrim

# Measure one registered case.
dev/run-expression-bench.sh --skip-build \
  --cases ltrim/l13-half-even \
  --rows 4000000 --batch-size 10240 \
  --seed 20260912 --key-cardinality 4000000 \
  --warmup-seconds 10 --measurement-seconds 50
```

`--skip-build` must be the first argument. It skips both Maven compilation and
classpath export. After changing Java/Scala or case resources, run without it
before relying on the outputs. After native changes, rebuild native code first
and then rebuild/package the JVM artifacts so they contain the new libraries.

The launcher uses the `java-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta`
profiles. It retains the backend `classes` and `test-classes` directories and
exports dependencies through the `gluten-arrow,backends-velox` reactor.
Normal Maven formatting and style checks remain enabled.

Selectors are either a comma-separated function list or `--cases`, not both:

```bash
dev/run-expression-bench.sh --skip-build ltrim,rtrim \
  --warmup-seconds 10 --measurement-seconds 50

dev/run-expression-bench.sh --skip-build \
  --cases 'ltrim/l13-*,rtrim/l13-*' \
  --warmup-seconds 10 --measurement-seconds 50
```

Quote patterns so the shell does not expand them. `*` and `?` match within a
function/case segment, not across `/`. Unknown or unmatched selectors fail.
`--list` accepts an optional function list but cannot be combined with run
options or `--config`.

## Options and configuration

| CLI option | JSON field | Default |
| --- | --- | --- |
| Positional function list | `functions` | No default |
| `--cases` | `cases` | No default |
| `--rows` | `rows` | 4000000 |
| `--batch-size` | `batchSize` | 10240 |
| `--seed` | `seed` | 20260912 |
| `--key-cardinality` | `keyCardinality` | Input row count |
| `--warmup-seconds` | `warmupSeconds` | 10 |
| `--measurement-seconds` | `measurementSeconds` | 60 |
| `--async-profiler` | `asyncProfiler` | Disabled |
| `--profile-event` | `profileEvent` | `cpu` when profiling is enabled |
| `--profile-output` | `profileOutput` | `target/expression-benchmark-profiles` |

Explicit warmup and measurement durations must be supplied together and be
positive integer seconds. Rows and batch size are positive integers.
The input seed is a signed 64-bit integer; key cardinality is positive and
controls generator inputs, not a guarantee about the output's distinct count.
The runner performs at least two measured iterations per engine.

For example, save this as `target/trim-run.json`:

```json
{
  "cases": ["ltrim/l13-half-even"],
  "rows": 4000000,
  "batchSize": 10240,
  "seed": 20260912,
  "keyCardinality": 4000000,
  "warmupSeconds": 10,
  "measurementSeconds": 50
}
```

```bash
dev/run-expression-bench.sh --skip-build --config target/trim-run.json
```

JSON selectors are nonempty arrays, not comma-separated strings. CLI options
override JSON values; a CLI selector replaces the JSON selector. Each source
must be valid on its own: overrides do not hide invalid JSON values.
Unknown fields, repeated CLI options, duplicate JSON keys and malformed types
are errors.

Relative CLI paths resolve from the directory where the launcher was invoked.
Profiler paths supplied by JSON resolve from the configuration file's directory.

## Add a catalog case

Resources live in `backends-velox/src/test/resources/expression-benchmark/cases`.
A function file contains one four-column Markdown table:

```markdown
# ltrim

| case | inputs | expression | description |
| --- | --- | --- | --- |
| ascii-half-32 | `input = ltrimPattern(length=32, pattern="half-even", nullPercent=0, utf8=false)` | `ltrim(input)` | 32-byte ASCII inputs; even batch-local rows need trimming. |
```

The file `ltrim.md` and local ID `ascii-half-32` form the selector
`ltrim/ascii-half-32`. For a new function file, add its filename to `index.txt`.
Use one unique lowercase `.md` filename per line, without blank or comment
lines. Each file needs at least one case.

The exact column names are `case`, `inputs`, `expression`, `description`.
IDs use lowercase letters, digits, `_` or `-`. Input and SQL cells each contain
one complete single-line code span. Descriptions are plain, nonempty text;
HTML, links and mixed markup are not accepted there. Escape a literal table
pipe as `\|`, including within a code span.

Inputs use named generator arguments, with bindings separated by semicolons:

```text
a = standard.long(); b = hashedLong(column=1)
(value, chars) = rtrimCustom()
```

Argument values are integers, booleans or double-quoted strings. Column names
must be unique, including case-insensitive duplicates.

Use the standalone `none` marker for an expression without input columns:

```markdown
| case | inputs | expression | description |
| --- | --- | --- | --- |
| seeded | `none` | `rand(7)` | Explicit seed without input columns. |
```

`none` produces an empty schema, empty JVM rows and zero-column native batches.
`--rows` and `--batch-size` still determine the logical row count and batch
boundaries. No placeholder input column is generated. Blank input cells remain
invalid, and `none` cannot be combined with generator bindings.

Each native evaluation releases its temporary zero-column input handle on both
success and failure. The underlying empty-schema batch stays cached until task
teardown. Nonempty input handles remain borrowed and are not closed by evaluation.

Common generator choices:

| Generator | Arguments and constraints |
| --- | --- |
| `standard.long/int/double/date/timestamp/binary/boundedInt/intArray/stringArray/struct/timestampString` | Basic prepared inputs; see `Data.scala` for exact distributions. |
| `standard.string`, `standard.stringArray` | Optional nonnegative `length`, default 10. |
| `standard.fractionalDouble`, `standard.positiveDouble` | No arguments; key plus 0.125, with alternating signs for fractionalDouble. |
| `standard.text` | No arguments; mixed-case words, a changing numeric key and UTF-8 text. |
| `standard.base64String`, `standard.hexString`, `standard.jsonArrayString`, `standard.urlString` | No arguments; valid encoded strings prepared before evaluation. JSON arrays cycle through empty and three-element arrays containing NULL. |
| `standard.dateString` | No arguments; ISO dates cycling through 2020-2029, including leap days. |
| `standard.intMap`, `standard.mapEntries` | No arguments; two unique string keys with bounded integer values and a periodically NULL second value, as a map or an array of key/value structs. |
| `rtrimPattern`, `ltrimPattern`, `trimPattern` | Required `length >= 2` in bytes and `pattern`; optional `nullPercent` from 0 to 100 and `utf8`, default false. |
| `rtrimBoundary`, `ltrimBoundary` | No arguments; deterministic boundary/empty-string distribution. |
| `rtrimCustom`, `ltrimCustom` | No arguments; bind both value and trim-character columns. |
| `hashedLong` | Required nonnegative `column` to distinguish changing columns. |
| `hashedString`, `hashedBinary` | Required `column` and positive `length`; optional `prefix` smaller than `length`, and `nullPercent` from 0 to 100. |
| `nullableBoundedInt` | Optional `nullPercent` from 0 to 100; bounded integer values. |

Trim patterns are `none`, `first`, `cluster`, `half-even`, `last`,
`penultimate`, and `all`. Their positions are batch-local; for example,
`cluster` means the first 16 rows of each batch.

New distributions belong in `Data.scala`, outside timing. Reuse existing
generators where possible. Update catalog registration/count expectations when
adding files or cases. A listed case is not a promise of native support:
unsupported native preparation is reported as `SKIPPED`, not as a timing result.

The preparation path accepts deterministic and nondeterministic scalar
expressions, but rejects aggregates, windows, generators and subqueries.
Function support is checked against the actual native build during preparation,
not a static case exclusion list. An unsupported conversion or rejected native
validation skips that case: the launcher reports `SKIPPED`, and the suite reports
a runtime cancellation (`canceled`). Other preparation/execution errors and
deterministic result mismatches still fail. Catalog cases are registered
regardless of Spark version; a version-related bug is not a reason to skip them.

Each Benchmark lazily parses and analyzes its SQL once against its input schema.
A shared RuleExecutor then applies Spark ReplaceExpressions, ComputeCurrentTime,
ReplaceCurrentLike with the active session CatalogManager, the registered With
rewrite when present, and ConstantFolding. It does not run the full optimizer or
the rules that would replace Project(LocalRelation) with precomputed data.
The LocalRelation supplies only schema; prepared rows and native batches are
passed directly to the expression evaluators, not installed into a scan operator.

The resulting Catalyst expression is an unexecuted template. A serialization
copy, following Spark's expression tests, is first converted and compiled into
the Native evaluator, then used to generate the JVM evaluator. Native execution
uses its compiled handle, not the Catalyst object, so execution state remains
independent. Correctness capture obtains a separate copy, including mutable
objects held inside replacement evaluator literals. All copies retain resolved
seeds and input identities. Native conversion and validation still run normally,
including existing blacklist checks.
There is no Native-only wrapper preservation or JVM fallback counted as Native
execution. For example, an Encode mapping alone does not establish support for
the StaticInvoke produced by Spark's replacement. The encode case remains
registered and is canceled at runtime if that prepared form is unsupported;
it is not statically ignored.

The added scalar cases cover representative signatures, not every overload.
Array aggregate, filter, exists, zip_with, map_filter and transform_values are
higher-order scalar expressions, not SQL aggregate operators. Their array/map
inputs, and encoded strings for parsing functions, are generated outside timing.
The current_timestamp, current_date, current_timezone, current_schema,
current_database and current_catalog constant cases use values fixed during
common preparation. Native, JVM and fresh correctness evaluators use the same
values for that Benchmark instance; a new instance reads its context anew.
Timestamp and session-string results are compared exactly. Together with
version/constant and typeof/constant, these cases measure constant-output costs,
not per-row clock, catalog, version or type discovery.

The `rand/seeded` and `rand/unseeded` cases use `none` inputs. `rand(7)` keeps its
explicit seed; common analysis resolves the seed for `rand()` once.
Both engines and fresh correctness evaluators inherit that seed, but maintain
independent RNG state. `--seed` controls input generators, not SQL Rand seeds.
State advances through normal evaluation across batches and iterations, without
per-batch resets. Equal seeds do not imply equal results across engines, so
nondeterministic value comparison remains disabled. The benchmark does not add an
`expression.dedup_non_deterministic` override. It inherits the OSS runtime's
shared query configuration and the pinned Velox defaults. Cross-engine RNG
equivalence is not implied by the benchmark's isolated evaluator copies.

The randn/seeded, randn/unseeded and uuid/no-input cases follow the same no-input,
shared-analysis and independent-state policy.

The `monotonically_increasing_id/no-input` case also uses `none` inputs. It
advances each engine's partition-local ID counter during evaluation. The suite
checks execution and result shape without comparing these nondeterministic
values across engines. Select it with
`--cases monotonically_increasing_id/no-input`.

## Collect CPU or allocation profiles

Use an async-profiler installation whose `lib/libasyncProfiler.so` is compatible
with the async-profiler Java API on the benchmark classpath.

```bash
dev/run-expression-bench.sh --skip-build \
  --cases ltrim/l13-half-even \
  --rows 4000000 --batch-size 10240 \
  --warmup-seconds 10 --measurement-seconds 50 \
  --async-profiler "$ASYNC_PROFILER_HOME" \
  --profile-event cpu \
  --profile-output target/expression-profiles
```

The runner reports the actual absolute output root once on stderr:

```text
Expression benchmark profiles: /.../target/expression-profiles/profile-...
```

Each case has separate files below that root:

```text
ltrim/l13-half-even/vanilla/profile.collapsed
ltrim/l13-half-even/native/profile.collapsed
```

CPU profiles use a 1 ms interval, DWARF native stacks and thread labels.
Allocation mode uses `--profile-event alloc` with a 512 KB allocation interval;
it is not a complete measurement of native malloc traffic.
Profile event/output options require `--async-profiler`.

Profiles cover the measured engine phase, including gaps between iterations;
warmup is excluded. Profile weights are not the same as benchmark wall time.
Keep profiled timings separate from unprofiled performance comparisons.

## Measurement and correctness scope

Analysis, expression conversion/compilation, input generation and conversion to
native batches happen before timing. Each iteration reuses those prepared inputs.
JVM timing includes generated expression execution and the compiler-blackhole
consumer. Native timing includes expression evaluation, result-vector creation,
JNI/handle work, and closing output batches, but no additional per-row result
scan by the default consumer.

The launcher supplies the required compiler-blackhole flags, uses `-Xmx4g`
without fixed `-Xms`, and does not pin CPUs. Normal benchmark SQL settings are
UTC, ANSI disabled, case-insensitive analysis, `CODEGEN_ONLY` and
`alwaysInlineCommonExpr=false`.

Results contain Best/Avg/Stdev time, throughput and time per row, with JVM
(`vanilla`) before Native. Different engines have different internal layouts and
output-materialization costs. These are not bare-function instruction timings
or ready-made marginal cost coefficients.

Catalog correctness checks are small smoke tests for the benchmark path, with
comparison outside timing. The suite compares values for deterministic prepared
expressions. Finite Float and Double output values use the symmetric condition
`abs(a - b) <= max(absTol, relTol * max(abs(a), abs(b)))`:

| Output type | Absolute tolerance | Relative tolerance |
| --- | --- | --- |
| Float | 1e-6 | 1e-5 |
| Double | 1e-12 | 1e-12 |

Float comparisons calculate the difference and tolerance in Double precision.
The same policy applies recursively to array elements, struct fields and map
values. Input data, map keys and non-floating output values still use exact
typed comparison. NULL placement, nullability, array/struct shape and unique
non-NULL map keys remain checked. Existing Spark comparison semantics are
preserved for NaN, infinity and signed zero: two NaNs compare equal, infinities
must have the same sign, and positive and negative zero compare equal.
These smoke-test tolerances are not a per-function ULP accuracy specification.

For nondeterministic expressions, including both Rand cases, the suite
reports that value comparison is skipped while retaining execution, result
type/nullability, column/row count and resource checks. It does not compare
random values across engines. Classification still uses the prepared
expression's determinism; there is no special exception for timestamp equality.

These checks do not replace Gluten expression suites as the owner of
comprehensive expression semantic correctness. Input generation and comparison
helpers still need to be correct for the cases they execute.

For a code-change comparison, retain exact source commits, runtime/library
identities, inputs, seed and batch size for each arm. Build/packaging can overwrite
mutable artifacts: do not assume an existing classpath or native library matches
the current worktree. The runner does not schedule interleaved A/B experiments or
prove repeatability from one run.
