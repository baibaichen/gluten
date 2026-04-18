#!/usr/bin/env python3
"""Unified ANSI PANORAMA log analyzer.

Parses test logs produced by `verify-ansi-expressions.sh` and generates
ANSI expression matrices for all categories: cast, arithmetic, collection,
datetime, math, decimal, string.

Input: A single log file or a directory of log files.
  - Single file: parse all suites found in that file
  - Directory: scan all *-ut-*.log files and merge results

Output: Per-category markdown matrix tables + summary statistics.

Usage:
    python3 dev/analyze-panorama.py /tmp/ansi-matrix/latest/
    python3 dev/analyze-panorama.py /tmp/ansi-matrix/latest/all-ut-spark41.log
    python3 dev/analyze-panorama.py /tmp/ansi-matrix/latest/cast-ut-spark41.log
"""
import argparse
import glob
import os
import re
import sys
from collections import OrderedDict, defaultdict

# ── Constants ──────────────────────────────────────────────

ANSI_ESC = re.compile(r'\x1b\[[0-9;]*m')

# ScalaTest result patterns
SUITE_RE = re.compile(r'^(\w+(?:Suite|Test)):$')
PASSED_RE = re.compile(r'^- (.+?) \(\d+.*\)$')
FAILED_RE = re.compile(r'^- (.+?) \*\*\* FAILED \*\*\* \(\d+.*\)$')
IGNORED_RE = re.compile(r'^- (.+?) !!! IGNORED !!!$')

# PANORAMA summary patterns (pipe-separated, indented)
PANORAMA_SUMMARY_BEGIN = 'ANSI_PANORAMA_SUMMARY_BEGIN'
PANORAMA_SUMMARY_END = 'ANSI_PANORAMA_SUMMARY_END'
PANORAMA_RECORD_RE = re.compile(r'^\s+([NE])\|(.+)\|(.+)\|(\w+)\|(\w+)$')
PANORAMA_SUITE_RE = re.compile(r'^(\w+(?:Suite|Test)):$')
PANORAMA_TEST_RE = re.compile(r'^\s{2}(.+):$')

# Meta field patterns
META_CAST_RE = re.compile(r'fromType=([^,]+),toType=(.+)')
META_OPERATOR_RE = re.compile(r'operator=(\w+)')
META_EXPR_RE = re.compile(r'expr=(\w+)')

# Failure classification
NO_EXCEPTION_RE = re.compile(
    r'Expected exception .+ to be thrown, but no exception was thrown')
MSG_MISMATCH_RE = re.compile(r"Expected error message containing")
WRONG_EXCEPTION_RE = re.compile(r'Expected \S+ but got \S+:')

# Suite → Category mapping
SUITE_CATEGORY = {
    'GlutenCastWithAnsiOnSuite': 'cast',
    'GlutenCastWithAnsiOffSuite': 'cast',
    'GlutenTryCastSuite': 'cast',
    'GlutenArithmeticExpressionSuite': 'arithmetic',
    'GlutenTryEvalSuite': 'arithmetic',
    'GlutenCollectionExpressionsSuite': 'collection',
    'GlutenDateExpressionsSuite': 'datetime',
    'GlutenIntervalExpressionsSuite': 'datetime',
    'GlutenMathExpressionsSuite': 'math',
    'GlutenDecimalExpressionSuite': 'decimal',
    'GlutenStringExpressionsSuite': 'string',
    # No PANORAMA, test results only
    'GlutenDateFunctionsSuite': 'datetime',
    'GlutenUrlFunctionsSuite': 'string',
    'GlutenDataFrameAggregateSuite': 'aggregate',
    'GlutenQueryExecutionAnsiErrorsSuite': 'errors',
}

# Cast suite → column (order determines table column order)
CAST_SUITE_COLUMN = OrderedDict([
    ('GlutenCastWithAnsiOnSuite', 'ANSI(on)'),
    ('GlutenTryCastSuite', 'TRY'),
    ('GlutenCastWithAnsiOffSuite', 'LEGACY'),
])

# Arithmetic suite → column
ARITH_SUITE_COLUMN = {
    'GlutenArithmeticExpressionSuite': 'ANSI(on)',
    'GlutenTryEvalSuite': 'TRY',
}

# Mechanism B category → {suite: column}
MECH_B_SUITE_COLUMN = {
    'collection': {'GlutenCollectionExpressionsSuite': 'Collection'},
    'datetime': {
        'GlutenDateExpressionsSuite': 'Date',
        'GlutenIntervalExpressionsSuite': 'Interval',
    },
    'math': {'GlutenMathExpressionsSuite': 'Math'},
    'decimal': {'GlutenDecimalExpressionSuite': 'Decimal'},
    'string': {'GlutenStringExpressionsSuite': 'String'},
}

# Category display order
CATEGORY_ORDER = [
    'cast', 'arithmetic',
    'collection', 'datetime', 'math', 'decimal', 'string',
    'aggregate', 'errors',
]


# ── Phase 1 & 2: Log Parsing ──────────────────────────────

def parse_log(path):
    """Parse a single log file → (test_results, panorama_entries, lines)."""
    with open(path, 'r', errors='replace') as f:
        raw_lines = f.readlines()
    lines = [ANSI_ESC.sub('', l).rstrip() for l in raw_lines]

    # Phase 1: ScalaTest results
    current_suite = None
    test_results = []
    for i, line in enumerate(lines):
        m = SUITE_RE.match(line)
        if m:
            current_suite = m.group(1)
            continue
        m = FAILED_RE.match(line)
        if m:
            test_results.append({
                'suite': current_suite, 'test': m.group(1),
                'result': 'FAILED', 'line': i})
            continue
        m = IGNORED_RE.match(line)
        if m:
            test_results.append({
                'suite': current_suite, 'test': m.group(1),
                'result': 'IGNORED', 'line': i})
            continue
        m = PASSED_RE.match(line)
        if m:
            test_results.append({
                'suite': current_suite, 'test': m.group(1),
                'result': 'PASSED', 'line': i})
            continue

    # Phase 2: PANORAMA summary
    panorama_entries = []
    in_summary = False
    cur_suite = None
    cur_test = None
    for i, line in enumerate(lines):
        content = line
        colon_idx = content.find(': ')
        if colon_idx >= 0:
            content = content[colon_idx + 2:]

        if PANORAMA_SUMMARY_BEGIN in line:
            in_summary = True
            continue
        if PANORAMA_SUMMARY_END in line:
            in_summary = False
            cur_suite = cur_test = None
            continue
        if not in_summary:
            continue

        m = PANORAMA_SUITE_RE.match(content)
        if m:
            cur_suite = m.group(1)
            continue
        m = PANORAMA_TEST_RE.match(content)
        if m:
            cur_test = m.group(1)
            continue
        m = PANORAMA_RECORD_RE.match(content)
        if m:
            method_tag, expression, meta, offload, status = m.groups()
            panorama_entries.append({
                'method': 'checkException' if method_tag == 'E' else 'checkExpression',
                'expression': expression,
                'meta': meta,
                'offload': offload == 'OFFLOAD',
                'suite': cur_suite,
                'test_name': cur_test,
                'test_status': status,
                'line': i,
            })

    # Phase 3: Classify failures
    for t in test_results:
        if t['result'] != 'FAILED':
            t['fail_cause'] = None
            continue
        cause = None
        start = max(0, t['line'] - 200)
        for li in range(start, t['line'] + 1):
            if NO_EXCEPTION_RE.search(lines[li]):
                cause = 'NO_EXCEPTION'
                break
            if MSG_MISMATCH_RE.search(lines[li]):
                cause = 'MSG_MISMATCH'
                break
            if WRONG_EXCEPTION_RE.search(lines[li]):
                cause = 'WRONG_EXCEPTION'
                break
        t['fail_cause'] = cause or 'OTHER'

    return test_results, panorama_entries


def load_logs(path):
    """Load from file or directory → merged (test_results, panorama_entries)."""
    if os.path.isfile(path):
        return parse_log(path)
    elif os.path.isdir(path):
        all_tests = []
        all_panorama = []
        for f in sorted(glob.glob(os.path.join(path, '*-ut-*.log'))):
            t, p = parse_log(f)
            all_tests.extend(t)
            all_panorama.extend(p)
        return all_tests, all_panorama
    else:
        print(f"Error: {path} is not a file or directory", file=sys.stderr)
        sys.exit(1)


# ── Helpers ────────────────────────────────────────────────

def normalize_type(t):
    """Normalize a Spark type name to display name."""
    t = t.strip()
    tl = t.lower()
    mapping = {
        'boolean': 'Boolean', 'tinyint': 'TinyInt', 'byte': 'TinyInt',
        'smallint': 'SmallInt', 'short': 'SmallInt',
        'int': 'Int', 'integer': 'Int', 'bigint': 'BigInt', 'long': 'BigInt',
        'float': 'Float', 'real': 'Float', 'double': 'Double',
        'string': 'String', 'binary': 'Binary', 'date': 'Date',
        'timestamp': 'Timestamp', 'timestamp_ntz': 'TimestampNTZ',
    }
    if tl in mapping:
        return mapping[tl]
    if tl.startswith('decimal'):
        return 'Decimal'
    if tl.startswith('time(') or tl == 'time':
        return 'Time'
    if tl.startswith('char(') or tl == 'char':
        return 'Char'
    if tl.startswith('varchar(') or tl == 'varchar':
        return 'Varchar'
    if tl == 'void' or tl == 'null':
        return 'Void'
    if 'interval' in tl:
        return 'YearMonthInterval' if ('year' in tl or 'month' in tl) else 'DayTimeInterval'
    # Container types with element info: array<int>, map<string,int>, struct<a:int,b:string>
    m = re.match(r'array<(.+)>', tl)
    if m:
        return f'Array\\<{normalize_type(m.group(1))}\\>'
    m = re.match(r'map<(.+),\s*(.+)>', tl)
    if m:
        return f'Map\\<{normalize_type(m.group(1))},{normalize_type(m.group(2))}\\>'
    m = re.match(r'struct<(.+)>', tl)
    if m:
        # Extract field types only (strip field names)
        fields = []
        for field in _split_struct_fields(m.group(1)):
            parts = field.split(':', 1)
            if len(parts) == 2:
                fields.append(normalize_type(parts[1]))
            else:
                fields.append(normalize_type(parts[0]))
        return f'Struct\\<{",".join(fields)}\\>'
    # Bare container names (legacy typeName format)
    if tl in ('array', 'map', 'struct'):
        return tl.capitalize()
    return t


def _split_struct_fields(s):
    """Split struct fields respecting nested angle brackets and parentheses."""
    fields = []
    depth = 0
    current = []
    for ch in s:
        if ch in '<(':
            depth += 1
            current.append(ch)
        elif ch in '>)':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            fields.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        fields.append(''.join(current).strip())
    return fields


def group_by_category(test_results, panorama_entries):
    """Split data by category using SUITE_CATEGORY mapping."""
    cat_tests = defaultdict(list)
    cat_panorama = defaultdict(list)
    for t in test_results:
        cat = SUITE_CATEGORY.get(t['suite'])
        if cat:
            cat_tests[cat].append(t)
    for p in panorama_entries:
        cat = SUITE_CATEGORY.get(p['suite'])
        if cat:
            cat_panorama[cat].append(p)
    return cat_tests, cat_panorama


def suite_test_map(test_results, suite_set):
    """Build {suite: {test_name: result_dict}} for given suites.

    When the same suite+test appears multiple times (e.g., from spark40 + spark41 logs),
    keep the worst result: FAILED > PASSED > IGNORED.
    """
    _result_rank = {'FAILED': 3, 'PASSED': 2, 'IGNORED': 1}
    m = defaultdict(dict)
    for t in test_results:
        if t['suite'] in suite_set:
            existing = m[t['suite']].get(t['test'])
            if existing is None or _result_rank.get(t['result'], 0) > _result_rank.get(existing['result'], 0):
                m[t['suite']][t['test']] = t
    return m


def _fail_icon(causes):
    """Return fail icon: 🚫 for pure NO_EXCEPTION, ❌ for OTHER or mixed."""
    if causes == {'NO_EXCEPTION'}:
        return '🚫'
    return '❌'


def determine_cell(entries, test_names, st_map, suite):
    """Determine matrix cell value from entries + test results."""
    if not entries:
        return '➖', ''

    statuses = set()
    causes = set()
    for tn in test_names:
        if tn in st_map.get(suite, {}):
            t = st_map[suite][tn]
            statuses.add(t['result'])
            if t.get('fail_cause'):
                causes.add(t['fail_cause'])

    has_offload = any(e['offload'] for e in entries)

    if 'FAILED' in statuses:
        icon = _fail_icon(causes)
        failed = [tn for tn in test_names
                  if st_map.get(suite, {}).get(tn, {}).get('result') == 'FAILED']
        return icon, '; '.join(sorted(failed)[:3])
    elif not has_offload:
        return '⚠️ FALLBACK', ''
    else:
        return '✅', ''


def determine_cast_cell(entries, test_names, st_map, suite):
    """Like determine_cell but treats mixed offload+fallback as FALLBACK.

    For Cast, a type pair with both OFFLOAD (from null-cast constant folding)
    and FALLBACK (from real cast) should be classified as FALLBACK.
    """
    if not entries:
        return '➖', ''

    statuses = set()
    causes = set()
    for tn in test_names:
        if tn in st_map.get(suite, {}):
            t = st_map[suite][tn]
            statuses.add(t['result'])
            if t.get('fail_cause'):
                causes.add(t['fail_cause'])

    has_offload = any(e['offload'] for e in entries)
    has_fallback = any(not e['offload'] for e in entries)

    if 'FAILED' in statuses:
        icon = _fail_icon(causes)
        failed = [tn for tn in test_names
                  if st_map.get(suite, {}).get(tn, {}).get('result') == 'FAILED']
        return icon, '; '.join(sorted(failed)[:3])
    elif not has_offload or has_fallback:
        return '⚠️ FALLBACK', '; '.join(sorted(test_names)[:3])
    else:
        return '✅', ''


# ── Cast ───────────────────────────────────────────────────

def analyze_cast(test_results, panorama_entries):
    """Analyze cast category → (success_rows, fallback_rows, failed_rows, unsupported_types).

    Each row is a dict with keys: from, to, ANSI(on), TRY, LEGACY, and detail fields.
    Rows are classified into three groups:
      - success: all present suites are ✅ or — (no ❌, no FALLBACK)
      - fallback: all present suites are ⚠️ FALLBACK
      - failed: any suite has ❌
    """
    st_map = suite_test_map(test_results, set(CAST_SUITE_COLUMN))

    # Filter test fixture types
    FIXTURE_TYPES = {'examplebasetype', 'examplesubtype'}

    # Extract type pairs from meta
    pair_data = defaultdict(lambda: defaultdict(list))
    for e in panorama_entries:
        m = META_CAST_RE.search(e.get('meta', ''))
        if not m:
            continue
        ft, tt = normalize_type(m.group(1)), normalize_type(m.group(2))
        if ft == 'Null' or ft == tt:
            continue
        if ft.lower() in FIXTURE_TYPES or tt.lower() in FIXTURE_TYPES:
            continue
        pair_data[(ft, tt)][e['suite']].append(e)

    # Build rows with cell values
    all_rows = []
    for (ft, tt) in sorted(pair_data.keys()):
        row = {'from': ft, 'to': tt}
        for suite, col in CAST_SUITE_COLUMN.items():
            entries = pair_data[(ft, tt)].get(suite, [])
            tests = set(en['test_name'] for en in entries)
            row[col], row[col + '_detail'] = determine_cast_cell(entries, tests, st_map, suite)
        all_rows.append(row)

    # Classify into three groups
    success_rows = []
    fallback_rows = []
    failed_rows = []
    for r in all_rows:
        cols = [r.get(c, '➖') for c in CAST_SUITE_COLUMN.values()]
        present = [c for c in cols if c != '➖']
        if any(_is_fail(c) for c in present):
            failed_rows.append(r)
        elif present and all('FALLBACK' in c for c in present):
            fallback_rows.append(r)
        else:
            success_rows.append(r)

    return success_rows, fallback_rows, failed_rows


def print_cast(test_results, panorama_entries):
    """Print cast section with three groups: fallback list, success table, failed table."""
    success_rows, fallback_rows, failed_rows = analyze_cast(
        test_results, panorama_entries)
    col_names = list(CAST_SUITE_COLUMN.values())  # ANSI(on), LEGACY, TRY

    # ── Group 1: Fallback ──
    if fallback_rows:
        print()
        print("### 不支持（FALLBACK）")
        print()
        print("> 详见 `ConverterUtils.getTypeNode()` in "
              "`gluten-substrait/src/main/scala/org/apache/gluten/expression/ConverterUtils.scala`")

        # 1a. Unsupported primitive types: types that ONLY appear in fallback rows,
        #     excluding container types (Array, Map, Struct)
        CONTAINER_PREFIXES = ('Array', 'Map', 'Struct')
        all_fb_types = set()
        for r in fallback_rows:
            all_fb_types.add(r['from'])
            all_fb_types.add(r['to'])
        non_fb_types = set()
        for r in success_rows + failed_rows:
            non_fb_types.add(r['from'])
            non_fb_types.add(r['to'])
        unsupported_primitives = sorted(
            t for t in (all_fb_types - non_fb_types)
            if not t.startswith(CONTAINER_PREFIXES))

        if unsupported_primitives:
            print()
            print(f"不支持的类型: {', '.join(unsupported_primitives)}")

        # 1b. Fallback type pairs table: exclude pairs where from OR to is
        #     an unsupported primitive (those are already explained above)
        unsup_set = set(unsupported_primitives)
        remaining_fb = [r for r in fallback_rows
                        if r['from'] not in unsup_set and r['to'] not in unsup_set]
        if remaining_fb:
            print()
            print("| From | To | ANSI | TRY | LEGACY | 边界 |")
            print("|------|-----|------|-----|--------|------|")
            # Group by (from, status pattern)
            fb_pattern_groups = defaultdict(list)
            for r in remaining_fb:
                pattern = tuple(r.get(c, '➖') for c in col_names)
                fb_pattern_groups[(r['from'], pattern)].append(r)
            for key in sorted(fb_pattern_groups):
                ft, pattern = key
                rows = fb_pattern_groups[key]
                tos = ', '.join(sorted(r['to'] for r in rows))
                suite_cells = []
                for i, col in enumerate(col_names):
                    suite_cells.append('⚠️' if pattern[i] != '➖' else '➖')
                # Collect test names from detail fields
                details = set()
                for r in rows:
                    for c in col_names:
                        d = r.get(c + '_detail', '')
                        if d:
                            for t in d.split('; '):
                                details.add(t)
                detail_str = '; '.join(sorted(details)[:3])
                print(f"| **{ft}** | {tos} | {' | '.join(suite_cells)} | {detail_str} |")

    # ── Group 2: Success (table, grouped by from + status pattern) ──
    if success_rows:
        print()
        print("### 全部通过")
        print()
        print("| From | To Types | ANSI | TRY | LEGACY |")
        print("|------|----------|------|-----|--------|")
        # Group by (from, status_pattern) so different coverage patterns get separate rows
        pattern_groups = defaultdict(list)
        for r in success_rows:
            pattern = tuple(r.get(c, '➖') for c in col_names)
            pattern_groups[(r['from'], pattern)].append(r)
        for key in sorted(pattern_groups):
            ft, pattern = key
            rows = pattern_groups[key]
            tos = ', '.join(sorted(r['to'] for r in rows))
            suite_cells = []
            for i, col in enumerate(col_names):
                suite_cells.append('✅' if pattern[i] != '➖' else '➖')
            print(f"| **{ft}** | {tos} | {' | '.join(suite_cells)} |")

    # ── Group 3: Failed (table, folded by from + status pattern) ──
    if failed_rows:
        print()
        print("### 失败")
        print()
        print("| From | To | ANSI | TRY | LEGACY | 边界 |")
        print("|------|-----|------|-----|--------|------|")
        # Group by (from, status_pattern)
        pattern_groups = defaultdict(lambda: {'tos': [], 'details': []})
        for r in failed_rows:
            pattern = tuple(r.get(c, '➖') for c in col_names)
            key = (r['from'], pattern)
            pattern_groups[key]['tos'].append(r['to'])
            for c in col_names:
                d = r.get(c + '_detail', '')
                if d and d not in pattern_groups[key]['details']:
                    pattern_groups[key]['details'].append(d)
        for key in sorted(pattern_groups):
            ft, pattern = key
            g = pattern_groups[key]
            tos = ', '.join(sorted(g['tos']))
            detail = '; '.join(g['details'][:3])
            cells = ' | '.join(pattern)
            print(f"| **{ft}** | {tos} | {cells} | {detail} |")

    # Summary
    total = len(success_rows) + len(fallback_rows) + len(failed_rows)
    print(f"\nTotal type pairs: {total} "
          f"(✅ {len(success_rows)} + ⚠️ {len(fallback_rows)} fallback + ❌ {len(failed_rows)} failed)")


# ── Arithmetic ─────────────────────────────────────────────

def infer_scenario(test_name):
    """Infer scenario from test name."""
    if not test_name:
        return 'basic'
    tn = test_name.lower()
    if 'throw exceptions from children' in tn or 'exception from children' in tn:
        return 'exception_children'
    if 'overflow' in tn or 'out-of-range' in tn:
        return 'overflow'
    if 'divide by zero' in tn or 'division by zero' in tn:
        return 'divzero'
    if 'sql text context' in tn or 'sql context' in tn or 'query context' in tn:
        return 'sql_context'
    if 'decimal' in tn and 'divide' in tn:
        return 'decimal_divzero'
    for sp in ['spark-33008', 'spark-34920', 'spark-26218']:
        if sp.replace('-', '') in tn.replace(' ', '').replace('-', ''):
            return sp.replace('-', '')
    if any(x in tn for x in ['try_add', 'try_subtract', 'try_multiply', 'try_divide', 'try_mod']):
        return 'try'
    return 'basic'


SCENARIO_LABELS = {
    'overflow': '整数溢出', 'divzero': '除零',
    'decimal_divzero': 'Decimal/Double 除零', 'sql_context': 'SQL context',
    'try': 'try', 'exception_children': '子表达式异常',
    'spark33008': 'SPARK-33008', 'spark34920': 'SPARK-34920', 'spark26218': 'SPARK-26218',
}

ARITH_ORDER = ['Add', 'Subtract', 'Multiply', 'Divide', 'IntegralDivide',
               'Remainder', 'Pmod', 'Abs', 'UnaryMinus']


def print_arithmetic(test_results, panorama_entries):
    """Print arithmetic section with three groups: fallback, success, failed."""
    st_map = suite_test_map(test_results, set(ARITH_SUITE_COLUMN))
    col_names = list(ARITH_SUITE_COLUMN.values())

    # Extract ops
    op_data = defaultdict(lambda: defaultdict(list))
    for e in panorama_entries:
        m = META_OPERATOR_RE.search(e.get('meta', ''))
        if not m:
            continue
        op_data[m.group(1)][e['suite']].append(e)

    # Group by (operator, scenario)
    scenario_rows = defaultdict(lambda: {'tests': set(), 'suites': defaultdict(list)})
    for operator in ARITH_ORDER:
        if operator not in op_data:
            continue
        test_groups = defaultdict(lambda: defaultdict(list))
        for suite, entries in op_data[operator].items():
            for en in entries:
                test_groups[en['test_name']][suite].append(en)

        for test_name, suite_entries in test_groups.items():
            scenario = infer_scenario(test_name)
            key = (operator, scenario)
            scenario_rows[key]['tests'].add(test_name)
            for suite, entries in suite_entries.items():
                scenario_rows[key]['suites'][suite].extend(entries)

    # Build rows with cell values
    all_rows = []
    for operator in ARITH_ORDER:
        for key in sorted(k for k in scenario_rows if k[0] == operator):
            op, scenario = key
            data = scenario_rows[key]
            label = op
            if scenario != 'basic':
                label = f"{op}（{SCENARIO_LABELS.get(scenario, scenario)}）"

            # Pick a representative expression:
            # prefer the FAIL entry (last record in a failed test), else shortest
            fail_exprs = set()
            all_exprs = set()
            for suite_entries in data['suites'].values():
                for en in suite_entries:
                    expr = en.get('expression', '')
                    if expr:
                        all_exprs.add(expr)
                        if en.get('test_status') == 'FAIL':
                            fail_exprs.add(expr)
            if fail_exprs:
                rep_expr = min(fail_exprs, key=len)
            elif all_exprs:
                rep_expr = min(all_exprs, key=len)
            else:
                rep_expr = ''

            row = {'pair': label, 'expr': rep_expr,
                   'tests': sorted(data['tests'])}
            for suite, col in ARITH_SUITE_COLUMN.items():
                entries = data['suites'].get(suite, [])
                row[col], row[col + '_detail'] = determine_cell(
                    entries, data['tests'], st_map, suite)
            all_rows.append(row)

    # Classify into three groups
    success_rows = []
    fallback_rows = []
    failed_rows = []
    for r in all_rows:
        cols = [r.get(c, '➖') for c in col_names]
        present = [c for c in cols if c != '➖']
        if any(_is_fail(c) for c in present):
            failed_rows.append(r)
        elif present and all('FALLBACK' in c for c in present):
            fallback_rows.append(r)
        else:
            success_rows.append(r)

    # ── Group 1: Fallback ──
    if fallback_rows:
        print()
        print("### 不支持（FALLBACK）")
        print()
        print(f"| 算术操作 | 表达式 | {' | '.join(col_names)} | 边界 |")
        print(f"|---------|--------|{''.join('------|' for _ in col_names)}------|")
        for r in fallback_rows:
            cells = ' | '.join('⚠️' if r.get(c, '➖') != '➖' else '➖'
                               for c in col_names)
            tests = '; '.join(r['tests'][:3])
            print(f"| **{r['pair']}** | `{r['expr']}` | {cells} | {tests} |")

    # ── Group 2: Success ──
    if success_rows:
        print()
        print("### 全部通过")
        print()
        print(f"| 算术操作 | 表达式 | {' | '.join(col_names)} | 边界 |")
        print(f"|---------|--------|{''.join('------|' for _ in col_names)}------|")
        for r in success_rows:
            cells = ' | '.join('✅' if r.get(c, '➖') != '➖' else '➖'
                               for c in col_names)
            tests = '; '.join(r['tests'][:3])
            print(f"| **{r['pair']}** | `{r['expr']}` | {cells} | {tests} |")

    # ── Group 3: Failed ──
    if failed_rows:
        print()
        print("### 失败")
        print()
        print(f"| 算术操作 | 表达式 | {' | '.join(col_names)} | 边界 |")
        print(f"|---------|--------|{''.join('------|' for _ in col_names)}------|")
        for r in failed_rows:
            cells = ' | '.join(r.get(c, '➖') for c in col_names)
            tests = '; '.join(r['tests'][:3])
            print(f"| **{r['pair']}** | `{r['expr']}` | {cells} | {tests} |")

    # Summary
    total = len(all_rows)
    print(f"\nTotal operation rows: {total} "
          f"(✅ {len(success_rows)} + ⚠️ {len(fallback_rows)} fallback + ❌ {len(failed_rows)} failed)")


# ── Mechanism B ────────────────────────────────────────────

def _is_fail(cell):
    return cell.startswith('❌') or cell.startswith('🚫')


def _merge_cells(cells_with_details):
    """Merge multiple (cell, detail) pairs into one using worst-status priority.

    Priority: ❌ > 🚫 > ⚠️ > ✅ > ➖
    """
    def _rank(cell):
        if cell.startswith('❌'):
            return 5
        if cell.startswith('🚫'):
            return 4
        if '⚠️' in cell:
            return 3
        if cell.startswith('✅'):
            return 2
        return 1  # ➖

    best_cell, best_detail, best_rank = '➖', '', 1
    for cell, detail in cells_with_details:
        r = _rank(cell)
        if r > best_rank:
            best_cell, best_detail, best_rank = cell, detail, r
        elif r == best_rank and detail and detail not in best_detail:
            best_detail = '; '.join(filter(None, [best_detail, detail]))
    return best_cell, best_detail


def print_mechanism_b(category, test_results, panorama_entries):
    """Print a mechanism B category section in three groups: FALLBACK → passed → failed."""
    suite_map = MECH_B_SUITE_COLUMN[category]
    st_map = suite_test_map(test_results, set(suite_map))

    # Group by expr_class
    expr_data = defaultdict(lambda: defaultdict(list))
    for e in panorama_entries:
        if e['suite'] not in suite_map:
            continue
        m = META_EXPR_RE.search(e.get('meta', ''))
        if not m:
            continue
        expr_data[m.group(1)][e['suite']].append(e)

    # Build rows with merged cell and test names as 边界
    rows = []
    for expr_class in sorted(expr_data.keys()):
        per_suite = []
        all_tests = set()
        for suite in suite_map:
            entries = expr_data[expr_class].get(suite, [])
            tests = set(en['test_name'] for en in entries)
            all_tests.update(tests)
            cell, detail = determine_cell(entries, tests, st_map, suite)
            per_suite.append((cell, detail))

        merged_cell, merged_detail = _merge_cells(per_suite)
        # Use merged_detail (failed test names) if present, otherwise all test names
        if merged_detail:
            border = merged_detail
        else:
            border = '; '.join(sorted(all_tests)[:3])
        rows.append({'expr': expr_class, 'cell': merged_cell, 'border': border})

    fallback_rows = [r for r in rows if '⚠️' in r['cell']]
    passed_rows = [r for r in rows if r['cell'].startswith('✅')]
    failed_rows = [r for r in rows if _is_fail(r['cell'])]

    # Group 1: FALLBACK
    if fallback_rows:
        print()
        print("### 不支持（FALLBACK）")
        print()
        print("| 表达式 | ANSI(on) | 边界 |")
        print("|--------|------|------|")
        for r in fallback_rows:
            print(f"| **{r['expr']}** | ⚠️ | {r['border']} |")

    # Group 2: Passed
    if passed_rows:
        print()
        print("### 全部通过")
        print()
        print("| 表达式 | ANSI(on) | 边界 |")
        print("|--------|------|------|")
        for r in passed_rows:
            print(f"| **{r['expr']}** | ✅ | {r['border']} |")

    # Group 3: Failed
    if failed_rows:
        print()
        print("### 失败")
        print()
        print("| 表达式 | ANSI(on) | 边界 |")
        print("|--------|------|------|")
        for r in failed_rows:
            print(f"| **{r['expr']}** | {r['cell']} | {r['border']} |")

    print()
    print(f"ANSI(on):  ✅ {len(passed_rows)}  ❌ {len(failed_rows)}  ⚠️ {len(fallback_rows)}")


# ── Test Summary ───────────────────────────────────────────

def print_test_summary(test_results, category, suite_col_map=None):
    """Print per-suite test summary for a category."""
    # Deduplicate: keep worst result per suite+test for cross-version merging
    _result_rank = {'FAILED': 3, 'PASSED': 2, 'IGNORED': 1}
    deduped = {}
    for t in test_results:
        key = (t['suite'], t['test'])
        existing = deduped.get(key)
        if existing is None or _result_rank.get(t['result'], 0) > _result_rank.get(existing['result'], 0):
            deduped[key] = t

    suites = OrderedDict()
    for t in deduped.values():
        s = t['suite']
        if s not in suites:
            suites[s] = {'passed': 0, 'failed': 0, 'ignored': 0, 'causes': defaultdict(int)}
        suites[s][t['result'].lower()] += 1
        if t.get('fail_cause'):
            suites[s]['causes'][t['fail_cause']] += 1

    total_all = sum(s['passed'] + s['failed'] + s['ignored'] for s in suites.values())
    passed_all = sum(s['passed'] for s in suites.values())
    failed_all = sum(s['failed'] for s in suites.values())
    ignored_all = sum(s['ignored'] for s in suites.values())

    title = category.capitalize()
    print(f"\n## {title} ({total_all} tests: {passed_all} passed, "
          f"{failed_all} failed, {ignored_all} ignored)")

    for suite, stats in suites.items():
        col = (suite_col_map or {}).get(suite, suite)
        total = stats['passed'] + stats['failed'] + stats['ignored']
        cause_str = ''
        if stats['causes']:
            cause_str = ' — ' + ', '.join(f"{c}: {n}" for c, n in sorted(stats['causes'].items()))
        print(f"  {col}: {total} tests "
              f"({stats['passed']}P/{stats['failed']}F/{stats['ignored']}I{cause_str})")

    return {'total': total_all, 'passed': passed_all,
            'failed': failed_all, 'ignored': ignored_all}


# ── Summary Table ──────────────────────────────────────────

def print_overall_summary(all_stats):
    """Print overall coverage summary table."""
    print("\n## 覆盖率统计\n")
    print("| Category | Tests | Passed | Failed | Ignored |")
    print("|----------|-------|--------|--------|---------|")
    total_t = total_p = total_f = total_i = 0
    for cat in CATEGORY_ORDER:
        if cat not in all_stats:
            continue
        s = all_stats[cat]
        print(f"| {cat} | {s['total']} | {s['passed']} | {s['failed']} | {s['ignored']} |")
        total_t += s['total']
        total_p += s['passed']
        total_f += s['failed']
        total_i += s['ignored']
    print(f"| **Total** | **{total_t}** | **{total_p}** | **{total_f}** | **{total_i}** |")
    if total_t > 0:
        print(f"\n通过率: {total_p}/{total_t} ({100*total_p/total_t:.0f}%)")


# ── Main ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Unified ANSI PANORAMA log analyzer')
    parser.add_argument('path', help='Log file or directory containing *-ut-*.log files')
    args = parser.parse_args()

    test_results, panorama_entries = load_logs(args.path)
    if not test_results:
        print(f"No tests found in {args.path}", file=sys.stderr)
        sys.exit(1)

    cat_tests, cat_panorama = group_by_category(test_results, panorama_entries)

    all_stats = {}

    for cat in CATEGORY_ORDER:
        if cat not in cat_tests:
            continue

        tr = cat_tests[cat]
        pe = cat_panorama.get(cat, [])

        if cat == 'cast':
            stats = print_test_summary(tr, cat, CAST_SUITE_COLUMN)
            print_cast(tr, pe)
        elif cat == 'arithmetic':
            stats = print_test_summary(tr, cat, ARITH_SUITE_COLUMN)
            print_arithmetic(tr, pe)
        elif cat in MECH_B_SUITE_COLUMN:
            stats = print_test_summary(tr, cat, MECH_B_SUITE_COLUMN[cat])
            print_mechanism_b(cat, tr, pe)
        else:
            # aggregate, errors — no PANORAMA, just test summary
            stats = print_test_summary(tr, cat)

        all_stats[cat] = stats
        print()
        print("---")

    print_overall_summary(all_stats)


if __name__ == '__main__':
    main()
