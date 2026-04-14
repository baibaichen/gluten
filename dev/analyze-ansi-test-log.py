#!/usr/bin/env python3
"""Analyze Gluten ANSI test logs.

Parses test log files produced by verify-ansi-expressions.sh and reports
per-test-method status: which suite it belongs to, whether it had fallback
(detected by 'Validation failed' logs in the test's region), and the test
result (PASSED / FAILED / IGNORED).

Algorithm:
  1. Strip ANSI escape codes from all lines.
  2. Single pass: track current suite, and for each test method, collect all
     lines in its region (from the test's result/offload line to the next
     test's result/offload line).
  3. A test is FALLBACK if any "Validation failed" line appears in its region.
     Otherwise it is OFFLOAD (native Velox execution).
  4. IGNORED tests have no execution region, marked as N/A.

Usage:
    python3 dev/analyze-ansi-test-log.py /tmp/ansi-verify-spark41-ut.log
    python3 dev/analyze-ansi-test-log.py /tmp/ansi-verify-spark41-ut.log --fallback-only
    python3 dev/analyze-ansi-test-log.py /tmp/ansi-verify-spark41-ut.log --suite GlutenCastWithAnsiOnSuite
"""
import argparse
import re
import sys
from collections import OrderedDict

ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

SUITE_RE = re.compile(r'^(Gluten\w+Suite):$')
OFFLOAD_RE = re.compile(r"^'(.+)' offload to gluten$")
FALLBACK_RE = re.compile(r"^'(.+)' NOT use gluten$")
PASSED_RE = re.compile(r'^- (.+?) \(\d+.*\)$')
FAILED_RE = re.compile(r'^- (.+?) \*\*\* FAILED \*\*\* \(\d+.*\)$')
IGNORED_RE = re.compile(r'^- (.+?) !!! IGNORED !!!$')
VALIDATION_FAILED_RE = re.compile(r'Validation failed for plan')
GLUTEN_CHECK_RE = re.compile(r'glutenCheck(?:Expression|ExceptionInExpression)')


def parse_log(path):
    """Parse log and return list of dicts: {suite, test, fallback, velox, result}.

    fallback: True if 'Validation failed' found in test region, False otherwise.
    velox: True if glutenCheckExpression/glutenCheckExceptionInExpression log found,
           False otherwise (e.g. compile-time checks that never go through Velox).
    """
    lines = []
    with open(path, 'r', errors='replace') as f:
        for raw_line in f:
            lines.append(ANSI_RE.sub('', raw_line).strip())

    # Build list of (line_index, suite, test_name, result) for each test,
    # plus track suite boundaries.
    # A "test boundary" is the first line that references a test name:
    #   - offload/fallback line: 'xxx' offload to gluten
    #   - result line: - xxx (duration) or *** FAILED *** or !!! IGNORED !!!
    # We record the earliest line_index for each (suite, test_name).

    tests = []  # list of {suite, test, result, start_line, end_line}
    current_suite = None

    # First, find all test entries with their line positions
    test_entries = []  # (line_idx, suite, test_name, result_or_none)

    for i, line in enumerate(lines):
        if not line:
            continue
        m = SUITE_RE.match(line)
        if m:
            current_suite = m.group(1)
            continue

        # Check offload/fallback lines (no result yet)
        m = OFFLOAD_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), None))
            continue
        m = FALLBACK_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), None))
            continue

        # Check result lines
        m = FAILED_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), 'FAILED'))
            continue
        m = IGNORED_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), 'IGNORED'))
            continue
        m = PASSED_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), 'PASSED'))
            continue

    # Merge entries by (suite, test_name) - a test may have multiple lines
    # (offload line + result line). Take earliest line as start, result from
    # the entry that has one.
    test_map = OrderedDict()  # (suite, name) -> {suite, test, result, start_line}
    for line_idx, suite, name, result in test_entries:
        key = (suite, name)
        if key not in test_map:
            test_map[key] = {
                'suite': suite or '(unknown)',
                'test': name,
                'result': result,
                'start_line': line_idx,
            }
        else:
            entry = test_map[key]
            if result is not None:
                entry['result'] = result
            entry['start_line'] = min(entry['start_line'], line_idx)

    # Convert to list and compute end_line for each test's region
    test_list = list(test_map.values())
    for i, t in enumerate(test_list):
        if i + 1 < len(test_list):
            t['end_line'] = test_list[i + 1]['start_line']
        else:
            t['end_line'] = len(lines)

    # Check for "Validation failed" and "glutenCheck" in each test's region
    results = []
    for t in test_list:
        if t['result'] == 'IGNORED':
            has_fallback = False
            has_velox = False
        else:
            has_fallback = False
            has_velox = False
            for j in range(t['start_line'], t['end_line']):
                if VALIDATION_FAILED_RE.search(lines[j]):
                    has_fallback = True
                if GLUTEN_CHECK_RE.search(lines[j]):
                    has_velox = True

        results.append({
            'suite': t['suite'],
            'test': t['test'],
            'fallback': has_fallback,
            'velox': has_velox,
            'result': t['result'] or 'PASSED',
        })

    return results


def print_table(tests, fallback_only=False, suite_filter=None):
    filtered = tests
    if suite_filter:
        filtered = [t for t in filtered if suite_filter in t['suite']]
    if fallback_only:
        filtered = [t for t in filtered if t['fallback']]

    if not filtered:
        print("No matching tests found.")
        return

    sw = max(len(t['suite']) for t in filtered)
    tw = max(len(t['test']) for t in filtered)
    sw = max(sw, len('Suite'))
    tw = max(tw, len('Test'))

    def velox_str(t):
        if t['result'] == 'IGNORED':
            return 'N/A'
        return 'yes' if t['velox'] else 'no'

    def fallback_str(t):
        if t['result'] == 'IGNORED':
            return 'N/A'
        return 'found' if t['fallback'] else 'not found'

    header = f"| {'Suite':<{sw}} | {'Test':<{tw}} | Velox | Fallback  | Result  |"
    sep = f"|{'-' * (sw + 2)}|{'-' * (tw + 2)}|-------|-----------|---------|"
    print(header)
    print(sep)
    for t in filtered:
        v = velox_str(t)
        fb = fallback_str(t)
        print(f"| {t['suite']:<{sw}} | {t['test']:<{tw}} | {v:<5} | {fb:<9} | {t['result']:<7} |")

    print()
    total = len(filtered)
    velox_yes = sum(1 for t in filtered if t['velox'])
    velox_no = sum(1 for t in filtered if not t['velox'] and t['result'] != 'IGNORED')
    fb_found = sum(1 for t in filtered if t['fallback'])
    passed = sum(1 for t in filtered if t['result'] == 'PASSED')
    failed = sum(1 for t in filtered if t['result'] == 'FAILED')
    ignored = sum(1 for t in filtered if t['result'] == 'IGNORED')
    print(f"Total: {total} | Velox: {velox_yes} | No Velox: {velox_no} | "
          f"Fallback: {fb_found}")
    print(f"Passed: {passed} | Failed: {failed} | Ignored: {ignored}")

    print()
    print("Per-suite summary:")
    suites = OrderedDict()
    for t in filtered:
        s = t['suite']
        if s not in suites:
            suites[s] = {'total': 0, 'velox': 0, 'no_velox': 0, 'fb_found': 0,
                         'passed': 0, 'failed': 0, 'ignored': 0}
        suites[s]['total'] += 1
        if t['velox']:
            suites[s]['velox'] += 1
        elif t['result'] != 'IGNORED':
            suites[s]['no_velox'] += 1
        if t['fallback']:
            suites[s]['fb_found'] += 1
        suites[s][t['result'].lower()] += 1

    sn_w = max(len(s) for s in suites)
    print(f"| {'Suite':<{sn_w}} | Total | Velox | No Velox | Fallback | Passed | Failed | Ignored |")
    print(f"|{'-' * (sn_w + 2)}|-------|-------|----------|----------|--------|--------|---------|")
    for s, c in suites.items():
        print(f"| {s:<{sn_w}} | {c['total']:>5} | {c['velox']:>5} | {c['no_velox']:>8} | {c['fb_found']:>8} | {c['passed']:>6} | {c['failed']:>6} | {c['ignored']:>7} |")


def main():
    parser = argparse.ArgumentParser(description='Analyze Gluten ANSI test logs')
    parser.add_argument('logfile', help='Path to the test log file')
    parser.add_argument('--fallback-only', action='store_true',
                        help='Show only tests that fell back to Spark')
    parser.add_argument('--suite', type=str, default=None,
                        help='Filter by suite name (substring match)')
    args = parser.parse_args()

    tests = parse_log(args.logfile)
    if not tests:
        print(f"No tests found in {args.logfile}")
        sys.exit(1)

    print_table(tests, fallback_only=args.fallback_only, suite_filter=args.suite)


if __name__ == '__main__':
    main()
