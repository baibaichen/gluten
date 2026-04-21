#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Analyze ANSI mode test results from surefire XML and/or test logs.

Supports two input formats:
  --format xml  : Parse surefire XML reports (CI artifacts)
  --format log  : Parse test log files (local verify-ansi-expressions.sh output)

Both formats are merged when available: XML provides authoritative pass/fail/skip,
logs provide expression-level offload/fallback data from EXPRESSION_OFFLOAD_SUMMARY blocks.

Three-color classification:
  🟢 Passed + Offload  — test passes natively in Velox ANSI mode
  🟡 Failed + Offload  — test runs in Velox but fails (needs ANSI fix)
  🔴 Passed + Fallback — test passes but fell back to Spark (Velox can't handle it)

Options:
  --ai           Call GitHub Models API for analysis
  --pr-comment   Post report as PR comment
  --report-dir   Directory containing surefire XML reports (default: ./test-reports/)
  --log-dir      Directory containing test log files
  --log          Single log file to parse

Environment variables:
  GH_TOKEN       — GitHub token for PR comments and GitHub Models API
  AI_MODEL       — Model name for GitHub Models API (default: gpt-4o)
  PR_NUMBER      — PR number to comment on
  TRIGGERED_BY   — GitHub user who triggered the analysis
  RUN_ID         — GitHub Actions run ID
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict, OrderedDict
from pathlib import Path

AI_ENDPOINT = "https://models.inference.ai.azure.com/chat/completions"
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# Log parsing patterns (from analyze-ansi-test-log.py)
SUITE_RE = re.compile(r'^(Gluten\w+Suite):$')
OFFLOAD_RE = re.compile(r"^'(.+)' offload to gluten$")
FALLBACK_RE = re.compile(r"^'(.+)' NOT use gluten$")
PASSED_RE = re.compile(r'^- (.+?)(?:\s+\(\d+.*\))?$')
FAILED_RE = re.compile(r'^- (.+?) \*\*\* FAILED \*\*\*(?:\s+\(\d+.*\))?$')
IGNORED_RE = re.compile(r'^- (.+?) !!! IGNORED !!!$')
VALIDATION_FAILED_RE = re.compile(r'Validation failed')
GLUTEN_CHECK_RE = re.compile(r'glutenCheck(?:Expression|ExceptionInExpression)')

NO_EXCEPTION_RE = re.compile(
    r'Expected exception .+ to be thrown, but no exception was thrown')
MSG_MISMATCH_RE = re.compile(
    r"Expected error message containing '(.+?)' but got '(.+?)'")
WRONG_EXCEPTION_RE = re.compile(
    r'Expected (\S+) but got (\S+):')

# EXPRESSION_OFFLOAD_SUMMARY markers
OFFLOAD_SUMMARY_BEGIN = 'EXPRESSION_OFFLOAD_SUMMARY_BEGIN'
OFFLOAD_SUMMARY_END = 'EXPRESSION_OFFLOAD_SUMMARY_END'


# ---------------------------------------------------------------------------
# Surefire XML parsing
# ---------------------------------------------------------------------------

def parse_surefire_reports(base_dir):
    """Parse all surefire XML reports and return structured test results."""
    results = []
    xml_files = glob.glob(f"{base_dir}/**/TEST-*.xml", recursive=True)
    if not xml_files:
        return results

    for xml_path in xml_files:
        try:
            tree = ET.parse(xml_path)
        except ET.ParseError:
            continue
        suite = tree.getroot()
        suite_name = suite.get("name", Path(xml_path).stem)
        job_name = _infer_job_name(xml_path)

        for tc in suite.findall("testcase"):
            test_name = tc.get("name", "unknown")
            classname = tc.get("classname", suite_name)
            time_s = float(tc.get("time", "0"))

            failure = tc.find("failure")
            error = tc.find("error")
            skipped = tc.find("skipped")

            if skipped is not None:
                status = "SKIPPED"
                message = skipped.get("message", "")
            elif failure is not None:
                status = "FAILED"
                message = (failure.get("message") or failure.text or "")[:500]
            elif error is not None:
                status = "ERROR"
                message = (error.get("message") or error.text or "")[:500]
            else:
                status = "PASSED"
                message = ""

            results.append({
                "suite": classname,
                "test": test_name,
                "status": status,
                "message": message,
                "time": time_s,
                "job": job_name,
            })
    return results


def _infer_job_name(xml_path):
    parts = Path(xml_path).parts
    for p in parts:
        if "spark-test-" in p:
            return p
    return "unknown"


def _short_job_name(job):
    """Shorten job name for table display."""
    job = job.replace("spark-test-", "").replace("-report", "")
    return job or "unknown"


# ---------------------------------------------------------------------------
# Log file parsing
# ---------------------------------------------------------------------------

def parse_log(path):
    """Parse test log and return list of test-level results."""
    lines = []
    with open(path, 'r', errors='replace') as f:
        for raw_line in f:
            lines.append(ANSI_RE.sub('', raw_line).strip())

    tests = []
    current_suite = None
    test_entries = []

    for i, line in enumerate(lines):
        if not line:
            continue
        m = SUITE_RE.match(line)
        if m:
            current_suite = m.group(1)
            continue
        m = OFFLOAD_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), None))
            continue
        m = FALLBACK_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), None))
            continue
        m = FAILED_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), 'FAILED'))
            continue
        m = IGNORED_RE.match(line)
        if m:
            test_entries.append((i, current_suite, m.group(1), 'IGNORED'))
            continue
        m = PASSED_RE.match(line)
        if m and not VALIDATION_FAILED_RE.search(line):
            test_entries.append((i, current_suite, m.group(1), 'PASSED'))
            continue

    test_map = OrderedDict()
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

    test_list = list(test_map.values())
    for i, t in enumerate(test_list):
        if i + 1 < len(test_list):
            t['end_line'] = test_list[i + 1]['start_line']
        else:
            t['end_line'] = len(lines)

    results = []
    for t in test_list:
        if t['result'] == 'IGNORED':
            has_fallback = False
            has_velox = False
            fail_cause = None
        else:
            has_fallback = False
            has_velox = False
            fail_cause = None
            for j in range(t['start_line'], t['end_line']):
                if VALIDATION_FAILED_RE.search(lines[j]):
                    has_fallback = True
                if GLUTEN_CHECK_RE.search(lines[j]):
                    has_velox = True
                if t['result'] == 'FAILED' and fail_cause is None:
                    if NO_EXCEPTION_RE.search(lines[j]):
                        fail_cause = 'NO_EXCEPTION'
                    elif MSG_MISMATCH_RE.search(lines[j]):
                        fail_cause = 'MSG_MISMATCH'
                    elif WRONG_EXCEPTION_RE.search(lines[j]):
                        fail_cause = 'WRONG_EXCEPTION'
            if t['result'] == 'FAILED' and fail_cause is None:
                fail_cause = 'OTHER'

        results.append({
            'suite': t['suite'],
            'test': t['test'],
            'fallback': has_fallback,
            'velox': has_velox,
            'result': t['result'] or 'PASSED',
            'fail_cause': fail_cause,
        })

    return results


# ---------------------------------------------------------------------------
# EXPRESSION_OFFLOAD_SUMMARY block parsing
# ---------------------------------------------------------------------------

def parse_offload_summary_from_logs(log_paths):
    """Parse EXPRESSION_OFFLOAD_SUMMARY blocks from log files.

    Returns dict: {(suite, test_name): [OffloadRecord...]}
    where each record is {method, expression, meta, offload, status}.
    """
    all_records = {}

    for path in log_paths:
        in_block = False
        current_suite = None
        current_test = None

        with open(path, 'r', errors='replace') as f:
            for raw_line in f:
                line = ANSI_RE.sub('', raw_line).strip()
                # Strip log4j prefix if present
                if '] ' in line:
                    line = line[line.rindex('] ') + 2:]
                # Strip SLF4J/logback prefix: "HH:MM:SS.mmm WARN classname: content"
                m = re.match(
                    r'\d{2}:\d{2}:\d{2}\.\d+ \w+ [\w.$]+:\s*(.*)', line)
                if m:
                    line = m.group(1)

                if OFFLOAD_SUMMARY_BEGIN in line:
                    in_block = True
                    continue
                if OFFLOAD_SUMMARY_END in line:
                    in_block = False
                    current_suite = None
                    current_test = None
                    continue
                if not in_block:
                    continue

                stripped = line.strip()
                if stripped.endswith(':') and not stripped.startswith(('N|', 'E|')):
                    if '|' not in stripped:
                        name = stripped[:-1].strip()
                        if current_suite is None:
                            current_suite = name
                        else:
                            current_test = name
                    continue

                if '|' in stripped and stripped[0] in ('N', 'E'):
                    parts = stripped.split('|')
                    if len(parts) >= 5:
                        record = {
                            'method': 'checkException' if parts[0] == 'E' else 'checkExpression',
                            'expression': parts[1],
                            'meta': parts[2],
                            'offload': parts[3],
                            'status': parts[4],
                        }
                        key = (current_suite, current_test)
                        all_records.setdefault(key, []).append(record)

    return all_records


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_test(status, has_fallback, has_offload_data):
    """Assign three-color classification."""
    if status == "SKIPPED":
        return "⚪", "Skipped"
    if has_fallback:
        if status == "PASSED":
            return "🔴", "Passed+Fallback"
        return "🔴", "Failed+Fallback"
    if not has_offload_data:
        if status == "PASSED":
            return "⚪", "Passed (no data)"
        return "🟡", "Failed (no data)"
    if status == "PASSED":
        return "🟢", "Passed+Offload"
    if status in ("FAILED", "ERROR"):
        return "🟡", "Failed+Offload"
    return "🟡", "Failed"


def determine_fallback(test_key, offload_records):
    """Determine fallback status and whether offload data exists."""
    records = offload_records.get(test_key, [])
    if not records:
        return False, False
    return any(r['offload'] == 'FALLBACK' for r in records), True


def build_summary(results, offload_records=None):
    """Build summary with three-color classification."""
    offload_records = offload_records or {}
    summary = {
        "total": len(results),
        "by_color": defaultdict(int),
        "by_job": defaultdict(lambda: defaultdict(int)),
        "failures": [],
    }
    for t in results:
        suite = t.get("suite", "")
        test = t.get("test", "")
        status = t.get("status", "PASSED")

        has_fallback = t.get("fallback", False)
        has_offload_data = False
        if offload_records:
            test_key = (suite.split('.')[-1], test)
            fb, has_data = determine_fallback(test_key, offload_records)
            has_fallback = has_fallback or fb
            has_offload_data = has_data

        color, label = classify_test(status, has_fallback, has_offload_data)
        t["color"] = color
        t["label"] = label
        t["fallback"] = has_fallback
        summary["by_color"][label] += 1
        job = t.get("job", "unknown")
        summary["by_job"][job][label] += 1
        if status in ("FAILED", "ERROR"):
            summary["failures"].append(t)
    return summary


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def format_markdown_report(summary, results, ai_analysis=None):
    """Generate markdown report."""
    lines = ["## ANSI Mode Test Analysis Report\n"]

    lines.append("### Overview\n")
    lines.append(f"Total tests: **{summary['total']}**\n")
    lines.append("| Classification | Count |")
    lines.append("|---|---|")
    for label in ["Passed+Offload", "Failed+Offload", "Passed+Fallback",
                  "Failed+Fallback", "Failed", "Passed (no data)",
                  "Failed (no data)", "Skipped"]:
        count = summary["by_color"].get(label, 0)
        if count > 0:
            color = {"Passed+Offload": "🟢", "Failed+Offload": "🟡",
                     "Passed+Fallback": "🔴", "Failed+Fallback": "🔴",
                     "Failed": "🟡", "Passed (no data)": "⚪",
                     "Failed (no data)": "🟡", "Skipped": "⚪"}.get(label, "")
            lines.append(f"| {color} {label} | {count} |")
    lines.append("")

    lines.append("### Per-Job Breakdown\n")
    for job, counts in sorted(summary["by_job"].items()):
        lines.append(f"**{job}**")
        for label, count in sorted(counts.items()):
            lines.append(f"- {label}: {count}")
        lines.append("")

    if summary["failures"]:
        lines.append("### Failed Tests (sample, max 50)\n")
        lines.append("| Suite | Test | Job | Color | Message |")
        lines.append("|---|---|---|---|---|")
        for t in summary["failures"][:50]:
            msg = t.get("message", "")[:200].replace("|", "\\|").replace("\n", " ")
            job = _short_job_name(t.get("job", ""))
            lines.append(
                f"| {t['suite'].split('.')[-1]} | {t['test']} "
                f"| {job} | {t['color']} {t['label']} | {msg} |")
        if len(summary["failures"]) > 50:
            lines.append(f"\n*...and {len(summary['failures']) - 50} more failures*\n")
        lines.append("")

    if ai_analysis:
        lines.append("### AI Analysis\n")
        lines.append(ai_analysis)
        lines.append("")

    triggered_by = os.environ.get("TRIGGERED_BY", "unknown")
    run_id = os.environ.get("RUN_ID", "")
    lines.append(f"\n---\n*Triggered by @{triggered_by} | "
                 f"[Workflow run](../actions/runs/{run_id})*")
    return "\n".join(lines)


def format_log_table(tests, fallback_only=False, failed_only=False, suite_filter=None):
    """Print table for log-based analysis (local usage)."""
    filtered = tests
    if suite_filter:
        filtered = [t for t in filtered if suite_filter in t['suite']]
    if fallback_only:
        filtered = [t for t in filtered if t['fallback']]
    if failed_only:
        filtered = [t for t in filtered if t['result'] == 'FAILED']

    if not filtered:
        print("No matching tests found.")
        return

    sw = max(max(len(t['suite']) for t in filtered), len('Suite'))
    tw = max(max(len(t['test']) for t in filtered), len('Test'))

    def velox_str(t):
        if t['result'] == 'IGNORED':
            return 'N/A'
        return 'yes' if t['velox'] else 'no'

    def fallback_str(t):
        if t['result'] == 'IGNORED':
            return 'N/A'
        return 'found' if t['fallback'] else 'not found'

    def cause_str(t):
        return t.get('fail_cause') or ''

    has_failures = any(t['result'] == 'FAILED' for t in filtered)
    if has_failures:
        cw = max(max((len(cause_str(t)) for t in filtered), default=5), len('Cause'))
        header = (f"| {'Suite':<{sw}} | {'Test':<{tw}} | Velox | Fallback  "
                  f"| Result  | {'Cause':<{cw}} |")
        sep = (f"|{'-' * (sw + 2)}|{'-' * (tw + 2)}|-------|--------"
               f"---|---------|{'-' * (cw + 2)}|")
    else:
        header = f"| {'Suite':<{sw}} | {'Test':<{tw}} | Velox | Fallback  | Result  |"
        sep = f"|{'-' * (sw + 2)}|{'-' * (tw + 2)}|-------|-----------|---------|"
    print(header)
    print(sep)
    for t in filtered:
        v = velox_str(t)
        fb = fallback_str(t)
        if has_failures:
            c = cause_str(t)
            print(f"| {t['suite']:<{sw}} | {t['test']:<{tw}} | {v:<5} | {fb:<9} "
                  f"| {t['result']:<7} | {c:<{cw}} |")
        else:
            print(f"| {t['suite']:<{sw}} | {t['test']:<{tw}} | {v:<5} | {fb:<9} "
                  f"| {t['result']:<7} |")

    print()
    total = len(filtered)
    passed = sum(1 for t in filtered if t['result'] == 'PASSED')
    failed = sum(1 for t in filtered if t['result'] == 'FAILED')
    ignored = sum(1 for t in filtered if t['result'] == 'IGNORED')
    velox_yes = sum(1 for t in filtered if t.get('velox'))
    velox_no = sum(1 for t in filtered if not t.get('velox') and t['result'] != 'IGNORED')
    fb_found = sum(1 for t in filtered if t['fallback'])
    print(f"Total: {total} | Velox: {velox_yes} | No Velox: {velox_no} | "
          f"Fallback: {fb_found}")
    print(f"Passed: {passed} | Failed: {failed} | Ignored: {ignored}")

    if failed > 0:
        causes = {}
        for t in filtered:
            c = t.get('fail_cause')
            if c:
                causes[c] = causes.get(c, 0) + 1
        cause_parts = [f"{c}: {n}" for c, n in sorted(causes.items())]
        print(f"Failure causes: {' | '.join(cause_parts)}")

    print()
    print("Per-suite summary:")
    suites = OrderedDict()
    for t in filtered:
        s = t['suite']
        if s not in suites:
            suites[s] = {'total': 0, 'velox': 0, 'no_velox': 0, 'fb_found': 0,
                         'passed': 0, 'failed': 0, 'ignored': 0}
        suites[s]['total'] += 1
        if t.get('velox'):
            suites[s]['velox'] += 1
        elif t['result'] != 'IGNORED':
            suites[s]['no_velox'] += 1
        if t['fallback']:
            suites[s]['fb_found'] += 1
        suites[s][t['result'].lower()] += 1

    sn_w = max(len(s) for s in suites)
    print(f"| {'Suite':<{sn_w}} | Total | Velox | No Velox | Fallback "
          f"| Passed | Failed | Ignored |")
    print(f"|{'-' * (sn_w + 2)}|-------|-------|----------|----------|"
          f"--------|--------|---------|")
    for s, c in suites.items():
        print(f"| {s:<{sn_w}} | {c['total']:>5} | {c['velox']:>5} | "
              f"{c['no_velox']:>8} | {c['fb_found']:>8} | {c['passed']:>6} | "
              f"{c['failed']:>6} | {c['ignored']:>7} |")


# ---------------------------------------------------------------------------
# AI analysis
# ---------------------------------------------------------------------------

def call_github_models_api(summary, failures_sample):
    """Call GitHub Models API for AI analysis."""
    token = os.environ.get("GH_TOKEN")
    model = os.environ.get("AI_MODEL", "gpt-4o")
    if not token:
        return None

    failure_text = "\n".join(
        f"- {t['suite'].split('.')[-1]}.{t['test']}: {t['color']} {t['label']} — "
        f"{t.get('message', '')[:200]}"
        for t in failures_sample[:30]
    )

    prompt = f"""Analyze these ANSI SQL mode test results from Apache Gluten (Velox backend).

Summary:
- Total tests: {summary['total']}
- 🟢 Passed+Offload (native Velox ANSI): {summary['by_color'].get('Passed+Offload', 0)}
- 🟡 Failed+Offload (Velox ran but failed): {summary['by_color'].get('Failed+Offload', 0)}
- 🔴 Passed+Fallback (fell back to Spark): {summary['by_color'].get('Passed+Fallback', 0)}

Sample failures:
{failure_text}

Provide a brief analysis (under 500 words):
1. Key failure patterns and root causes
2. Which expression types need ANSI implementation work
3. Priority recommendations for fixing"""

    try:
        import requests
        resp = requests.post(
            AI_ENDPOINT,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1000,
            },
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        else:
            return (f"*AI analysis unavailable (HTTP {resp.status_code}). "
                    f"The commenter may not have GitHub Models API access.*")
    except Exception as e:
        return f"*AI analysis unavailable: {e}*"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def post_pr_comment(markdown):
    pr_number = os.environ.get("PR_NUMBER")
    if not pr_number:
        print("PR_NUMBER not set, skipping PR comment")
        return
    try:
        subprocess.run(
            ["gh", "pr", "comment", pr_number, "--body", markdown],
            check=True, timeout=30,
        )
        print(f"Posted comment to PR #{pr_number}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as e:
        print(f"Failed to post PR comment: {e}")


def write_job_summary(markdown):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(markdown)
        print("Wrote Job Summary")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Gluten ANSI mode test results')
    parser.add_argument('--format', choices=['xml', 'log'], default='xml',
                        help='Input format: xml (surefire reports) or log (test logs)')
    parser.add_argument('--report-dir', default='./test-reports/',
                        help='Directory with surefire XML reports')
    parser.add_argument('--log-dir', default=None,
                        help='Directory with test log files')
    parser.add_argument('--log', default=None,
                        help='Single log file to parse')
    parser.add_argument('--ai', action='store_true',
                        help='Call GitHub Models API for analysis')
    parser.add_argument('--pr-comment', action='store_true',
                        help='Post report as PR comment')
    parser.add_argument('--fallback-only', action='store_true',
                        help='Show only tests that fell back to Spark (log mode)')
    parser.add_argument('--failed-only', action='store_true',
                        help='Show only failed tests (log mode)')
    parser.add_argument('--suite', type=str, default=None,
                        help='Filter by suite name (log mode)')
    args = parser.parse_args()

    offload_records = {}

    if args.format == 'log':
        # Log-based analysis (local mode)
        log_path = args.log
        if not log_path:
            print("--log is required for --format log")
            sys.exit(1)
        tests = parse_log(log_path)
        if not tests:
            print(f"No tests found in {log_path}")
            sys.exit(1)

        offload_records = parse_offload_summary_from_logs([log_path])

        for t in tests:
            key = (t['suite'], t['test'])
            records = offload_records.get(key, [])
            if records:
                t['fallback'] = any(r['offload'] == 'FALLBACK' for r in records)
                t['velox'] = any(r['offload'] == 'OFFLOAD' for r in records)

        format_log_table(tests, fallback_only=args.fallback_only,
                         failed_only=args.failed_only, suite_filter=args.suite)
        return

    # XML-based analysis (CI mode)
    results = parse_surefire_reports(args.report_dir)

    # Parse offload summary from log files if available
    log_paths = []
    if args.log_dir:
        log_paths = glob.glob(f"{args.log_dir}/**/*.log", recursive=True)
        log_paths += glob.glob(f"{args.log_dir}/**/*.txt", recursive=True)
    if args.log:
        log_paths.append(args.log)
    if log_paths:
        offload_records = parse_offload_summary_from_logs(log_paths)

    if not results:
        msg = ("## ANSI Mode Test Analysis\n\nNo test reports found. "
               "Build may have failed — check individual job logs.")
        if args.pr_comment:
            post_pr_comment(msg)
        write_job_summary(msg)
        print("No surefire reports found")
        sys.exit(0)

    summary = build_summary(results, offload_records)

    ai_analysis = None
    if args.ai:
        ai_analysis = call_github_models_api(summary, summary["failures"])

    report = format_markdown_report(summary, results, ai_analysis)

    if args.pr_comment:
        post_pr_comment(report)
    else:
        print(report)
    write_job_summary(report)

    print(f"Analysis complete: {summary['total']} tests, "
          f"{len(summary['failures'])} failures")
    if offload_records:
        print(f"Offload records parsed from {len(log_paths)} log file(s), "
              f"{len(offload_records)} test entries")


if __name__ == "__main__":
    main()
