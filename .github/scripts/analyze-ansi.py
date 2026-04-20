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
"""Unified ANSI mode test analyzer.

Data sources:
  --json-dir     JSON files from GlutenExpressionOffloadTracker (expression-level)
  --report-dir   Surefire XML reports (test-method-level, for backends-velox)

Output modes:
  --mode summary   Test-level three-color classification + failure list
  --mode matrix    Expression-level matrix by category
  --mode full      summary + matrix (matrix in <details> fold)
  --mode json      Structured JSON output for tool consumption

Output targets:
  stdout (default), --pr-comment, --job-summary, --output-file FILE
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# ---------------------------------------------------------------------------
# Fail cause classification patterns
# ---------------------------------------------------------------------------
NO_EXCEPTION_RE = re.compile(
    r"Expected .+ to be thrown, but no exception was thrown")
WRONG_EXCEPTION_RE = re.compile(r"Expected \S+ but got \S+:")
MSG_MISMATCH_RE = re.compile(r"Expected error message containing")


def classify_fail_cause(message):
    if not message:
        return "OTHER"
    if NO_EXCEPTION_RE.search(message):
        return "NO_EXCEPTION"
    if WRONG_EXCEPTION_RE.search(message):
        return "WRONG_EXCEPTION"
    if MSG_MISMATCH_RE.search(message):
        return "MSG_MISMATCH"
    return "OTHER"


# ===========================================================================
# DATA LAYER
# ===========================================================================

def load_json_data(json_dir):
    """Load all JSON files from Tracker output directory.

    Returns list of suite dicts: {suite, category, tests: [{name, status, records}]}
    """
    suites = []
    if not json_dir or not os.path.isdir(json_dir):
        return suites
    for path in sorted(glob.glob(os.path.join(json_dir, "*.json"))):
        with open(path) as f:
            try:
                data = json.load(f)
                suites.append(data)
            except json.JSONDecodeError:
                print(f"Warning: could not parse {path}", file=sys.stderr)
    return suites


def load_surefire_xml(report_dir):
    """Load surefire XML reports for backends-velox test results.

    Returns list of test dicts: {suite, test, status, message, job}
    """
    results = []
    if not report_dir or not os.path.isdir(report_dir):
        return results
    for xml_path in sorted(glob.glob(os.path.join(report_dir, "**/*.xml"),
                                     recursive=True)):
        try:
            tree = ET.parse(xml_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        suite_name = root.get("name", "")
        job = _infer_job_name(xml_path)
        for tc in root.iter("testcase"):
            test_name = tc.get("name", "")
            failure = tc.find("failure")
            error = tc.find("error")
            skipped = tc.find("skipped")
            if skipped is not None:
                status = "SKIPPED"
                msg = ""
            elif failure is not None:
                status = "FAILED"
                msg = failure.get("message", "")
            elif error is not None:
                status = "ERROR"
                msg = error.get("message", "")
            else:
                status = "PASSED"
                msg = ""
            results.append({
                "suite": suite_name,
                "test": test_name,
                "status": status,
                "message": msg,
                "job": job,
            })
    return results


def _infer_job_name(xml_path):
    parts = xml_path.replace("\\", "/").split("/")
    for p in parts:
        if "spark" in p and ("backend" in p or "ut" in p):
            return p
    return "unknown"


# ===========================================================================
# ANALYSIS LAYER
# ===========================================================================

def classify_test(status, has_fallback, has_offload_data):
    """Unified three-color classification."""
    is_pass = status in ("PASSED", "PASS")
    is_skip = status in ("SKIPPED", "SKIP")
    if is_skip:
        return "⚪", "Skipped"
    if has_fallback:
        if is_pass:
            return "🔴", "Passed+Fallback"
        return "🔴", "Failed+Fallback"
    if not has_offload_data:
        if is_pass:
            return "⚪", "Passed (no data)"
        return "🟡", "Failed (no data)"
    if is_pass:
        return "🟢", "Passed+Offload"
    if status in ("FAILED", "ERROR", "FAIL"):
        return "🟡", "Failed+Offload"
    return "🟡", "Failed"


def analyze_json_tests(suites):
    """Analyze JSON data at test level. Returns summary dict + flat test list."""
    tests = []
    for suite_data in suites:
        suite_name = suite_data.get("suite", "")
        category = suite_data.get("category", "")
        for t in suite_data.get("tests", []):
            records = t.get("records", [])
            has_fallback = any(r.get("offload") == "FALLBACK" for r in records)
            has_offload_data = len(records) > 0
            color, label = classify_test(
                t.get("status", "PASSED"), has_fallback, has_offload_data)
            tests.append({
                "suite": suite_name,
                "test": t["name"],
                "status": t.get("status", "PASSED"),
                "color": color,
                "label": label,
                "category": category,
                "has_fallback": has_fallback,
                "records": records,
            })
    return tests


def analyze_xml_tests(xml_results):
    """Analyze surefire XML at test method level."""
    tests = []
    for t in xml_results:
        color, label = classify_test(t["status"], False, False)
        tests.append({
            "suite": t["suite"],
            "test": t["test"],
            "status": t["status"],
            "color": color,
            "label": label,
            "job": t.get("job", ""),
            "message": t.get("message", ""),
            "source": "xml",
        })
    return tests


def build_summary(json_tests, xml_tests):
    """Build unified summary from both data sources."""
    by_color = defaultdict(int)
    failures = []
    total = 0

    for t in json_tests:
        total += 1
        by_color[t["label"]] += 1
        if t["status"] in ("FAILED", "ERROR", "FAIL"):
            fail_cause = ""
            for r in t.get("records", []):
                if r.get("failCause"):
                    fail_cause = r["failCause"]
                    break
            failures.append({
                "suite": t["suite"],
                "test": t["test"],
                "color": t["color"],
                "label": t["label"],
                "message": fail_cause,
                "source": "json",
            })

    for t in xml_tests:
        total += 1
        by_color[t["label"]] += 1
        if t["status"] in ("FAILED", "ERROR"):
            failures.append({
                "suite": t["suite"],
                "test": t["test"],
                "color": t["color"],
                "label": t["label"],
                "message": t.get("message", ""),
                "job": t.get("job", ""),
                "source": "xml",
            })

    return {
        "total": total,
        "by_color": dict(by_color),
        "failures": failures,
        "json_test_count": len(json_tests),
        "xml_test_count": len(xml_tests),
    }


# ---------------------------------------------------------------------------
# Expression-level matrix analysis
# ---------------------------------------------------------------------------

def _normalize_type(t):
    """Normalize Spark type names for display."""
    simple = {
        "boolean": "Boolean", "byte": "TinyInt", "short": "SmallInt",
        "int": "Int", "bigint": "BigInt", "float": "Float",
        "double": "Double", "string": "String", "binary": "Binary",
        "date": "Date", "timestamp": "Timestamp", "timestamp_ntz": "TimestampNTZ",
        "void": "Null",
    }
    low = t.lower().strip()
    if low in simple:
        return simple[low]
    if low.startswith("decimal"):
        return t.replace("decimal", "Decimal")
    if low.startswith("array"):
        return "Array\\<" + _normalize_type(t[6:-1]) + "\\>"
    if low.startswith("map"):
        inner = t[4:-1]
        parts = inner.split(",", 1)
        if len(parts) == 2:
            return "Map\\<" + _normalize_type(parts[0]) + "," + _normalize_type(parts[1]) + "\\>"
    return t


def _cell_symbol(offload, status, fail_cause_type):
    """Determine cell symbol using unified three-color scheme."""
    if offload == "FALLBACK":
        return "🔴"
    if status == "PASS":
        return "🟢"
    return "🟡"


def analyze_expression_matrix(suites):
    """Build expression-level matrices grouped by category.

    Returns dict: {category: {info, entries, matrix_data}}
    """
    categories = defaultdict(lambda: {"entries": [], "suites": set(),
                                       "tests_pass": 0, "tests_fail": 0,
                                       "tests_skip": 0})

    for suite_data in suites:
        cat = suite_data.get("category", "unknown")
        suite_name = suite_data.get("suite", "")
        cat_data = categories[cat]
        cat_data["suites"].add(suite_name)

        for t in suite_data.get("tests", []):
            if t.get("status") == "PASS":
                cat_data["tests_pass"] += 1
            elif t.get("status") == "FAIL":
                cat_data["tests_fail"] += 1
            else:
                cat_data["tests_skip"] += 1

            for rec in t.get("records", []):
                meta = rec.get("meta", {})
                fail_cause = rec.get("failCause")
                fail_type = classify_fail_cause(fail_cause) if fail_cause else None
                cat_data["entries"].append({
                    "suite": suite_name,
                    "test": t["name"],
                    "method": rec.get("method", "N"),
                    "expression": rec.get("expression", ""),
                    "meta": meta,
                    "offload": rec.get("offload", ""),
                    "status": rec.get("status", "PASS"),
                    "fail_cause": fail_cause,
                    "fail_type": fail_type,
                })

    return dict(categories)


def _build_cast_matrix(entries, suites):
    """Build cast matrix: (fromType, toType) -> {suite: symbol}."""
    matrix = defaultdict(lambda: defaultdict(lambda: "⚪"))
    for e in entries:
        from_t = _normalize_type(e["meta"].get("fromType", "?"))
        to_t = _normalize_type(e["meta"].get("toType", "?"))
        key = (from_t, to_t)
        sym = _cell_symbol(e["offload"], e["status"], e.get("fail_type"))
        cur = matrix[key].get(e["suite"], "⚪")
        matrix[key][e["suite"]] = _worse(sym, cur)
    return matrix


def _build_arithmetic_matrix(entries, suites):
    """Build arithmetic matrix: operator -> {suite: symbol}."""
    matrix = defaultdict(lambda: defaultdict(lambda: "⚪"))
    for e in entries:
        op = e["meta"].get("operator", "?")
        key = op
        sym = _cell_symbol(e["offload"], e["status"], e.get("fail_type"))
        cur = matrix[key].get(e["suite"], "⚪")
        matrix[key][e["suite"]] = _worse(sym, cur)
    return matrix


def _build_mechanism_b_matrix(entries, suites):
    """Build mechanism B matrix: expr_class -> {suite: symbol}."""
    matrix = defaultdict(lambda: defaultdict(lambda: "⚪"))
    for e in entries:
        expr = e["meta"].get("expr", "?")
        key = expr
        sym = _cell_symbol(e["offload"], e["status"], e.get("fail_type"))
        cur = matrix[key].get(e["suite"], "⚪")
        matrix[key][e["suite"]] = _worse(sym, cur)
    return matrix


_PRIORITY = {"🟡": 3, "🔴": 2, "🟢": 1, "⚪": 0}


def _worse(a, b):
    return a if _PRIORITY.get(a, 0) >= _PRIORITY.get(b, 0) else b


# ===========================================================================
# OUTPUT LAYER
# ===========================================================================

def format_summary(summary, json_tests):
    """Format test-level summary as markdown."""
    lines = ["## ANSI Mode Test Analysis Report\n"]
    lines.append(f"**Total tests: {summary['total']}** "
                 f"(JSON: {summary['json_test_count']}, "
                 f"XML: {summary['xml_test_count']})\n")

    lines.append("### Overview\n")
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

    if summary["failures"]:
        lines.append("### Failed Tests (sample, max 50)\n")
        lines.append("| Suite | Test | Source | Color | Message |")
        lines.append("|---|---|---|---|---|")
        for t in summary["failures"][:50]:
            msg = (t.get("message") or "")[:200].replace("|", "\\|").replace("\n", " ")
            source = t.get("source", "")
            suite_short = t["suite"].split(".")[-1]
            lines.append(
                f"| {suite_short} | {t['test']} "
                f"| {source} | {t['color']} {t['label']} | {msg} |")
        if len(summary["failures"]) > 50:
            lines.append(
                f"\n*...and {len(summary['failures']) - 50} more failures*\n")
        lines.append("")

    return "\n".join(lines)


def format_matrix(categories):
    """Format expression-level matrix as markdown."""
    lines = ["## Expression Offload Matrix\n"]

    cat_order = ["cast", "arithmetic", "collection", "datetime",
                 "math", "decimal", "string", "aggregate", "errors"]

    for cat in cat_order:
        if cat not in categories:
            continue
        data = categories[cat]
        total = data["tests_pass"] + data["tests_fail"] + data["tests_skip"]
        lines.append(
            f"### {cat.title()} "
            f"({total} tests: {data['tests_pass']} passed, "
            f"{data['tests_fail']} failed, {data['tests_skip']} skipped)\n")

        suites = sorted(data["suites"])
        entries = data["entries"]

        if not entries:
            lines.append("*No expression-level data for this category.*\n")
            continue

        if cat == "cast":
            matrix = _build_cast_matrix(entries, suites)
            _format_cast_tables(lines, matrix, suites)
        elif cat == "arithmetic":
            matrix = _build_arithmetic_matrix(entries, suites)
            _format_generic_table(lines, matrix, suites, "Operator")
        else:
            matrix = _build_mechanism_b_matrix(entries, suites)
            _format_generic_table(lines, matrix, suites, "Expression")

        lines.append("")

    # Coverage table
    lines.append("### Coverage Summary\n")
    lines.append("| Category | Tests | Passed | Failed | Skipped |")
    lines.append("|---|---|---|---|---|")
    t_total = t_pass = t_fail = t_skip = 0
    for cat in cat_order:
        if cat not in categories:
            continue
        d = categories[cat]
        total = d["tests_pass"] + d["tests_fail"] + d["tests_skip"]
        lines.append(
            f"| {cat} | {total} | {d['tests_pass']} | "
            f"{d['tests_fail']} | {d['tests_skip']} |")
        t_total += total
        t_pass += d["tests_pass"]
        t_fail += d["tests_fail"]
        t_skip += d["tests_skip"]
    lines.append(
        f"| **Total** | **{t_total}** | **{t_pass}** | "
        f"**{t_fail}** | **{t_skip}** |")
    if t_total > 0:
        pct = t_pass * 100 / t_total
        lines.append(f"\nPass rate: {t_pass}/{t_total} ({pct:.1f}%)\n")

    return "\n".join(lines)


def _format_cast_tables(lines, matrix, suites):
    """Format cast matrix split into fallback/pass/fail groups."""
    fallback_rows = []
    pass_rows = []
    fail_rows = []

    for (from_t, to_t), suite_map in sorted(matrix.items()):
        merged = "⚪"
        for s in suites:
            merged = _worse(merged, suite_map.get(s, "⚪"))

        if merged == "🔴":
            fallback_rows.append((from_t, to_t, suite_map))
        elif merged == "🟢":
            pass_rows.append((from_t, to_t, suite_map))
        else:
            fail_rows.append((from_t, to_t, suite_map))

    suite_headers = " | ".join(s.replace("Gluten", "") for s in suites)

    if fallback_rows:
        lines.append(f"**🔴 Fallback ({len(fallback_rows)} type pairs)**\n")
        lines.append(f"| From | To | {suite_headers} |")
        lines.append("|---|---| " + " | ".join(["---"] * len(suites)) + " |")
        for from_t, to_t, sm in fallback_rows:
            cells = " | ".join(sm.get(s, "⚪") for s in suites)
            lines.append(f"| {from_t} | {to_t} | {cells} |")
        lines.append("")

    if pass_rows:
        lines.append(f"**🟢 Passed ({len(pass_rows)} type pairs)**\n")
        by_from = defaultdict(list)
        for from_t, to_t, _ in pass_rows:
            by_from[from_t].append(to_t)
        for from_t in sorted(by_from):
            tos = ", ".join(sorted(by_from[from_t]))
            lines.append(f"- {from_t} → {tos}")
        lines.append("")

    if fail_rows:
        lines.append(f"**🟡 Failed ({len(fail_rows)} type pairs)**\n")
        lines.append(f"| From | To | {suite_headers} |")
        lines.append("|---|---| " + " | ".join(["---"] * len(suites)) + " |")
        for from_t, to_t, sm in fail_rows:
            cells = " | ".join(sm.get(s, "⚪") for s in suites)
            lines.append(f"| {from_t} | {to_t} | {cells} |")
        lines.append("")


def _format_generic_table(lines, matrix, suites, key_label):
    """Format a generic matrix table split into fallback/pass/fail groups."""
    fallback_rows = []
    pass_rows = []
    fail_rows = []

    for key, suite_map in sorted(matrix.items()):
        merged = "⚪"
        for s in suites:
            merged = _worse(merged, suite_map.get(s, "⚪"))

        if merged == "🔴":
            fallback_rows.append((key, suite_map))
        elif merged == "🟢":
            pass_rows.append((key, suite_map))
        else:
            fail_rows.append((key, suite_map))

    suite_headers = " | ".join(s.replace("Gluten", "") for s in suites)

    if fallback_rows:
        lines.append(f"**🔴 Fallback ({len(fallback_rows)})**\n")
        lines.append(f"| {key_label} | {suite_headers} |")
        lines.append("|---| " + " | ".join(["---"] * len(suites)) + " |")
        for key, sm in fallback_rows:
            cells = " | ".join(sm.get(s, "⚪") for s in suites)
            lines.append(f"| {key} | {cells} |")
        lines.append("")

    if pass_rows:
        lines.append(f"**🟢 Passed ({len(pass_rows)})**\n")
        for key, _ in pass_rows:
            lines.append(f"- {key}")
        lines.append("")

    if fail_rows:
        lines.append(f"**🟡 Failed ({len(fail_rows)})**\n")
        lines.append(f"| {key_label} | {suite_headers} |")
        lines.append("|---| " + " | ".join(["---"] * len(suites)) + " |")
        for key, sm in fail_rows:
            cells = " | ".join(sm.get(s, "⚪") for s in suites)
            lines.append(f"| {key} | {cells} |")
        lines.append("")


def format_full(summary, json_tests, categories):
    """Format full report: summary + matrix in <details>."""
    parts = [format_summary(summary, json_tests)]
    matrix_md = format_matrix(categories)
    parts.append("<details>")
    parts.append(
        f"<summary>📊 Expression Offload Matrix "
        f"({len(categories)} categories)</summary>\n")
    parts.append(matrix_md)
    parts.append("</details>")
    return "\n".join(parts)


def format_json_output(summary, json_tests, xml_tests, categories):
    """Format analysis results as JSON."""
    output = {
        "summary": summary,
        "categories": {},
    }
    for cat, data in categories.items():
        output["categories"][cat] = {
            "tests_pass": data["tests_pass"],
            "tests_fail": data["tests_fail"],
            "tests_skip": data["tests_skip"],
            "suites": sorted(data["suites"]),
            "entry_count": len(data["entries"]),
        }
    return json.dumps(output, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Output targets
# ---------------------------------------------------------------------------

def post_pr_comment(report):
    pr = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN", "")
    if not all([pr, repo, token]):
        print("Warning: missing PR_NUMBER/GITHUB_REPOSITORY/GH_TOKEN, "
              "skipping PR comment", file=sys.stderr)
        return
    cmd = [
        "gh", "api",
        f"repos/{repo}/issues/{pr}/comments",
        "-f", f"body={report}",
    ]
    env = dict(os.environ, GH_TOKEN=token)
    subprocess.run(cmd, env=env, check=True)
    print(f"Posted PR comment to {repo}#{pr}")


def write_job_summary(report):
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(report + "\n")


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Unified ANSI test analyzer")
    parser.add_argument("--json-dir", help="JSON directory from Tracker")
    parser.add_argument("--report-dir", help="Surefire XML directory")
    parser.add_argument("--mode", choices=["summary", "matrix", "full", "json"],
                        default="full")
    parser.add_argument("--pr-comment", action="store_true")
    parser.add_argument("--job-summary", action="store_true")
    parser.add_argument("--output-file", help="Write output to file")
    args = parser.parse_args()

    # Load data
    suites = load_json_data(args.json_dir)
    xml_results = load_surefire_xml(args.report_dir)

    # Analyze
    json_tests = analyze_json_tests(suites)
    xml_tests = analyze_xml_tests(xml_results)
    summary = build_summary(json_tests, xml_tests)
    categories = analyze_expression_matrix(suites)

    # Format output
    if args.mode == "summary":
        report = format_summary(summary, json_tests)
    elif args.mode == "matrix":
        report = format_matrix(categories)
    elif args.mode == "full":
        report = format_full(summary, json_tests, categories)
    elif args.mode == "json":
        report = format_json_output(summary, json_tests, xml_tests, categories)

    # Output
    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(report)
        print(f"Report written to {args.output_file}")

    if args.pr_comment:
        post_pr_comment(report)

    if args.job_summary:
        write_job_summary(report)

    if not args.output_file and not args.pr_comment:
        print(report)

    test_count = summary["total"]
    fail_count = len(summary["failures"])
    print(f"Analysis complete: {test_count} tests, {fail_count} failures",
          file=sys.stderr)


if __name__ == "__main__":
    main()
