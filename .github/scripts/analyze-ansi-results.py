#!/usr/bin/env python3
"""Analyze ANSI mode test results and generate AI-powered report.

Parses surefire XML reports from CI test jobs, classifies each test with
a three-color scheme, optionally calls GitHub Models API for analysis,
and posts a markdown report as a PR comment + Job Summary.

Three-color classification:
  🟢 Passed + Offload  — test passes natively in Velox ANSI mode
  🟡 Failed + Offload  — test runs in Velox but fails (needs ANSI fix)
  🔴 Passed + Fallback — test passes but fell back to Spark (Velox can't handle it)

Environment variables:
  GH_TOKEN       — GitHub token for PR comments and GitHub Models API
  AI_MODEL       — Model name for GitHub Models API (default: gpt-4o)
  PR_NUMBER      — PR number to comment on
  TRIGGERED_BY   — GitHub user who triggered the analysis
  RUN_ID         — GitHub Actions run ID
"""
import glob
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

REPORT_DIR = "./test-reports/"
AI_ENDPOINT = "https://models.inference.ai.azure.com/chat/completions"


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

            has_fallback = "Validation failed" in message or "NOT use gluten" in message

            results.append({
                "suite": classname,
                "test": test_name,
                "status": status,
                "message": message,
                "time": time_s,
                "fallback": has_fallback,
                "job": job_name,
            })
    return results


def _infer_job_name(xml_path):
    """Infer which CI job produced this report from the artifact directory structure."""
    parts = Path(xml_path).parts
    for p in parts:
        if "spark-test-" in p:
            return p
    return "unknown"


def classify(test):
    """Assign three-color classification."""
    if test["status"] == "SKIPPED":
        return "⚪", "Skipped"
    if test["status"] in ("PASSED",) and not test["fallback"]:
        return "🟢", "Passed+Offload"
    if test["status"] in ("FAILED", "ERROR") and not test["fallback"]:
        return "🟡", "Failed+Offload"
    if test["fallback"]:
        return "🔴", "Passed+Fallback" if test["status"] == "PASSED" else "Failed+Fallback"
    return "🟡", "Failed"


def build_summary(results):
    """Build a summary dict with counts by color and job."""
    summary = {
        "total": len(results),
        "by_color": defaultdict(int),
        "by_job": defaultdict(lambda: defaultdict(int)),
        "failures": [],
    }
    for t in results:
        color, label = classify(t)
        t["color"] = color
        t["label"] = label
        summary["by_color"][label] += 1
        summary["by_job"][t["job"]][label] += 1
        if t["status"] in ("FAILED", "ERROR"):
            summary["failures"].append(t)
    return summary


def format_markdown_report(summary, results, ai_analysis=None):
    """Generate markdown report."""
    lines = ["## ANSI Mode Test Analysis Report\n"]

    lines.append("### Overview\n")
    lines.append(f"Total tests: **{summary['total']}**\n")
    lines.append("| Classification | Count |")
    lines.append("|---|---|")
    for label in ["Passed+Offload", "Failed+Offload", "Passed+Fallback",
                  "Failed+Fallback", "Failed", "Skipped"]:
        count = summary["by_color"].get(label, 0)
        if count > 0:
            color = {"Passed+Offload": "🟢", "Failed+Offload": "🟡",
                     "Passed+Fallback": "🔴", "Failed+Fallback": "🔴",
                     "Failed": "🟡", "Skipped": "⚪"}.get(label, "")
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
        lines.append("| Suite | Test | Color | Message |")
        lines.append("|---|---|---|---|")
        for t in summary["failures"][:50]:
            msg = t["message"][:100].replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {t['suite'].split('.')[-1]} | {t['test']} | {t['color']} {t['label']} | {msg} |")
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


def call_github_models_api(summary, failures_sample):
    """Call GitHub Models API for AI analysis. Returns analysis text or None."""
    token = os.environ.get("GH_TOKEN")
    model = os.environ.get("AI_MODEL", "gpt-4o")
    if not token:
        return None

    failure_text = "\n".join(
        f"- {t['suite'].split('.')[-1]}.{t['test']}: {t['color']} {t['label']} — {t['message'][:200]}"
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
            return f"*AI analysis unavailable (HTTP {resp.status_code}). " \
                   f"The commenter may not have GitHub Models API access.*"
    except Exception as e:
        return f"*AI analysis unavailable: {e}*"


def post_pr_comment(markdown):
    """Post markdown as PR comment via gh CLI."""
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
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"Failed to post PR comment: {e}")


def write_job_summary(markdown):
    """Write markdown to GitHub Actions Job Summary."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(markdown)
        print("Wrote Job Summary")


def main():
    results = parse_surefire_reports(REPORT_DIR)
    if not results:
        msg = "## ANSI Mode Test Analysis\n\nNo test reports found. " \
              "Build may have failed — check individual job logs."
        post_pr_comment(msg)
        write_job_summary(msg)
        print("No surefire reports found")
        sys.exit(0)

    summary = build_summary(results)

    ai_analysis = call_github_models_api(summary, summary["failures"])

    report = format_markdown_report(summary, results, ai_analysis)

    post_pr_comment(report)
    write_job_summary(report)

    print(f"Analysis complete: {summary['total']} tests, "
          f"{len(summary['failures'])} failures")


if __name__ == "__main__":
    main()
