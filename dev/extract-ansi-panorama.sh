#!/bin/bash
# Extract ANSI panorama tree from ScalaTest output.
# Usage: ./dev/extract-ansi-panorama.sh <test-output-file>
#   or:  ... | ./dev/extract-ansi-panorama.sh
#
# Input: ScalaTest output lines like:
#   - ANSI: String>Int: 'abc' (100 milliseconds)
#   - TRY: String>Int: 'abc' *** FAILED *** (50 milliseconds)
#
# Output: Tree grouped by MODE → TypePair → case with ✅/❌/⚠️

set -euo pipefail

INPUT="${1:--}"

# Strip ANSI colors, extract test lines
parse_lines() {
  sed 's/\x1b\[[0-9;]*m//g' | \
  grep -E '^- (ANSI|TRY|LEGACY):' | \
  sed -E 's/^- (ANSI|TRY|LEGACY): (.+>)([^:]+): (.*) \((FAILED|[0-9]+ .+)\)$/\1|\2\3|\4|\5/' | \
  sed -E 's/^- (ANSI|TRY|LEGACY): (.+>)([^:]+): (.*) \*\*\* FAILED \*\*\* \(.*\)$/\1|\2\3|\4|FAILED/' | \
  sed -E 's/^- (ANSI|TRY|LEGACY): (.+>)([^:]+): (.*) \(([0-9]+ .+)\)$/\1|\2\3|\4|PASSED/'
}

# Simpler approach: just parse mode, full desc, and status
parse_simple() {
  sed 's/\x1b\[[0-9;]*m//g' | \
  grep -E '^- (ANSI|TRY|LEGACY):' | \
  while IFS= read -r line; do
    if echo "$line" | grep -q 'FAILED'; then
      status="FAILED"
    elif echo "$line" | grep -q '(Pending)'; then
      status="PENDING"
    else
      status="PASSED"
    fi
    # Extract mode and desc
    mode=$(echo "$line" | sed -E 's/^- (ANSI|TRY|LEGACY): .*/\1/')
    desc=$(echo "$line" | sed -E 's/^- (ANSI|TRY|LEGACY): (.*[^ ]) +\(.*/\2/' | sed 's/ \*\*\* FAILED \*\*\*//')
    # Split desc into type_pair and case_detail
    if echo "$desc" | grep -q '>.*:'; then
      type_pair=$(echo "$desc" | sed -E 's/^([^:]+): .*/\1/')
      case_detail=$(echo "$desc" | sed -E 's/^[^:]+: //')
    else
      type_pair="Other"
      case_detail="$desc"
    fi
    echo "${mode}|${type_pair}|${case_detail}|${status}"
  done
}

# Read input
if [ "$INPUT" = "-" ]; then
  data=$(cat)
else
  data=$(cat "$INPUT")
fi

# Parse
parsed=$(echo "$data" | parse_simple)

if [ -z "$parsed" ]; then
  echo "No test results found in input."
  exit 1
fi

# Count totals
total=$(echo "$parsed" | wc -l)
passed=$(echo "$parsed" | grep -c '|PASSED$' || true)
failed=$(echo "$parsed" | grep -c '|FAILED$' || true)
pending=$(echo "$parsed" | grep -c '|PENDING$' || true)

echo "═══════════════════════════════════════════════════════════"
echo "  Velox ANSI Panorama — Cast"
echo "  Total: $total  ✅ $passed  ❌ $failed  ⚠️ $pending"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Print tree grouped by mode → type_pair → case
for mode in ANSI TRY LEGACY; do
  mode_data=$(echo "$parsed" | grep "^${mode}|" || true)
  if [ -z "$mode_data" ]; then
    continue
  fi
  mode_passed=$(echo "$mode_data" | grep -c '|PASSED$' || true)
  mode_failed=$(echo "$mode_data" | grep -c '|FAILED$' || true)
  mode_pending=$(echo "$mode_data" | grep -c '|PENDING$' || true)
  mode_total=$(echo "$mode_data" | wc -l)

  echo "┌─ ${mode} (${mode_passed}/${mode_total} passed)"

  # Get unique type pairs in order
  type_pairs=$(echo "$mode_data" | cut -d'|' -f2 | awk '!seen[$0]++')

  last_tp=$(echo "$type_pairs" | tail -1)

  while IFS= read -r tp; do
    tp_data=$(echo "$mode_data" | grep "^${mode}|${tp}|" || true)
    tp_count=$(echo "$tp_data" | wc -l)
    tp_passed=$(echo "$tp_data" | grep -c '|PASSED$' || true)

    if [ "$tp" = "$last_tp" ]; then
      branch="└──"
      indent="    "
    else
      branch="├──"
      indent="│   "
    fi

    echo "${branch} ${tp} (${tp_passed}/${tp_count})"

    last_case=$(echo "$tp_data" | tail -1)
    while IFS='|' read -r m t c s; do
      case "$s" in
        PASSED)  icon="✅" ;;
        FAILED)  icon="❌" ;;
        PENDING) icon="⚠️" ;;
        *)       icon="?" ;;
      esac

      current_line="${m}|${t}|${c}|${s}"
      if [ "$current_line" = "$last_case" ]; then
        echo "${indent}└── ${icon} ${c}"
      else
        echo "${indent}├── ${icon} ${c}"
      fi
    done <<< "$tp_data"
  done <<< "$type_pairs"
  echo ""
done
