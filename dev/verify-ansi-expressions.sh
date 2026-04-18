#!/usr/bin/env bash
# verify-ansi-expressions.sh — 按 expression-matrix 分类验证 ANSI 表达式
#
# 用法：
#   cd /root/SourceCode/gluten
#   bash dev/verify-ansi-expressions.sh <category> [spark41|spark40|all] [--clean]
#
# category（对应矩阵第三节）：
#   cast        — §3.1.1 Cast + §3.3.1 try_cast
#   arithmetic  — §3.1.2 算术 + §3.2.6 Abs/UnaryMinus + §3.3.1 try 算术
#   collection  — §3.2.1 集合 + §3.3.2 try_element_at
#   datetime    — §3.2.2 日期时间/Interval + §3.3.2 try_to_timestamp 等
#   math        — §3.2.3 数学（Round/BRound/conv）
#   decimal     — §3.2.4 Decimal（CheckOverflow）
#   string      — §3.2.5 字符串 + §3.3.2 try_parse_url
#   aggregate   — §3.1.3 聚合 + §3.4 间接（Sum/Avg/VAR/STDDEV，需人工校验）
#   errors      — QueryExecutionAnsiErrorsSuite
#   all         — 以上全部（一次性组装所有 suite，单次 JVM 执行）
#
# spark version（默认 spark41）：
#   spark41     — Spark 4.1
#   spark40     — Spark 4.0
#   all         — 先 spark41 再 spark40
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SPARK_ANSI_SQL_MODE=true
export SPARK_TESTING=true

CATEGORY="${1:?Usage: $0 <category> [spark41|spark40|all] [--clean]}"
SPARK_VER="${2:-spark41}"
CLEAN_FLAG=""
if [[ "${3:-}" == "--clean" ]] || [[ "${2:-}" == "--clean" ]]; then
  CLEAN_FLAG="--clean"
  # if --clean was $2, default spark version
  if [[ "${2:-}" == "--clean" ]]; then
    SPARK_VER="spark41"
  fi
fi

case "${SPARK_VER}" in
  spark41) PROFILES="-Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta"; UT_MODULE="gluten-ut/spark41" ;;
  spark40) PROFILES="-Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,delta"; UT_MODULE="gluten-ut/spark40" ;;
  all)     ;; # handled in main entry
  *)       echo "Unknown spark version: ${SPARK_VER}"; echo "Usage: $0 <category> [spark41|spark40|all] [--clean]"; exit 1 ;;
esac

ANSI_ARG="--jvm-arg -Dspark.gluten.sql.ansiFallback.enabled=false"
LOG_TS="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="/tmp/ansi-matrix/${LOG_TS}"
mkdir -p "${LOG_DIR}"
# Symlink latest run for easy access
ln -sfn "${LOG_DIR}" "/tmp/ansi-matrix/latest"

# ── Suite 定义 ──────────────────────────────────────────────
# 按矩阵第三节，强相关 Suite 映射

# §3.1.1 Cast + §3.3.1 try_cast
CAST_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOnSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOffSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenTryCastSuite
)
CAST_BACKENDS=(
  -s org.apache.spark.sql.catalyst.expressions.VeloxCastSuite
)

# §3.1.2 算术 + §3.2.6 Abs/UnaryMinus + §3.3.1 try 算术
ARITHMETIC_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenArithmeticExpressionSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenTryEvalSuite
)
ARITHMETIC_BACKENDS=(
  -s org.apache.gluten.functions.ArithmeticAnsiValidateSuite
  -s org.apache.gluten.functions.MathFunctionsValidateSuiteAnsiOn
)

# §3.2.1 集合 + §3.3.2 try_element_at
COLLECTION_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenCollectionExpressionsSuite
)

# §3.2.2 日期时间/Interval + §3.3.2 try_to_timestamp 等
DATETIME_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenDateExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenIntervalExpressionsSuite
  -s org.apache.spark.sql.GlutenDateFunctionsSuite
)

# §3.2.3 数学
MATH_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenMathExpressionsSuite
)

# §3.2.4 Decimal
DECIMAL_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenDecimalExpressionSuite
)

# §3.2.5 字符串 + §3.3.2 try_parse_url
STRING_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenStringExpressionsSuite
  -s org.apache.spark.sql.GlutenUrlFunctionsSuite
)

# §3.1.3 聚合 + §3.4 间接（VAR/STDDEV）— 需人工校验
AGGREGATE_UT=(
  -s org.apache.spark.sql.GlutenDataFrameAggregateSuite
)

# ANSI 错误语义
ERRORS_UT=(
  -s org.apache.spark.sql.errors.GlutenQueryExecutionAnsiErrorsSuite
)

# ── 运行函数 ──────────────────────────────────────────────

run_single() {
  local label="$1"
  local module="$2"
  local profiles="$3"
  shift 3
  local log="${LOG_DIR}/${label}-${SPARK_VER}.log"
  echo ""
  echo "=== ${label}: ${module} (${SPARK_VER}) ==="
  ./dev/run-scala-test.sh --mvnd \
    ${CLEAN_FLAG} \
    ${ANSI_ARG} \
    ${profiles} \
    -pl "${module}" \
    "$@" \
    2>&1 | tee "${log}"
  # 只第一次 clean
  CLEAN_FLAG=""
}

# ── Collect suites for a category ──────────────────────────

get_ut_suites() {
  local cat="$1"
  case "${cat}" in
    cast)       echo "${CAST_UT[*]}" ;;
    arithmetic) echo "${ARITHMETIC_UT[*]}" ;;
    collection) echo "${COLLECTION_UT[*]}" ;;
    datetime)   echo "${DATETIME_UT[*]}" ;;
    math)       echo "${MATH_UT[*]}" ;;
    decimal)    echo "${DECIMAL_UT[*]}" ;;
    string)     echo "${STRING_UT[*]}" ;;
    aggregate)  echo "${AGGREGATE_UT[*]}" ;;
    errors)     echo "${ERRORS_UT[*]}" ;;
  esac
}

get_backends_suites() {
  local cat="$1"
  case "${cat}" in
    cast)       echo "${CAST_BACKENDS[*]}" ;;
    arithmetic) echo "${ARITHMETIC_BACKENDS[*]}" ;;
    *)          echo "" ;;
  esac
}

ALL_CATEGORIES=(cast arithmetic collection datetime math decimal string aggregate errors)

# ── 分类执行 ──────────────────────────────────────────────

run_category_single() {
  local cat="$1"
  local ut_suites
  read -ra ut_suites <<< "$(get_ut_suites "${cat}")"
  if [[ ${#ut_suites[@]} -gt 0 ]]; then
    run_single "${cat}-ut" "${UT_MODULE}" "${PROFILES},spark-ut" "${ut_suites[@]}"
  fi

  local backends_suites
  read -ra backends_suites <<< "$(get_backends_suites "${cat}")"
  if [[ ${#backends_suites[@]} -gt 0 ]]; then
    run_single "${cat}-backends" "backends-velox" "${PROFILES}" "${backends_suites[@]}"
  fi
}

run_all() {
  # Assemble all UT suites into one invocation
  local all_ut_suites=()
  for cat in "${ALL_CATEGORIES[@]}"; do
    local suites
    read -ra suites <<< "$(get_ut_suites "${cat}")"
    all_ut_suites+=("${suites[@]}")
  done

  echo ""
  echo "=== ALL UT suites (single JVM, ${#all_ut_suites[@]} -s args) ==="
  run_single "all-ut" "${UT_MODULE}" "${PROFILES},spark-ut" "${all_ut_suites[@]}"

  # Assemble all backends suites into one invocation
  local all_backends_suites=()
  for cat in "${ALL_CATEGORIES[@]}"; do
    local suites
    read -ra suites <<< "$(get_backends_suites "${cat}")"
    if [[ ${#suites[@]} -gt 0 && -n "${suites[0]}" ]]; then
      all_backends_suites+=("${suites[@]}")
    fi
  done

  if [[ ${#all_backends_suites[@]} -gt 0 ]]; then
    echo ""
    echo "=== ALL backends suites (single JVM, ${#all_backends_suites[@]} -s args) ==="
    run_single "all-backends" "backends-velox" "${PROFILES}" "${all_backends_suites[@]}"
  fi
}

# ── 主入口 ──────────────────────────────────────────────

run_for_spark_ver() {
  case "${CATEGORY}" in
    all) run_all ;;
    *)   run_category_single "${CATEGORY}" ;;
  esac
}

echo "========================================"
echo "ANSI Expression Matrix Verification"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Category: ${CATEGORY}"
echo "Spark: ${SPARK_VER}"
echo "SPARK_ANSI_SQL_MODE=${SPARK_ANSI_SQL_MODE}"
echo "SPARK_TESTING=${SPARK_TESTING}"
echo "ansiFallback=false"
echo "Logs: ${LOG_DIR}/"
echo "========================================"

if [[ "${SPARK_VER}" == "all" ]]; then
  # Run spark41 first, then spark40
  SPARK_VER="spark41"
  PROFILES="-Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta"
  UT_MODULE="gluten-ut/spark41"
  run_for_spark_ver

  SPARK_VER="spark40"
  PROFILES="-Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,delta"
  UT_MODULE="gluten-ut/spark40"
  CLEAN_FLAG="--clean"
  run_for_spark_ver
else
  run_for_spark_ver
fi

echo ""
echo "========================================"
echo "Verification Complete — ${CATEGORY}"
echo "Results:"
if [[ "${CATEGORY}" == "all" ]]; then
  LOG_PATTERN="${LOG_DIR}/all-*.log"
else
  LOG_PATTERN="${LOG_DIR}/${CATEGORY}-*.log"
fi
for f in ${LOG_PATTERN}; do
  if [ -f "$f" ]; then
    echo ""
    echo "--- $(basename "$f") ---"
    python3 "${SCRIPT_DIR}/analyze-ansi-test-log.py" "$f"
  fi
done
echo "========================================"
