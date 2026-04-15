#!/usr/bin/env bash
# verify-ansi-expressions.sh — 按 expression-matrix v7 分类验证 ANSI 表达式
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
#   all         — 以上全部
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
LOG_DIR="/tmp/ansi-matrix"
mkdir -p "${LOG_DIR}"

# ── Suite 定义 ──────────────────────────────────────────────
# 按矩阵 v7 第三节，强相关 Suite 映射

# §3.1.1 Cast + §3.3.1 try_cast
# 强相关 Suite: CastWithAnsiOnSuite(42), CastWithAnsiOffSuite(30), TryCastSuite(3), ComplexTypeSuite(2)
CAST_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOnSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOffSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenTryCastSuite
)
CAST_BACKENDS=(
  -s org.apache.spark.sql.catalyst.expressions.VeloxCastSuite
)

# §3.1.2 算术 + §3.2.6 Abs/UnaryMinus + §3.3.1 try 算术
# 强相关 Suite: ArithmeticExpressionSuite(37), TryEvalSuite(1)
ARITHMETIC_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenArithmeticExpressionSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenTryEvalSuite
)
ARITHMETIC_BACKENDS=(
  -s org.apache.gluten.functions.ArithmeticAnsiValidateSuite
  -s org.apache.gluten.functions.MathFunctionsValidateSuiteAnsiOn
)

# §3.2.1 集合 + §3.3.2 try_element_at
# 强相关 Suite: CollectionExpressionsSuite(21)
COLLECTION_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenCollectionExpressionsSuite
)

# §3.2.2 日期时间/Interval + §3.3.2 try_to_timestamp/try_to_date/try_make_timestamp/try_make_interval
# 强相关 Suite: DateExpressionsSuite(22), IntervalExpressionsSuite(9), DateFunctionsSuite(6*)
DATETIME_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenDateExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenIntervalExpressionsSuite
  -s org.apache.spark.sql.GlutenDateFunctionsSuite
)

# §3.2.3 数学
# 强相关 Suite: MathExpressionsSuite(2)
MATH_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenMathExpressionsSuite
)

# §3.2.4 Decimal
# 强相关 Suite: DecimalExpressionSuite(2)
DECIMAL_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenDecimalExpressionSuite
)

# §3.2.5 字符串 + §3.3.2 try_parse_url
# 强相关 Suite: StringExpressionsSuite(3), UrlFunctionsSuite(2*)
STRING_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenStringExpressionsSuite
  -s org.apache.spark.sql.GlutenUrlFunctionsSuite
)

# §3.1.3 聚合 + §3.4 间接（VAR/STDDEV）— 需人工校验
# 强相关 Suite: DataFrameAggregateSuite(2* intercept)
AGGREGATE_UT=(
  -s org.apache.spark.sql.GlutenDataFrameAggregateSuite
)

# ANSI 错误语义
# 强相关 Suite: QueryExecutionAnsiErrorsSuite(3* intercept)
ERRORS_UT=(
  -s org.apache.spark.sql.errors.GlutenQueryExecutionAnsiErrorsSuite
)

# ── 运行函数 ──────────────────────────────────────────────

run_ut() {
  local name="$1"
  shift
  local log="${LOG_DIR}/${name}-${SPARK_VER}-ut.log"
  echo ""
  echo "=== ${name}: ${UT_MODULE} ==="
  ./dev/run-scala-test.sh --mvnd \
    ${CLEAN_FLAG} \
    ${ANSI_ARG} \
    ${PROFILES},spark-ut \
    -pl "${UT_MODULE}" \
    "$@" \
    2>&1 | tee "${log}"
  # 只第一次 clean
  CLEAN_FLAG=""
}

run_backends() {
  local name="$1"
  shift
  local log="${LOG_DIR}/${name}-${SPARK_VER}-backends.log"
  echo ""
  echo "=== ${name}: backends-velox (${SPARK_VER}) ==="
  ./dev/run-scala-test.sh --mvnd \
    ${ANSI_ARG} \
    ${PROFILES} \
    -pl backends-velox \
    "$@" \
    2>&1 | tee "${log}"
}

# ── 分类执行 ──────────────────────────────────────────────

run_cast() {
  run_ut cast "${CAST_UT[@]}"
  run_backends cast "${CAST_BACKENDS[@]}"
}

run_arithmetic() {
  run_ut arithmetic "${ARITHMETIC_UT[@]}"
  run_backends arithmetic "${ARITHMETIC_BACKENDS[@]}"
}

run_collection() {
  run_ut collection "${COLLECTION_UT[@]}"
}

run_datetime() {
  run_ut datetime "${DATETIME_UT[@]}"
}

run_math() {
  run_ut math "${MATH_UT[@]}"
}

run_decimal() {
  run_ut decimal "${DECIMAL_UT[@]}"
}

run_string() {
  run_ut string "${STRING_UT[@]}"
}

run_aggregate() {
  echo "⚠️  aggregate: §3.1.3 + §3.4 — DataFrame 级 intercept，需人工校验结果"
  run_ut aggregate "${AGGREGATE_UT[@]}"
}

run_errors() {
  run_ut errors "${ERRORS_UT[@]}"
}

run_all() {
  run_cast
  run_arithmetic
  run_collection
  run_datetime
  run_math
  run_decimal
  run_string
  run_aggregate
  run_errors
}

# ── 主入口 ──────────────────────────────────────────────

run_category() {
  case "${CATEGORY}" in
    cast)       run_cast ;;
    arithmetic) run_arithmetic ;;
    collection) run_collection ;;
    datetime)   run_datetime ;;
    math)       run_math ;;
    decimal)    run_decimal ;;
    string)     run_string ;;
    aggregate)  run_aggregate ;;
    errors)     run_errors ;;
    all)        run_all ;;
    *)
      echo "Unknown category: ${CATEGORY}"
      echo "Usage: $0 <category> [spark41|spark40|all] [--clean]"
      exit 1
      ;;
  esac
}

echo "========================================"
echo "ANSI Expression Matrix Verification (v7)"
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
  run_category

  SPARK_VER="spark40"
  PROFILES="-Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3,delta"
  UT_MODULE="gluten-ut/spark40"
  CLEAN_FLAG="--clean"
  run_category
else
  run_category
fi

echo ""
echo "========================================"
echo "Verification Complete — ${CATEGORY}"
echo "Results:"
if [[ "${CATEGORY}" == "all" ]]; then
  LOG_PATTERN="${LOG_DIR}/*-ut.log"
else
  LOG_PATTERN="${LOG_DIR}/${CATEGORY}-*-ut.log"
fi
for f in ${LOG_PATTERN}; do
  if [ -f "$f" ]; then
    echo ""
    echo "--- $(basename "$f") ---"
    python3 "${SCRIPT_DIR}/analyze-ansi-test-log.py" "$f"
  fi
done
echo "========================================"
