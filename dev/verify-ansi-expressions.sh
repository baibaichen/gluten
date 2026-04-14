#!/usr/bin/env bash
# verify-ansi-expressions.sh — 验证 73 个 ANSI 表达式
#
# 基于 expression-matrix.md v3，运行所有覆盖 ANSI 影响表达式的 Gluten UT
# 分 4 段运行：spark41 gluten-ut + backends-velox，spark40 gluten-ut + backends-velox
#
# 用法：
#   cd /root/SourceCode/gluten
#   bash dev/verify-ansi-expressions.sh [spark41|spark40|all]
#
# 输出：
#   /tmp/ansi-verify-spark41-ut.log
#   /tmp/ansi-verify-spark41-backends.log
#   /tmp/ansi-verify-spark40-ut.log
#   /tmp/ansi-verify-spark40-backends.log

set -uo pipefail

export SPARK_ANSI_SQL_MODE=true
export SPARK_TESTING=true

TARGET="${1:-all}"

SUITES_UT=(
  -s org.apache.spark.sql.catalyst.expressions.GlutenArithmeticExpressionSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOnSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenCastWithAnsiOffSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenTryCastSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenMathExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenCollectionExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenStringExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenDateExpressionsSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenDecimalExpressionSuite
  -s org.apache.spark.sql.catalyst.expressions.GlutenDecimalPrecisionSuite
  -s org.apache.spark.sql.GlutenMathFunctionsSuite
  -s org.apache.spark.sql.GlutenDataFrameAggregateSuite
  -s org.apache.spark.sql.GlutenStringFunctionsSuite
  -s org.apache.spark.sql.errors.GlutenQueryExecutionAnsiErrorsSuite
)

SUITES_BACKENDS=(
  -s org.apache.gluten.functions.ArithmeticAnsiValidateSuite
  -s org.apache.gluten.functions.MathFunctionsValidateSuiteAnsiOn
  -s org.apache.gluten.functions.FunctionsValidateSuite
  -s org.apache.spark.sql.catalyst.expressions.VeloxCastSuite
)

echo "========================================"
echo "ANSI Expression Matrix Verification"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Target: ${TARGET}"
echo "SPARK_ANSI_SQL_MODE=${SPARK_ANSI_SQL_MODE}"
echo "SPARK_TESTING=${SPARK_TESTING}"
echo "spark.gluten.sql.ansiFallback.enabled=false"
echo "========================================"

run_spark41() {
  local PROFILES="-Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3"
  local ANSI_ARG='--jvm-arg -Dspark.gluten.sql.ansiFallback.enabled=false'

  echo ""
  echo "=== Part 1/4: spark41 gluten-ut (14 suites) ==="
  ./dev/run-scala-test.sh \
    --clean \
    ${ANSI_ARG} \
    ${PROFILES},spark-ut \
    -pl gluten-ut/spark41 \
    "${SUITES_UT[@]}" \
    2>&1 | tee /tmp/ansi-verify-spark41-ut.log

  echo ""
  echo "=== Part 2/4: spark41 backends-velox (4 suites) ==="
  ./dev/run-scala-test.sh \
    ${ANSI_ARG} \
    ${PROFILES} \
    -pl backends-velox \
    "${SUITES_BACKENDS[@]}" \
    2>&1 | tee /tmp/ansi-verify-spark41-backends.log
}

run_spark40() {
  local PROFILES="-Pjava-17,spark-4.0,scala-2.13,backends-velox,hadoop-3.3"
  local ANSI_ARG='--jvm-arg -Dspark.gluten.sql.ansiFallback.enabled=false'

  echo ""
  echo "=== Part 3/4: spark40 gluten-ut (14 suites) ==="
  ./dev/run-scala-test.sh \
    --clean \
    ${ANSI_ARG} \
    ${PROFILES},spark-ut \
    -pl gluten-ut/spark40 \
    "${SUITES_UT[@]}" \
    2>&1 | tee /tmp/ansi-verify-spark40-ut.log

  echo ""
  echo "=== Part 4/4: spark40 backends-velox (4 suites) ==="
  ./dev/run-scala-test.sh \
    ${ANSI_ARG} \
    ${PROFILES} \
    -pl backends-velox \
    "${SUITES_BACKENDS[@]}" \
    2>&1 | tee /tmp/ansi-verify-spark40-backends.log
}

case "${TARGET}" in
  spark41) run_spark41 ;;
  spark40) run_spark40 ;;
  all)     run_spark41; run_spark40 ;;
  *)       echo "Usage: $0 [spark41|spark40|all]"; exit 1 ;;
esac

echo ""
echo "========================================"
echo "Verification Complete"
echo "Results:"
for f in /tmp/ansi-verify-spark4{1,0}-{ut,backends}.log; do
  if [ -f "$f" ]; then
    echo "  $(basename $f):"
    grep -E "Tests:.*succeeded|All tests passed|FAILED" "$f" | tail -2 | sed 's/^/    /'
  fi
done
echo "========================================"
