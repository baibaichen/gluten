#!/usr/bin/env bash
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

set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: dev/run-expression-bench.sh functionsCSV [options] | --cases function/case,glob [options]' \
    '       dev/run-expression-bench.sh --list [functionsCSV] | --config file [options]' \
    'Options: --rows --batch-size --seed --key-cardinality' \
    '         --warmup-seconds N --measurement-seconds N' \
    '         --async-profiler dir --profile-event cpu|alloc --profile-output dir' \
    'Default: 10s warmup / 60s measurement per engine; at least 2 iterations.' \
    'Always measures JVM first, then Native.' \
    'Requires JAVA_HOME (Java 17 HotSpot) and native libraries built in this checkout.' \
    'Compiles Spark 4.1 / Scala 2.13 sources before each run; does not rebuild native libraries.' \
    'Example: dev/run-expression-bench.sh --cases rtrim/l13-half-even --rows 4000000 --batch-size 10240'
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ $# -eq 0 ]]; then
  usage >&2
  exit 1
fi
MAIN='org.apache.spark.sql.execution.benchmark.expression.RegisteredExpressionBenchmark'
CALLER_CWD="$PWD"

if [[ -z "${JAVA_HOME:-}" || ! -x "$JAVA_HOME/bin/java" ]]; then
  printf 'Set JAVA_HOME to a Java 17 HotSpot JDK.\n' >&2
  exit 1
fi

GLUTEN_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$GLUTEN_HOME"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
PROFILES=java-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,delta
TARGET="$GLUTEN_HOME/backends-velox/target"
CLASSPATH_FILE="$TARGET/expression-benchmark.classpath"
JVM_ARGS_FILE="$TARGET/expression-benchmark.jvmargs"

mkdir -p "$TARGET"

# Install this reactor's dependencies rather than using older local SNAPSHOT jars.
./build/mvn -P"$PROFILES" -pl backends-velox -am install -DskipTests
./build/mvn -P"$PROFILES" -pl backends-velox dependency:build-classpath \
  -DincludeScope=test -Dmdep.outputFile="$CLASSPATH_FILE"
./build/mvn -N -P"$PROFILES" help:evaluate -q \
  -Dexpression=extraJavaTestArgs -Doutput="$JVM_ARGS_FILE"
test -s "$CLASSPATH_FILE"
test -s "$JVM_ARGS_FILE"

exec "$JAVA_HOME/bin/java" @"$JVM_ARGS_FILE" \
  -Dspark.testing=true -Xms4g -Xmx4g \
  "-Dgluten.expressionBenchmark.cwd=$CALLER_CWD" \
  -XX:+UnlockExperimentalVMOptions \
  '-XX:CompileCommand=blackhole,org.apache.spark.sql.execution.benchmark.expression.BenchmarkBlackhole::consume' \
  -cp "$TARGET/scala-2.13/test-classes:$TARGET/scala-2.13/classes:$(< "$CLASSPATH_FILE")" \
  "$MAIN" "$@"
