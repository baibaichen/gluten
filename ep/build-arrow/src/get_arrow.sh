#!/bin/bash
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

set -exu

ARROW_REPO=https://github.com/apache/arrow.git
ARROW_BRANCH=apache-arrow-12.0.0
ARROW_HOME=""

for arg in "$@"; do
  case $arg in
  --arrow_repo=*)
    ARROW_REPO=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --arrow_branch=*)
    ARROW_BRANCH=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --arrow_home=*)
    ARROW_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function checkout_code {
  TARGET_BUILD_COMMIT="$(git ls-remote $ARROW_REPO $ARROW_BRANCH | awk '{print $1;}')"
  if [ -d $ARROW_SOURCE_DIR ]; then
    echo "Arrow source folder $ARROW_SOURCE_DIR already exists..."
    cd $ARROW_SOURCE_DIR
    git init .
    EXISTS=$(git show-ref refs/tags/build_$TARGET_BUILD_COMMIT || true)
    if [ -z "$EXISTS" ]; then
      git fetch $ARROW_REPO $TARGET_BUILD_COMMIT:refs/tags/build_$TARGET_BUILD_COMMIT
    fi
    git reset --hard HEAD
    git checkout refs/tags/build_$TARGET_BUILD_COMMIT
  else
    git clone $ARROW_REPO -b $ARROW_BRANCH $ARROW_SOURCE_DIR
    cd $ARROW_SOURCE_DIR
    git checkout $TARGET_BUILD_COMMIT
  fi
}

echo "Preparing Arrow source code..."

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$ARROW_HOME" == "" ]; then
  ARROW_HOME="$CURRENT_DIR/../build"
fi
ARROW_SOURCE_DIR="${ARROW_HOME}/arrow_ep"

checkout_code

cat > cpp/src/parquet/symbols.map <<EOF
{
  global:
    extern "C++" {
      *parquet::*;
    };
  local:
    *;
};
EOF

echo "Arrow-get finished."
