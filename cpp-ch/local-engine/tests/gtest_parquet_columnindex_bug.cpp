/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gluten_test_util.h>
#include <incbin.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parser/LocalExecutor.h>
#include <Parser/SubstraitParserUtils.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/DebugUtils.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>

using namespace local_engine;

using namespace DB;

INCBIN(_pr_18_2, SOURCE_DIR "/utils/extern-local-engine/tests/decimal_filter_push_down/18_2.json");
TEST(ColumnIndex, Decimal182)
{
    // [precision,scale] = [18,2]
    auto query_id = local_engine::QueryContext::instance().initializeQuery("RowIndex");
    SCOPE_EXIT({ local_engine::QueryContext::instance().finalizeQuery(query_id); });
    const auto context = local_engine::QueryContext::instance().currentQueryContext();
    const auto config = ExecutorConfig::loadFromContext(context);
    EXPECT_TRUE(config.use_local_format) << "gtest need set use_local_format to true";

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"488","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    constexpr std::string_view file{GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/decimal_filter_push_down/18_2_flba.snappy.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_18_2), split_template, file, context);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}

INCBIN(_read_metadata, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/read_metadata.row_index.json");
TEST(RowIndex, Basic)
{
    auto query_id = local_engine::QueryContext::instance().initializeQuery("RowIndex");
    SCOPE_EXIT({ local_engine::QueryContext::instance().finalizeQuery(query_id); });
    const auto context = local_engine::QueryContext::instance().currentQueryContext();
    constexpr std::string_view file{GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/data/metadata.rowindex.snappy.parquet")};
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1767","parquet":{},"partitionColumns":[{"key":"pb","value":"1003"}],"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"1767"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"1767","modificationTime":"1736847651881"}}]})";

    auto [_, local_executor] = local_engine::test::create_plan_and_executor(EMBEDDED_PLAN(_read_metadata), split_template, file, context);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}

INCBIN(_rowindex_in, SOURCE_DIR "/utils/extern-local-engine/tests/json/parquet_metadata/rowindex_in.json");
TEST(RowIndex, In)
{
    auto query_id = local_engine::QueryContext::instance().initializeQuery("RowIndex");
    SCOPE_EXIT({ local_engine::QueryContext::instance().finalizeQuery(query_id); });
    const auto context = local_engine::QueryContext::instance().currentQueryContext();
    constexpr std::string_view file{GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/data/rowindex_in.snappy.parquet")};
    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"256","parquet":{},"schema":{},"metadataColumns":[{"key":"input_file_name","value":"{replace_local_files}"},{"key":"input_file_block_length","value":"256"},{"key":"input_file_block_start","value":"0"}],"properties":{"fileSize":"125451","modificationTime":"1737104830724"}}]})";

    auto [_, local_executor] = local_engine::test::create_plan_and_executor(EMBEDDED_PLAN(_rowindex_in), split_template, file, context);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}