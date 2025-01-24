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
#include "ParquetFormatFile.h"

#if USE_PARQUET

#include <memory>

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/Parquet/VirtualColumnRowIndexReader.h>
#include <Common/Exception.h>

namespace DB
{
namespace Setting
{
extern const SettingsMaxThreads max_download_threads;
extern const SettingsMaxThreads max_parsing_threads;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
using namespace ParquetVirtualMeta;
struct ParquetInputFormat : FormatFile::InputFormat
{
    const DB::Block readHeader;
    const DB::Block outputHeader;
    std::unique_ptr<ColumnIndexRowRangesProvider> column_index_row_ranges_provider;
    std::optional<VirtualColumnRowIndexReader> row_index_reader;

    ParquetInputFormat(
        std::unique_ptr<DB::ReadBuffer> read_buffer_,
        const DB::InputFormatPtr & input_,
        std::unique_ptr<ColumnIndexRowRangesProvider> column_index_row_ranges_provider_,
        DB::Block readHeader_,
        DB::Block outputHeader_)
        : InputFormat(std::move(read_buffer_), input_)
        , readHeader(std::move(readHeader_))
        , outputHeader(std::move(outputHeader_))
        , column_index_row_ranges_provider(std::move(column_index_row_ranges_provider_))
        , row_index_reader(
              outputHeader.columns() > readHeader.columns()
                  ? std::make_optional<VirtualColumnRowIndexReader>(*column_index_row_ranges_provider, getMetaColumnType(outputHeader))
                  : std::nullopt)
    {
    }

    DB::Chunk generate() override
    {
        if (readHeader.columns() == 0)
        {
            assert(outputHeader.columns());
            assert(row_index_reader);
            // TODO: format_settings_.parquet.max_block_size
            DB::Columns cols{row_index_reader->readBatch(8192)};
            size_t rows = cols[0]->size();
            return DB::Chunk(std::move(cols), rows);
        }
        auto chunk = input->generate();
        size_t num_rows = chunk.getNumRows();
        if (row_index_reader && num_rows)
        {
            auto row_index_column = row_index_reader->readBatch(num_rows);
            assert(outputHeader.columns() == readHeader.columns() + 1);
            size_t column_pos = outputHeader.getPositionByName(TMP_ROWINDEX);
            if (column_pos < chunk.getNumColumns())
                chunk.addColumn(column_pos, std::move(row_index_column));
            else
                chunk.addColumn(std::move(row_index_column));
        }
        return chunk;
    }
};

ParquetFormatFile::ParquetFormatFile(
    const DB::ContextPtr & context_,
    const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_,
    const ReadBufferBuilderPtr & read_buffer_builder_,
    bool use_local_format_)
    : FormatFile(context_, file_info_, read_buffer_builder_), use_pageindex_reader(use_local_format_)
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(
    const DB::Block & header,
    const std::shared_ptr<const DB::KeyCondition> & key_condition,
    const ColumnIndexFilterPtr & column_index_filter) const
{
    bool readRowIndex = hasMetaColumns(header);
    bool shouldUseClickHouseReader = !supportPageindexReader(header);

    if (shouldUseClickHouseReader && readRowIndex)
        throw DB::Exception(
            DB::ErrorCodes::UNSUPPORTED_METHOD,
            "VectorizedParquetBlockInputFormat doesn't support read complex type and "
            "ParquetBlockInputFormat doesn't support read row index.");

    bool usePageIndexReader = !shouldUseClickHouseReader && (use_pageindex_reader || readRowIndex);

    auto read_buffer = read_buffer_builder->build(file_info);
    auto format_settings = DB::getFormatSettings(context);
    ParquetMetaBuilder metaBuilder;

    DB::Block output_header = header;
    DB::Block read_header = removeMetaColumns(header);

    if (usePageIndexReader)
    {
        metaBuilder.collectPageIndex = true;
        metaBuilder.case_insensitive = format_settings.parquet.case_insensitive_column_matching;
        metaBuilder.allow_missing_columns = format_settings.parquet.allow_missing_columns;
    }
    else
        metaBuilder.collectSkipRowGroup = true;

    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(read_buffer.get()))
    {
        // reuse the read_buffer to avoid opening the file twice.
        // especially，the cost of opening a hdfs file is large.
        metaBuilder.build(seekable_in, file_info, &read_header, column_index_filter.get());
        seekable_in->seek(0, SEEK_SET);
    }
    else
    {
        const auto in = read_buffer_builder->build(file_info);
        metaBuilder.build(in.get(), file_info, &read_header, column_index_filter.get());
    }


    if (usePageIndexReader)
    {
        auto column_index_row_ranges_provider = std::make_unique<ColumnIndexRowRangesProvider>(metaBuilder);

        auto input = std::make_shared<VectorizedParquetBlockInputFormat>(
            *read_buffer, read_header, *column_index_row_ranges_provider, format_settings);
        return std::make_shared<ParquetInputFormat>(
            std::move(read_buffer), input, std::move(column_index_row_ranges_provider), std::move(read_header), std::move(output_header));
    }

    const DB::Settings & settings = context->getSettingsRef();
    format_settings.parquet.skip_row_groups = std::unordered_set<int>(metaBuilder.skipRowGroups.begin(), metaBuilder.skipRowGroups.end());

    auto input = std::make_shared<DB::ParquetBlockInputFormat>(
        *read_buffer,
        read_header,
        format_settings,
        settings[DB::Setting::max_parsing_threads],
        settings[DB::Setting::max_download_threads],
        8192);
    input->setKeyCondition(key_condition);
    return std::make_shared<InputFormat>(std::move(read_buffer), input);
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    auto in = read_buffer_builder->build(file_info);
    auto result = ParquetMetaBuilder::collectRequiredRowGroups(in.get(), file_info);

    size_t rows = 0;
    for (const auto & rowgroup : result.readRowGroups)
        rows += rowgroup.num_rows;

    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

bool ParquetFormatFile::supportPageindexReader(const DB::Block & header)
{
    const auto result = std::ranges::find_if(
        header,
        [](DB::ColumnWithTypeAndName const & col)
        {
            const DB::DataTypePtr type_not_nullable = DB::removeNullable(col.type);
            const DB::WhichDataType which(type_not_nullable);
            return DB::isArray(which) || DB::isMap(which) || DB::isTuple(which);
        });

    return result == header.end();
}


}
#endif
