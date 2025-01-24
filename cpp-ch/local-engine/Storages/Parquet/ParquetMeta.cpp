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

#include "ParquetMeta.h"
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>
#include <Storages/Parquet/ArrowUtils.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/metadata.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}


namespace local_engine
{
std::unique_ptr<parquet::ParquetFileReader> ParquetMetaBuilder::openInputParquetFile(DB::ReadBuffer * read_buffer)
{
    const DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    auto arrow_file = asArrowFile(*read_buffer, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

    return parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties(), nullptr);
}

std::vector<RowGroupInformation>
ParquetMetaBuilder::collectRequiredRowGroups(const parquet::FileMetaData & file_meta, const substraitInputFile & file_info_)
{
    Int32 total_row_groups = file_meta.num_row_groups();

    std::vector<RowGroupInformation> row_group_metadatas;
    row_group_metadatas.reserve(total_row_groups);

    auto get_column_start_offset = [&](const parquet::ColumnChunkMetaData & metadata_)
    {
        Int64 offset = metadata_.data_page_offset();
        if (metadata_.has_dictionary_page() && offset > metadata_.dictionary_page_offset())
            offset = metadata_.dictionary_page_offset();
        return offset;
    };

    UInt64 rowStartIndexOffset = 0;
    for (int i = 0; i < total_row_groups; ++i)
    {
        const auto row_group_meta = file_meta.RowGroup(i);
        Int64 start_offset = 0;
        Int64 total_bytes = 0;
        start_offset = get_column_start_offset(*row_group_meta->ColumnChunk(0));
        total_bytes = row_group_meta->total_compressed_size();
        if (!total_bytes)
            for (int j = 0; j < row_group_meta->num_columns(); ++j)
                total_bytes += row_group_meta->ColumnChunk(j)->total_compressed_size();

        const UInt64 midpoint_offset = static_cast<UInt64>(start_offset + total_bytes / 2);

        UInt64 currentRow = row_group_meta->num_rows();
        /// Current row group has intersection with the required range.
        if (file_info_.start() <= midpoint_offset && midpoint_offset < file_info_.start() + file_info_.length())
        {
            RowGroupInformation info;
            info.index = i;
            info.num_rows = currentRow;
            info.start = row_group_meta->file_offset();
            info.total_compressed_size = row_group_meta->total_compressed_size();
            info.total_size = row_group_meta->total_byte_size();
            info.rowStartIndexOffset = rowStartIndexOffset;
            row_group_metadatas.emplace_back(std::move(info));
        }
        rowStartIndexOffset += currentRow;
    }
    return row_group_metadatas;
}

std::vector<Int32> ParquetMetaBuilder::pruneColumn(
    const DB::Block & header, const parquet::FileMetaData & metadata, bool case_insensitive, bool allow_missing_columns)
{
    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata.schema(), &schema));

    DB::ArrowFieldIndexUtil field_util(case_insensitive, allow_missing_columns);
    auto index_mapping = field_util.findRequiredIndices(header, *schema, metadata);

    std::vector<Int32> column_indices;
    for (const auto & [clickhouse_header_index, parquet_indexes] : index_mapping)
        for (auto parquet_index : parquet_indexes)
            column_indices.push_back(parquet_index);
    return column_indices;
}

std::unique_ptr<ColumnIndexStore> ParquetMetaBuilder::collectColumnIndex(
    const parquet::RowGroupMetaData & rgMeta,
    parquet::RowGroupPageIndexReader & rowGroupIndex,
    const std::vector<Int32> & column_indices,
    bool case_insensitive)
{
    auto result = std::make_unique<ColumnIndexStore>();
    ColumnIndexStore & column_index_store = *result;
    column_index_store.reserve(column_indices.size());

    for (auto const column_index : column_indices)
    {
        const auto * col_desc = rgMeta.schema()->Column(column_index);
        const auto col_index = rowGroupIndex.GetColumnIndex(column_index);
        const auto offset_index = rowGroupIndex.GetOffsetIndex(column_index);
        const std::string columnName = case_insensitive ? boost::to_lower_copy(col_desc->name()) : col_desc->name();
        column_index_store[columnName] = ColumnIndex::create(col_desc, col_index, offset_index);
    }
    return result;
}

/// TODO: Benchmark
void ParquetMetaBuilder::build(
    DB::ReadBuffer * read_buffer,
    const substraitInputFile & file_info,
    const DB::Block * readBlock,
    const ColumnIndexFilter * column_index_filter)
{
    auto reader = openInputParquetFile(read_buffer);
    const auto file_meta = reader->metadata();

    readRowGroups = collectRequiredRowGroups(*file_meta, file_info);

    if (collectSkipRowGroup || collectPageIndex)
    {
        Int32 total_row_groups = file_meta->num_row_groups();
        std::vector<Int32> required_row_group_indices(readRowGroups.size());
        for (size_t i = 0; i < readRowGroups.size(); ++i)
            required_row_group_indices[i] = readRowGroups[i].index;

        if (collectSkipRowGroup)
        {
            std::vector<int> total_row_group_indices(total_row_groups);
            std::iota(total_row_group_indices.begin(), total_row_group_indices.end(), 0);
            std::ranges::set_difference(total_row_group_indices, required_row_group_indices, std::back_inserter(skipRowGroups));
        }

        if (collectPageIndex)
        {
            assert(readBlock != nullptr);
            readColumns = pruneColumn(*readBlock, *file_meta, case_insensitive, allow_missing_columns);

            if (column_index_filter == nullptr)
                return;
            columnIndexStore.reserve(readRowGroups.size());
            readRowRangesPerRG.reserve(readRowGroups.size());

            for (const auto row_group : readRowGroups)
            {
                const auto rgMeta = file_meta->RowGroup(row_group.index);
                const auto pageIndex = reader->GetPageIndexReader();
                if (pageIndex == nullptr)
                    continue;
                const auto rowGroupIndex = pageIndex->RowGroup(row_group.index);
                auto columnIndex = collectColumnIndex(*rgMeta, *rowGroupIndex, readColumns, case_insensitive);

                readRowRangesPerRG.insert_or_assign(row_group.index,
                    std::make_unique<RowRanges>(column_index_filter->calculateRowRanges(*columnIndex, row_group.num_rows)));
                columnIndexStore.insert_or_assign(row_group.index, std::move(columnIndex));
            }
        }
    }
}
}