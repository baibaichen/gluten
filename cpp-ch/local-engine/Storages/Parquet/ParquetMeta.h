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
#pragma once

#include <Storages/Parquet/ColumnIndexFilter.h>
#include <Storages/Parquet/RowRanges.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <absl/container/flat_hash_map.h>
#include <base/types.h>
#include <parquet/file_reader.h>

namespace local_engine
{
using ColumnIndexStoreMap = absl::flat_hash_map<Int32, std::unique_ptr<ColumnIndexStore>>;
using RowRangesMap = absl::flat_hash_map<Int32, std::unique_ptr<RowRanges>>;
struct RowGroupInformation
{
    UInt32 index = 0;
    UInt64 start = 0;
    UInt64 total_compressed_size = 0;
    UInt64 total_size = 0;
    UInt64 num_rows = 0;
    UInt64 rowStartIndexOffset = 0;
};

struct ParquetMetaBuilder
{
    // control flag
    bool case_insensitive = false;
    bool allow_missing_columns = false;
    bool collectSkipRowGroup = false;
    bool collectPageIndex = false;

    // result
    std::vector<RowGroupInformation> readRowGroups;

    std::vector<Int32> skipRowGroups;

    std::vector<Int32> readColumns;
    ColumnIndexStoreMap columnIndexStore;
    RowRangesMap readRowRangesPerRG;

    static ParquetMetaBuilder collectRequiredRowGroups(DB::ReadBuffer * read_buffer, const substraitInputFile & file_info)
    {
        ParquetMetaBuilder result;
        result.build(read_buffer, file_info);
        return result;
    }
    void build(
        DB::ReadBuffer * read_buffer,
        const substraitInputFile & file_info,
        const DB::Block * readBlock = nullptr,
        const ColumnIndexFilter * column_index_filter = nullptr);

private:
    static std::unique_ptr<parquet::ParquetFileReader> openInputParquetFile(DB::ReadBuffer * read_buffer);

    static std::vector<Int32>
    pruneColumn(const DB::Block & header, const parquet::FileMetaData & metadata, bool case_insensitive, bool allow_missing_columns);

    static std::vector<RowGroupInformation>
    collectRequiredRowGroups(const parquet::FileMetaData & file_meta, const substraitInputFile & file_info_);

    static std::unique_ptr<ColumnIndexStore> collectColumnIndex(
        const parquet::RowGroupMetaData & rgMeta,
        parquet::RowGroupPageIndexReader & rowGroupIndex,
        const std::vector<Int32> & column_indices,
        bool case_insensitive = false);
};

namespace ParquetVirtualMeta
{
inline constexpr auto TMP_ROWINDEX = "_tmp_metadata_row_index";
inline bool hasMetaColumns(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX) != nullptr;
}
inline DB::DataTypePtr getMetaColumnType(const DB::Block & header)
{
    return header.findByName(TMP_ROWINDEX)->type;
}
inline DB::Block removeMetaColumns(const DB::Block & header)
{
    DB::Block new_header;
    for (const auto & col : header)
        if (col.name != TMP_ROWINDEX)
            new_header.insert(col);
    return new_header;
}
}

class IRowRangesProvider
{
public:
    virtual ~IRowRangesProvider() = default;
    virtual std::optional<RowRanges> getRowRanges(Int32 row_group_index) const = 0;
    virtual UInt64 getRowGroupStartIndex(Int32 row_group_index) const = 0;
};

class IColumnIndexStoreProvider : public IRowRangesProvider
{
public:
    ~IColumnIndexStoreProvider() override = default;
    virtual const ColumnIndexStore & getColumnIndexStore(Int32 row_group) const = 0;
};

class ColumnIndexRowRangesProvider : public IColumnIndexStoreProvider
{
public:
    explicit ColumnIndexRowRangesProvider(ParquetMetaBuilder & meta_collect)
        : columnIndexStorePerRG_(std::move(meta_collect.columnIndexStore))
        , rowRangesPerRG_(std::move(meta_collect.readRowRangesPerRG))
        , readColumns(std::move(meta_collect.readColumns))
    {
        for (const auto & rg : meta_collect.readRowGroups)
        {
            readRowGroups.push_back(rg.index);
            rowCountsPerRG_[rg.index] = rg.num_rows;
            startIndexPerRG_[rg.index] = rg.rowStartIndexOffset;
        }
    }

    std::optional<RowRanges> getRowRanges(Int32 row_group_index) const override
    {
        const auto rg_count = rowGroupCount(row_group_index);
        if (rg_count == 0 || (canPruningPage(row_group_index) && internalGetRowRanges(row_group_index).rowCount() == 0))
            return std::nullopt;

        return canPruningPage(row_group_index) ? internalGetRowRanges(row_group_index) : RowRanges::createSingle(rg_count);
    }
    UInt64 getRowGroupStartIndex(Int32 row_group_index) const override { return startIndexPerRG_.at(row_group_index); }
    const ColumnIndexStore & getColumnIndexStore(Int32 row_group) const override { return *columnIndexStorePerRG_.at(row_group); }

private:
    const ColumnIndexStoreMap columnIndexStorePerRG_;
    const RowRangesMap rowRangesPerRG_;
    std::map<Int32, Int32> rowCountsPerRG_;
    std::map<Int32, UInt64> startIndexPerRG_;

    Int64 rowGroupCount(const Int32 row_group) const { return rowCountsPerRG_.at(row_group); }
    bool canPruningPage(const Int32 row_group) const { return rowRangesPerRG_.contains(row_group); }
    const RowRanges & internalGetRowRanges(Int32 row_group) const { return *rowRangesPerRG_.at(row_group); }

public:
    std::vector<Int32> readRowGroups;
    const std::vector<Int32> readColumns;
};

}