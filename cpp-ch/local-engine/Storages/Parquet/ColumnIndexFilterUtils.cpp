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

#include "ColumnIndexFilterUtils.h"


struct FilteredOffsetIndex final : local_engine::OffsetIndex
{
    std::unique_ptr<OffsetIndex> offset_index_;
    std::vector<int32_t> index_map_;

    FilteredOffsetIndex(std::unique_ptr<OffsetIndex> offset_index, std::vector<int32_t> index_map)
        : offset_index_(std::move(offset_index)), index_map_(std::move(index_map))
    {
    }

    int32_t getPageCount() const override { return static_cast<int32_t>(index_map_.size()); }

    int64_t getOffset(int32_t pageIndex) const override { return offset_index_->getOffset(index_map_[pageIndex]); }

    int32_t getCompressedPageSize(int32_t pageIndex) const override { return offset_index_->getCompressedPageSize(index_map_[pageIndex]); }

    int64_t getFirstRowIndex(int32_t pageIndex) const override { return offset_index_->getFirstRowIndex(index_map_[pageIndex]); }

    int64_t getLastRowIndex(int32_t pageIndex, int64_t rowGroupRowCount) const override
    {
        const int32_t nextIndex = index_map_[pageIndex] + 1;
        return (nextIndex >= offset_index_->getPageCount() ? rowGroupRowCount : offset_index_->getFirstRowIndex(nextIndex)) - 1;
    }
};

namespace local_engine
{

std::unique_ptr<OffsetIndex> ColumnIndexFilterUtils::filterOffsetIndex(
    const std::vector<parquet::PageLocation> & page_locations, const RowRanges & rowRanges, int64_t rowGroupRowCount)
{
    auto offset_index = std::make_unique<OffsetIndexImpl>(page_locations);
    std::vector<int32_t> index_map;
    const auto pageCount = offset_index->getPageCount();
    for (int32_t i = 0; i < pageCount; ++i)
    {
        int64_t from = offset_index->getFirstRowIndex(i);
        if (rowRanges.isOverlapping(from, offset_index->getLastRowIndex(i, rowGroupRowCount)))
            index_map.push_back(i);
    }
    return std::make_unique<FilteredOffsetIndex>(std::move(offset_index), std::move(index_map));
}

ReadRanges ColumnIndexFilterUtils::calculateReadRanges(
    const OffsetIndex & offset_index, const arrow::io::ReadRange & chunk_range, int64_t firstPageOffset)
{
    // offsetIndex could be a FilteredOffsetIndex or OffsetIndexImpl
    int32_t n = offset_index.getPageCount();
    if (n <= 0)
        return {};

    ReadRanges ranges;

    // Add a range for dictionary page if required
    int64_t rowGroupOffset = chunk_range.offset;
    arrow::io::ReadRange * currentRange = nullptr;
    if (rowGroupOffset < firstPageOffset)
    {
        ranges.emplace_back(rowGroupOffset, firstPageOffset - rowGroupOffset);
        currentRange = &ranges.back();
    }
    auto extend = [](arrow::io::ReadRange & read_range, const int64_t offset, const int64_t length)
    {
        if (read_range.offset + read_range.length == offset)
        {
            read_range.length += length;
            return true;
        }
        return false;
    };

    for (int32_t i = 0; i < n; ++i)
    {
        int64_t offset = offset_index.getOffset(i);
        int32_t length = offset_index.getCompressedPageSize(i);
        if (currentRange && !extend(*currentRange, offset, length))
        {
            // create a new range
            ranges.emplace_back(offset, length);
            currentRange = &ranges.back();
        }
    }
    return ranges;
}

ReadRanges ColumnIndexFilterUtils::calculateReadRanges(
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & rowRanges,
    int64_t rowGroupRowCount,
    const arrow::io::ReadRange & chunk_range)
{
    //TODO: implement in product codes
    if (rowRanges.rowCount() == rowGroupRowCount)
        return {chunk_range};

    auto offset_index = filterOffsetIndex(page_locations, rowRanges, rowGroupRowCount);
    return calculateReadRanges(*offset_index, chunk_range, page_locations[0].offset);
}

}