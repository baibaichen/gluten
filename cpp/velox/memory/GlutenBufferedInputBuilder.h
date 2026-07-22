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

#include "GlutenDirectBufferedInput.h"
#include "velox/ch/Disks/IO/FileCacheBufferedInputBuilder.h"
#include "velox/ch/Interpreters/FileCache/FileCacheManager.h"
#include "velox/connectors/hive/BufferedInputBuilder.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/dwio/common/CachedBufferedInput.h"

namespace gluten {

/// Three-branch buffered-input builder for the Hive connector read path:
///   1. `connectorQueryCtx->cache() != nullptr` -> native `CachedBufferedInput`
///      (Gluten's AsyncDataCache path, unchanged);
///   2. else if a ClickHouse `FileCache` default is available -> the ported
///      `FileCacheBufferedInput` (via the fork's `FileCacheBufferedInputBuilder`);
///   3. else -> `GlutenDirectBufferedInput` (direct path, unchanged).
///
/// The `FileCacheManager*` is nullable. When null, this builder behaves exactly
/// like the original two-branch builder (branch 2 is never taken), so a build
/// without FileCache configured is unchanged.
class GlutenBufferedInputBuilder : public facebook::velox::connector::hive::BufferedInputBuilder {
 public:
  GlutenBufferedInputBuilder() = default;

  explicit GlutenBufferedInputBuilder(facebook::velox::ch::FileCacheManager* fileCacheManager)
      : fileCacheManager_(fileCacheManager) {
    if (fileCacheManager_ != nullptr && fileCacheManager_->hasDefault()) {
      fileCacheBuilder_ = std::make_unique<facebook::velox::ch::FileCacheBufferedInputBuilder>(*fileCacheManager_);
    }
  }

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> create(
      const facebook::velox::FileHandle& fileHandle,
      const facebook::velox::dwio::common::ReaderOptions& readerOpts,
      const facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStatistics,
      std::shared_ptr<facebook::velox::IoStats> ioStats,
      folly::Executor* executor,
      const folly::F14FastMap<std::string, std::string>& fileReadOps = {}) override {
    if (connectorQueryCtx->cache()) {
      return std::make_unique<facebook::velox::dwio::common::CachedBufferedInput>(
          fileHandle.file,
          dwio::common::MetricsLog::voidLog(),
          fileHandle.uuid,
          connectorQueryCtx->cache(),
          facebook::velox::connector::Connector::getTracker(connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
          fileHandle.groupId,
          std::move(ioStatistics),
          std::move(ioStats),
          executor,
          readerOpts,
          fileReadOps);
    }
    if (fileCacheBuilder_ != nullptr) {
      return fileCacheBuilder_->create(
          fileHandle,
          readerOpts,
          connectorQueryCtx,
          std::move(ioStatistics),
          std::move(ioStats),
          executor,
          fileReadOps);
    }
    return std::make_unique<GlutenDirectBufferedInput>(
        fileHandle.file,
        dwio::common::MetricsLog::voidLog(),
        fileHandle.uuid,
        facebook::velox::connector::Connector::getTracker(connectorQueryCtx->scanId(), readerOpts.loadQuantum()),
        fileHandle.groupId,
        std::move(ioStatistics),
        std::move(ioStats),
        executor,
        readerOpts,
        fileReadOps);
  }

 private:
  // Non-owning; the FileCacheManager is owned by VeloxBackend and must outlive
  // this builder. Held only to document the dependency; branch 2 uses the
  // delegate below.
  facebook::velox::ch::FileCacheManager* fileCacheManager_{nullptr};
  std::unique_ptr<facebook::velox::ch::FileCacheBufferedInputBuilder> fileCacheBuilder_;
};

} // namespace gluten
