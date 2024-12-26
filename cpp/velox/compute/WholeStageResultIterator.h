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

#include "compute/Runtime.h"
#include "iceberg/IcebergPlanConverter.h"
#include "memory/ColumnarBatchIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/plan.pb.h"
#include "utils/Metrics.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"

namespace gluten {

class WholeStageResultIterator : public ColumnarBatchIterator {
 public:
  WholeStageResultIterator(
      VeloxMemoryManager* memoryManager,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      const std::vector<facebook::velox::core::PlanNodeId>& scanNodeIds,
      const std::vector<std::shared_ptr<SplitInfo>>& scanInfos,
      const std::vector<facebook::velox::core::PlanNodeId>& streamIds,
      const std::string spillDir,
      const std::unordered_map<std::string, std::string>& confMap,
      const SparkTaskInfo& taskInfo);

  virtual ~WholeStageResultIterator() {
    if (task_ != nullptr && task_->isRunning()) {
      // calling .wait() may take no effect in single thread execution mode
      task_->requestCancel().wait();
    }
  }

  std::shared_ptr<ColumnarBatch> next() override;

  int64_t spillFixedSize(int64_t size) override;

  Metrics* getMetrics(int64_t exportNanos) {
    collectMetrics();
    if (metrics_) {
      metrics_->veloxToArrow = exportNanos;
    }
    return metrics_.get();
  }

  const facebook::velox::exec::Task* task() const {
    return task_.get();
  }

  const facebook::velox::core::PlanNode* veloxPlan() const {
    return veloxPlan_.get();
  }

 private:
  /// Get the Spark confs to Velox query context.
  std::unordered_map<std::string, std::string> getQueryContextConf();

  /// Create QueryCtx.
  std::shared_ptr<facebook::velox::core::QueryCtx> createNewVeloxQueryCtx();

  /// Get all the children plan node ids with postorder traversal.
  void getOrderedNodeIds(
      const std::shared_ptr<const facebook::velox::core::PlanNode>&,
      std::vector<facebook::velox::core::PlanNodeId>& nodeIds);

  /// Create connector config.
  std::shared_ptr<facebook::velox::config::ConfigBase> createConnectorConfig();

  /// Construct partition columns.
  void constructPartitionColumns(
      std::unordered_map<std::string, std::optional<std::string>>&,
      const std::unordered_map<std::string, std::string>&);

  /// Add splits to task. Skip if already added.
  void tryAddSplitsToTask();

  /// Collect Velox metrics.
  void collectMetrics();

  /// Return a certain type of runtime metric. Supported metric types are: sum, count, min, max.
  static int64_t runtimeMetric(
      const std::string& type,
      const std::unordered_map<std::string, facebook::velox::RuntimeMetric>& runtimeStats,
      const std::string& metricId);

  /// Memory.
  VeloxMemoryManager* memoryManager_;

  /// Config, task and plan.
  std::shared_ptr<config::ConfigBase> veloxCfg_;
  const SparkTaskInfo taskInfo_;
  std::shared_ptr<facebook::velox::exec::Task> task_;
  std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan_;

  /// Spill.
  std::string spillStrategy_;
  std::shared_ptr<folly::Executor> spillExecutor_ = nullptr;

  /// Metrics
  std::unique_ptr<Metrics> metrics_{};

  /// All the children plan node ids with postorder traversal.
  std::vector<facebook::velox::core::PlanNodeId> orderedNodeIds_;

  /// Node ids should be omitted in metrics.
  std::unordered_set<facebook::velox::core::PlanNodeId> omittedNodeIds_;
  std::vector<facebook::velox::core::PlanNodeId> scanNodeIds_;
  std::vector<std::shared_ptr<SplitInfo>> scanInfos_;
  std::vector<facebook::velox::core::PlanNodeId> streamIds_;
  std::vector<std::vector<facebook::velox::exec::Split>> splits_;
  bool noMoreSplits_ = false;
};

} // namespace gluten
