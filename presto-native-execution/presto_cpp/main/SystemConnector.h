/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#pragma once

#include "presto_cpp/main/SystemSplit.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"

#include "velox/connectors/Connector.h"

namespace facebook::presto {

class TaskManager;

class SystemColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit SystemColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

class SystemTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  explicit SystemTableHandle(
      std::string connectorId, std::string schemaName, std::string tableName);

  ~SystemTableHandle() override {}

  std::string toString() const override;

  const std::string& schemaName() {
    return schemaName_;
  }

  const std::string& tableName() {
    return tableName_;
  }

  const velox::RowTypePtr& taskSchema() {
    return kTaskSchema_;
  }

 private:
  const std::string schemaName_;
  const std::string tableName_;

  velox::RowTypePtr kTaskSchema_;
};

class SystemDataSource : public velox::connector::DataSource {
 public:
  SystemDataSource(
      const std::shared_ptr<const velox::RowType>& outputType,
      const std::shared_ptr<velox::connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      const TaskManager* taskManager,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  void addDynamicFilter(
      velox::column_index_t /*outputChannel*/,
      const std::shared_ptr<velox::common::Filter>& /*filter*/) override {
    VELOX_NYI("Dynamic filters not supported by SystemConnector.");
  }

  std::optional<velox::RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  std::unordered_map<std::string, velox::RuntimeCounter> runtimeStats() override {
    // TODO: Which stats do we want to expose here?
    return {};
  }

 private:

  std::shared_ptr<SystemTableHandle> taskTableHandle_;
  // Mapping between output columns and their indices (column_index_t) corresponding
  // to the taskInfo fields for them.
  std::vector<velox::column_index_t> outputColumnMappings_;
  velox::RowTypePtr outputType_;

  velox::RowVectorPtr taskTableResult_;

  std::shared_ptr<SystemSplit> currentSplit_;

  size_t completedRows_{0};
  size_t completedBytes_{0};

  const TaskManager* taskManager_;
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
};

class SystemConnector : public velox::connector::Connector {
 public:
  SystemConnector(
      const std::string& id,
      std::shared_ptr<const velox::Config> properties,
      folly::Executor* FOLLY_NULLABLE /*executor*/)
      : Connector(id, properties) {}

  void setTaskManager(const TaskManager* taskManager) {
    taskManager_ = taskManager;
  }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const std::shared_ptr<const velox::RowType>& outputType,
      const std::shared_ptr<velox::connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      velox::connector::ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    VELOX_CHECK(taskManager_);
    return std::make_unique<SystemDataSource>(
        outputType,
        tableHandle,
        columnHandles,
        taskManager_,
        connectorQueryCtx->memoryPool());
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr /*inputType*/,
      std::shared_ptr<
          velox::connector::ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      velox::connector::CommitStrategy /*commitStrategy*/) override final {
    VELOX_NYI("SystemConnector does not support data sink.");
  }

 private :

  const TaskManager* taskManager_;
};

class SystemConnectorFactory : public velox::connector::ConnectorFactory {
 public:
  static constexpr const char* FOLLY_NONNULL kSystemConnectorName{"$system"};

  SystemConnectorFactory() : SystemConnectorFactory(kSystemConnectorName) {}

  explicit SystemConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::Config> properties,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<SystemConnector>(id, properties, executor);
  }
};

} // namespace facebook::presto
