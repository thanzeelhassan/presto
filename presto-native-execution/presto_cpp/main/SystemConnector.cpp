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
#include "presto_cpp/main/SystemConnector.h"
#include "presto_cpp/main/PrestoTask.h"
#include "presto_cpp/main/TaskManager.h"

namespace facebook::presto {

using namespace velox;

SystemTableHandle::SystemTableHandle(std::string connectorId, std::string schemaName, std::string tableName) :
      ConnectorTableHandle(std::move(connectorId)), schemaName_(std::move(schemaName)), tableName_(std::move(tableName)){
  VELOX_USER_CHECK_EQ(schemaName_, "runtime", "SystemConnector supports only runtime schema");
  VELOX_USER_CHECK_EQ(tableName_, "tasks", "SystemConnector supports only tasks table");

  std::vector<std::string> kTaskColumnNames =
      {"node_id", "task_id", "stage_execution_id", "stage_id", "query_id", "state", "splits", "queued_splits", "running_splits", "completed_splits",
       "split_scheduled_time_ms", "split_cpu_time_ms", "split_blocked_time_ms", "raw_input_bytes", "raw_input_rows", "processed_input_bytes",
       "processed_input_rows", "output_bytes", "output_rows", "physical_written_bytes", "created", "start", "last_heartbeat", "end"};
  std::vector<velox::TypePtr> kTaskColumnTypes =
      {velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(),
       velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(),
       velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::BIGINT(), velox::TIMESTAMP(), velox::TIMESTAMP(), velox::TIMESTAMP(), velox::TIMESTAMP()};
  kTaskSchema_ = ROW(std::move(kTaskColumnNames), std::move(kTaskColumnTypes));
}

std::string SystemTableHandle::toString() const {
  return fmt::format("schema: {} table: {}", schemaName_, tableName_);
}

SystemDataSource::SystemDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    const TaskManager* taskManager,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : taskManager_(taskManager), pool_(pool) {
  auto systemTableHandle =
      std::dynamic_pointer_cast<SystemTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      systemTableHandle, "TableHandle must be an instance of SystemTableHandle");
  VELOX_USER_CHECK_EQ(systemTableHandle->schemaName(), "runtime", "SystemConnector supports only runtime schema");
  VELOX_USER_CHECK_EQ(systemTableHandle->tableName(), "tasks", "SystemConnector supports only tasks table");

  taskTableHandle_ = systemTableHandle;
  outputColumnMappings_.reserve(outputType->names().size());
  auto taskSchema = taskTableHandle_->taskSchema();
  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}'",
        outputName);

    auto handle = std::dynamic_pointer_cast<SystemColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of SystemColumnHandle "
        "for '{}' on table '{}'",
        handle->name());

    auto columnIndex = taskSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(columnIndex.has_value());
    outputColumnMappings_.push_back(columnIndex.value());
  }

  outputType_ = outputType;
  taskTableResult_ = std::dynamic_pointer_cast<RowVector>(BaseVector::create(taskSchema, 0, pool_));
}

void SystemDataSource::addSplit(std::shared_ptr<connector::ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<SystemSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for SystemDataSource.");
  VELOX_USER_CHECK_EQ(currentSplit_->schemaName(), "runtime", "SystemConnector supports only runtime schema");
  VELOX_USER_CHECK_EQ(currentSplit_->tableName(), "tasks", "SystemConnector supports only tasks table");
}

namespace {
void getTaskResults(const TaskMap& taskMap, const RowVectorPtr& taskTableResult) {
  auto numRows = taskMap.size();
  taskTableResult->resize(numRows);
  for (auto i = 0; i < taskTableResult->type()->size(); i++) {
    taskTableResult->childAt(i)->resize(numRows);
  }

  auto toMillis = [](int64_t nanos) -> int64_t {
    return nanos/1'000'000;
  };
  int i = 0;
  for (const auto& taskEntry : taskMap) {
    auto task = taskEntry.second;
    auto taskInfo = task->updateInfo();
    taskTableResult->childAt(0)->asFlatVector<StringView>()->set(i, StringView(taskInfo.nodeId));
    taskTableResult->childAt(1)->asFlatVector<StringView>()->set(i, StringView(taskInfo.taskId));
    std::string temp = fmt::format("{}", task->id.stageExecutionId());
    taskTableResult->childAt(2)->asFlatVector<StringView>()->set(i, StringView(temp));
    temp = fmt::format("{}", task->id.stageId());
    taskTableResult->childAt(3)->asFlatVector<StringView>()->set(i, StringView(temp));
    temp = fmt::format("{}", task->id.queryId());
    taskTableResult->childAt(4)->asFlatVector<StringView>()->set(i, StringView(temp));
    temp = fmt::format("{}", taskInfo.taskStatus.state);
    taskTableResult->childAt(5)->asFlatVector<StringView>()->set(i, StringView(temp));
    taskTableResult->childAt(6)->asFlatVector<int64_t>()->set(i, taskInfo.stats.totalDrivers);
    taskTableResult->childAt(7)->asFlatVector<int64_t>()->set(i, taskInfo.stats.queuedDrivers);
    taskTableResult->childAt(8)->asFlatVector<int64_t>()->set(i, taskInfo.stats.runningDrivers);
    taskTableResult->childAt(9)->asFlatVector<int64_t>()->set(i, taskInfo.stats.completedDrivers);
    taskTableResult->childAt(10)->asFlatVector<int64_t>()->set(i, toMillis(taskInfo.stats.totalScheduledTimeInNanos));
    taskTableResult->childAt(11)->asFlatVector<int64_t>()->set(i, toMillis(taskInfo.stats.totalCpuTimeInNanos));
    taskTableResult->childAt(12)->asFlatVector<int64_t>()->set(i, toMillis(taskInfo.stats.totalBlockedTimeInNanos));
    taskTableResult->childAt(13)->asFlatVector<int64_t>()->set(i, taskInfo.stats.rawInputDataSizeInBytes);
    taskTableResult->childAt(14)->asFlatVector<int64_t>()->set(i, taskInfo.stats.rawInputPositions);
    taskTableResult->childAt(15)->asFlatVector<int64_t>()->set(i, taskInfo.stats.processedInputDataSizeInBytes);
    taskTableResult->childAt(16)->asFlatVector<int64_t>()->set(i, taskInfo.stats.processedInputPositions);
    taskTableResult->childAt(17)->asFlatVector<int64_t>()->set(i, taskInfo.stats.outputDataSizeInBytes);
    taskTableResult->childAt(18)->asFlatVector<int64_t>()->set(i, taskInfo.stats.outputPositions);
    taskTableResult->childAt(19)->asFlatVector<int64_t>()->set(i, taskInfo.stats.physicalWrittenDataSizeInBytes);
    // Setting empty TIMESTAMP for below fields as string -> Timestamp is not implemented in Velox.
    //taskTableResult->childAt(20)->asFlatVector<Timestamp>()->set(i, Timestamp(taskInfo.stats.createTime));
    taskTableResult->childAt(20)->asFlatVector<Timestamp>()->set(i, Timestamp());
    //taskTableResult->childAt(21)->asFlatVector<Timestamp>()->set(i, Timestamp(taskInfo.stats.firstStartTime));
    taskTableResult->childAt(21)->asFlatVector<Timestamp>()->set(i, Timestamp());
    //taskTableResult->childAt(22)->asFlatVector<Timestamp>()->set(i, Timestamp(taskInfo.lastHeartbeat));
    taskTableResult->childAt(22)->asFlatVector<Timestamp>()->set(i, Timestamp());
    //taskTableResult->childAt(23)->asFlatVector<Timestamp>()->set(i, Timestamp(taskInfo.stats.endTime));
    taskTableResult->childAt(23)->asFlatVector<Timestamp>()->set(i, Timestamp());

    i++;
  }
}

}

std::optional<RowVectorPtr> SystemDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  if (!currentSplit_) {
    return nullptr;
  }

  TaskMap taskMap = taskManager_->tasks();
  auto numRows = taskMap.size();
  getTaskResults(taskMap, taskTableResult_);

  auto result = std::dynamic_pointer_cast<RowVector>
      (BaseVector::create(outputType_, numRows, pool_));

  for (auto i = 0; i < outputColumnMappings_.size(); i++) {
    result->childAt(i) = taskTableResult_->childAt(outputColumnMappings_.at(i));
  }

  currentSplit_ = nullptr;
  return result;
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SystemConnectorFactory>())

} // namespace facebook::presto