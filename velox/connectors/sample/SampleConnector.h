#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::sample {

// class SampleConnectorSplit : public ConnectorSplit {
//   public:
//     SampleConnectorSplit(std::string id) : ConnectorSplit(id) {}
// };

class SampleTableHandle : public ConnectorTableHandle {
 public:
  SampleTableHandle(std::string id) : ConnectorTableHandle(std::move(id)) {}
};

class SampleDataSource : public DataSource {
 public:
  SampleDataSource(const RowTypePtr outputType, memory::MemoryPool* pool)
      : outputType_{std::move(outputType)}, pool_{pool} {
    VELOX_CHECK_NOT_NULL(pool);
  }

  void addSplit(std::shared_ptr<ConnectorSplit> split) {
    // VELOX_NYI("not implemented yet");
    splits_ += 1;
  }
  std::optional<RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) {
    // VELOX_NYI("not implemented yet");
    if (completed_ >= splits_) {
      return nullptr;
    }
    completed_ += 1;
    return RowVector::createEmpty(outputType_, pool_);
  }
  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) {
    VELOX_NYI("not implemented yet");
  }
  uint64_t getCompletedBytes() {
    // VELOX_NYI("not implemented yet");
    return 0;
  }
  uint64_t getCompletedRows() {
    // VELOX_NYI("not implemented yet");
    return completed_;
  }
  std::unordered_map<std::string, RuntimeCounter> runtimeStats() {
    // VELOX_NYI("not implemented yet");
    return {};
  }

 private:
  const RowTypePtr outputType_;
  memory::MemoryPool* const pool_;
  int splits_ = 0;
  int completed_ = 0;
};

class SampleConnector : public Connector {
 public:
  SampleConnector(const std::string& id) : Connector(id) {}

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) {
    // VELOX_NYI("not implemented yet");
    VELOX_DCHECK_NOT_NULL(connectorQueryCtx)
    return std::make_unique<SampleDataSource>(
        outputType, connectorQueryCtx->memoryPool());
  }

  virtual std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) {
    VELOX_NYI("Sample connector doesn't support DataSink");
  }
};

class SampleConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kSampleConnectorName{"sample"};

  SampleConnectorFactory() : ConnectorFactory(kSampleConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<SampleConnector>(id);
  }
};

} // namespace facebook::velox::connector::sample
