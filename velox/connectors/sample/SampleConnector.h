#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::sample {

class SampleTableHandle : public ConnectorTableHandle {
  public:
    SampleTableHandle(std::string id) : ConnectorTableHandle(std::move(id)) {}
};

class SampleDataSource : public DataSource {
  public:
    void addSplit(std::shared_ptr<ConnectorSplit> split) {
      VELOX_NYI("not implemented yet");
    }
    std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future) {
      VELOX_NYI("not implemented yet");
    }
    void addDynamicFilter(column_index_t outputChannel, const std::shared_ptr<common::Filter>& filter) {
      VELOX_NYI("not implemented yet");
    }
    uint64_t getCompletedBytes() {
      VELOX_NYI("not implemented yet");
    }
    uint64_t getCompletedRows() {
      VELOX_NYI("not implemented yet");
    }
    std::unordered_map<std::string, RuntimeCounter> runtimeStats() {
      VELOX_NYI("not implemented yet");
    }
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
    return std::make_unique<SampleDataSource>();
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
