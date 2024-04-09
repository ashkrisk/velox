#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::sample {

class SampleConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kSampleConnectorName{"sample"};

  SampleConnectorFactory() : ConnectorFactory(kSampleConnectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor = nullptr) override;
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
    VELOX_NYI("not implemented yet");
  }

  virtual std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) {
    VELOX_NYI("not implemented yet");
  }
};

} // namespace facebook::velox::connector::sample
