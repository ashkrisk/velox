#include "SampleConnector.h"

namespace facebook::velox::connector::sample {

std::shared_ptr<Connector> SampleConnectorFactory::newConnector(
    const std::string& id,
    std::shared_ptr<const Config> config,
    folly::Executor* executor) {
  // VELOX_NYI("not yet implemented");
  return std::make_shared<SampleConnector>(id);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SampleConnectorFactory>())

} // namespace facebook::velox::connector::sample
