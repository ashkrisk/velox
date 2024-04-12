#include "SampleConnector.h"

namespace facebook::velox::connector::sample {

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SampleConnectorFactory>())

} // namespace facebook::velox::connector::sample
